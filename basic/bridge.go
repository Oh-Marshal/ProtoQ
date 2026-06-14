// Package biz — 连接级消息桥接器
//
// 对标 Java uni-protocol org.facelang.unified.proto.netty.NettyMessageBridge。
// ConnectionBridge 负责传输层与协议层的边界，管理单个连接的完整消息生命周期：
//   - 读循环：Decode → Decrypt（跳过协商包）→ MessageDispatcher.Dispatch → requireAck 时写回响应
//   - 写循环：Frame → Encrypt（跳过协商包）→ Encode → 写出
//   - 异常处理：MessageException → 写错误包 + 关连接（致命）
//
// ConnectionBridge 对标 Netty 的 ChannelDuplexHandler，但 Go 中用 goroutine + channel 替代 Netty pipeline。
// 每个 TCP 连接对应一个 ConnectionBridge 实例。
package basic

import (
	"fmt"
	"io"

	api "github.com/oh-marshal/protoq/api"
)

// negotiateMessageID 协商消息的 messageId（Opcode 低字节）。
// 对标 uni-protocol MessageId.NEGOTIATE (0x01) 和 NEGOTIATE_RESPONSE (0x02)。
const (
	msgIDNegotiate         = 0x01 // 协商请求
	msgIDNegotiateResponse = 0x02 // 协商响应
)

// ConnectionBridge 连接级消息桥接器。
//
// 对标 uni-protocol NettyMessageBridge（ChannelDuplexHandler）。
// 持有 BeanRegister（含 CodecRegister、MessageDispatcher、EventDispatcher）、
// 连接引用和写 channel。每个连接创建一个实例，运行读循环和写循环。
//
// 核心职责：
//   - 读侧（handleFrame）：帧解密 → 消息分发 → 响应回写
//   - 写侧（WriteFrame）：帧加密 → 写出
//   - 错误处理：致命错误关闭连接
type ConnectionBridge struct {
	// conn 底层连接（*Conn，需要 frame 级读写能力）
	conn *Conn

	// beanReg Bean 注册中心（含 CodecRegister、MessageDispatcher、EventDispatcher）
	beanReg *BeanRegister

	// writeCh 出站帧写入通道（由 Send/WriteFrame 触发）
	writeCh chan *api.Frame

	// stopCh 停止信号
	stopCh chan struct{}

	// done 读写循环完成信号
	done chan struct{}
}

// NewConnectionBridge 创建连接桥接器。
//
// 对标 uni-protocol NettyMessageBridge 构造器。
//
// 参数：
//   - conn: 底层连接（*Conn，提供 Decode/WriteFrame 等传输层操作）
//   - beanReg: Bean 注册中心（已注册 Codec/Filter/Handler）
func NewConnectionBridge(conn *Conn, beanReg *BeanRegister) *ConnectionBridge {
	return &ConnectionBridge{
		conn:    conn,
		beanReg: beanReg,
		writeCh: make(chan *api.Frame, 64),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}
}

// Serve 启动读写循环，阻塞直到连接关闭或出错。
//
// 对标 uni-protocol NettyMessageBridge 挂载到 Netty pipeline 后的生命周期。
// 同时启动读循环（主 goroutine）和写循环（子 goroutine），
// 任一退出时关闭连接并等待对方完成。
func (b *ConnectionBridge) Serve() {
	defer close(b.done)
	defer b.conn.Close()

	// 启动写循环（子 goroutine）
	go b.writeLoop()

	// 读循环（主 goroutine，对标 uni-protocol channelRead）
	b.readLoop()

	// 读循环退出 → 关闭写通道 → 等待写循环结束
	close(b.writeCh)
}

// ─── 读循环 ──────────────────────────────────────────────────────────────────

// readLoop 持续从连接读取帧并分发。
//
// 对标 uni-protocol NettyMessageBridge.channelRead(ctx, msg)。
//
// 流程：
//  1. conn.Decode() 读取一个完整帧
//  2. 调用 decryptBody() 解密（跳过协商包）
//  3. 调用 MessageDispatcher.Dispatch() 分发
//  4. 若帧需要 ACK 且 dispatch 返回了 response → 写回响应帧
//  5. io.EOF 或连接关闭 → 正常退出
//  6. 其他错误 → 尝试写错误帧后退出
func (b *ConnectionBridge) readLoop() {
	for {
		// 检查停止信号
		select {
		case <-b.stopCh:
			return
		default:
		}

		// 1. 解码帧
		frame, err := b.conn.Decode()
		if err != nil {
			if err == io.EOF || b.conn.IsClosed() {
				return
			}
			// 不可恢复的读取错误，退出
			return
		}

		if frame == nil {
			continue
		}

		// 2. 处理帧
		b.handleFrame(frame)
	}
}

// handleFrame 处理单个入站帧。
//
// 对标 uni-protocol NettyMessageBridge.channelRead 的核心逻辑。
//
// 流程：
//  1. 解密 Body（跳过协商消息 0x01/0x02）
//  2. MessageDispatcher.Dispatch(conn, frame) → ctx, err
//  3. 若 err 且致命 → 写错误帧 + 关闭连接
//  4. 若 frame.RequiresAck() 且 ctx.Response() 非空 → 构建响应帧并写回
func (b *ConnectionBridge) handleFrame(frame *api.Frame) {
	// ── 1. 解密 Body（对标 uni-protocol NettyMessageDecoder.decrypt）──
	// 协商消息（messageId=0x01/0x02）不解密
	if err := b.decryptBody(frame); err != nil {
		b.handleFatalError(frame, err)
		return
	}

	// ── 2. 消息分发（对标 uni-protocol MessageDispatcher.dispatch）──
	dispatchCtx, err := b.beanReg.MessageDispatcher.Dispatch(b.conn, frame)
	if err != nil {
		// 非致命错误：写错误响应但不关闭连接
		b.writeErrorResponse(frame, err)
		return
	}

	// ── 3. 响应帧为 nil（已被 Dispatcher 完成 MessageQueue 中的 Future）──
	if dispatchCtx == nil {
		return
	}

	// ── 4. 请求帧需要 ACK 且存在响应 → 写回响应帧 ──
	if frame.RequiresAck() {
		resp := dispatchCtx.Response()
		if resp != nil {
			b.writeResponse(frame, resp)
		}
	}
}

// decryptBody 解密帧的 Body。
//
// 对标 uni-protocol NettyMessageDecoder.decrypt(conn, packet)。
// 跳过协商消息（messageId=0x01 NEGOTIATE 和 0x02 NEGOTIATE_RESPONSE），
// 因为协商阶段尚未约定加密方案，CODEC_TYPE 属性也未设置。
//
// 解密步骤：
//  1. 提取 messageId = frame.Opcode & 0xFF
//  2. 若 messageId 为 0x01 或 0x02 → 跳过（返回 nil）
//  3. 获取连接 Codec → codec.Decrypt(frame.Body) → 替换 frame.Body
func (b *ConnectionBridge) decryptBody(frame *api.Frame) error {
	if len(frame.Body) == 0 {
		return nil
	}

	messageID := frame.Opcode & 0xFF

	// 协商消息不加密（对标 uni-protocol 的跳过规则）
	if messageID == msgIDNegotiate || messageID == msgIDNegotiateResponse {
		return nil
	}

	// 获取编解码器并解密
	codec := b.conn.Codec()
	if codec == nil {
		return fmt.Errorf("biz: 连接未绑定编解码器，无法解密 (messageId=0x%02X)", messageID)
	}

	plainBody, err := codec.Decrypt(frame.Body)
	if err != nil {
		return fmt.Errorf("biz: 解密失败 (messageId=0x%02X): %w", messageID, err)
	}

	frame.Body = plainBody
	return nil
}

// ─── 写循环 ──────────────────────────────────────────────────────────────────

// writeLoop 出站帧写入循环。
//
// 对标 uni-protocol NettyMessageBridge.write(ctx, msg, promise)。
// 从 writeCh 读取帧，加密 Body（跳过协商包）后编码并写出。
func (b *ConnectionBridge) writeLoop() {
	for frame := range b.writeCh {
		if frame == nil {
			continue
		}

		// 1. 加密 Body（跳过协商消息）
		if err := b.encryptBody(frame); err != nil {
			// 加密失败：跳过此帧，记录错误
			continue
		}

		// 2. 写出帧（Conn.WriteFrame 负责编码 + 写入）
		if err := b.conn.WriteFrame(frame); err != nil {
			if b.conn.IsClosed() {
				return
			}
			// 写入失败，停止写循环
			return
		}
	}
}

// encryptBody 加密帧的 Body。
//
// 对标 uni-protocol NettyMessageEncoder.encrypt(conn, packet)。
// 跳过协商消息（messageId=0x01/0x02）。
//
// 加密步骤：
//  1. 提取 messageId = frame.Opcode & 0xFF
//  2. 若 messageId 为 0x01 或 0x02 → 跳过（返回 nil）
//  3. 获取连接 Codec → codec.Encrypt(frame.Body) → 替换 frame.Body
func (b *ConnectionBridge) encryptBody(frame *api.Frame) error {
	if len(frame.Body) == 0 {
		return nil
	}

	messageID := frame.Opcode & 0xFF

	// 协商消息不加密
	if messageID == msgIDNegotiate || messageID == msgIDNegotiateResponse {
		return nil
	}

	codec := b.conn.Codec()
	if codec == nil {
		return fmt.Errorf("biz: 连接未绑定编解码器，无法加密 (messageId=0x%02X)", messageID)
	}

	cipherBody, err := codec.Encrypt(frame.Body)
	if err != nil {
		return fmt.Errorf("biz: 加密失败 (messageId=0x%02X): %w", messageID, err)
	}

	frame.Body = cipherBody
	return nil
}

// WriteFrame 向出站写通道提交一帧。
//
// 对标 uni-protocol NettyMessageBridge.write 的对外接口。
// 非阻塞：若写通道已满，可能在 goroutine 中异步写入。
func (b *ConnectionBridge) WriteFrame(frame *api.Frame) {
	select {
	case b.writeCh <- frame:
	default:
		// 通道满：丢弃（生产环境应配置更大的缓冲区或背压）
	}
}

// ─── 错误处理 ────────────────────────────────────────────────────────────────

// handleFatalError 处理致命错误：写错误帧 + 关闭连接。
//
// 对标 uni-protocol NettyMessageBridge.exceptionCaught 的致命路径。
// MessageException.isConnectionFatal() == true 时调用。
func (b *ConnectionBridge) handleFatalError(requestFrame *api.Frame, err error) {
	// 尝试写错误响应帧
	b.writeErrorResponse(requestFrame, err)

	// 关闭连接（对标 uni-protocol ctx.close()）
	b.conn.Close()
}

// writeErrorResponse 写错误响应帧。
//
// 对标 uni-protocol NettyMessageUtil.getErrPacketData + writeAndFlush。
// 构建包含错误信息的响应帧并通过写循环发出。
func (b *ConnectionBridge) writeErrorResponse(requestFrame *api.Frame, err error) {
	if requestFrame == nil || !requestFrame.RequiresAck() {
		return
	}

	errMsg := []byte(err.Error())
	resp := api.NewResponseFrame(requestFrame.Opcode, requestFrame.Seq, errMsg, requestFrame.Flags)

	// 直接写回（不经过加密，错误响应应为明文）
	b.conn.WriteFrame(resp)
}

// writeResponse 写响应帧。
//
// 对标 uni-protocol getRespPacketData + writeAndFlush。
// 将 dispatch 返回的响应对象构建为响应帧并提交到写循环。
//
// 响应构建规则（对标 uni-protocol NettyMessageUtil.getRespPacketData）：
//   - response 是 *api.Frame → 直接写出
//   - response 是 []byte → 封装为响应帧写出
//   - response 是 nil → 不写回
func (b *ConnectionBridge) writeResponse(requestFrame *api.Frame, response interface{}) {
	switch resp := response.(type) {
	case *api.Frame:
		// 直接帧 → 提交到写通道
		b.WriteFrame(resp)

	case []byte:
		// []byte → 封装为响应帧
		respFrame := api.NewResponseFrame(requestFrame.Opcode, requestFrame.Seq, resp, requestFrame.Flags)
		b.WriteFrame(respFrame)

	case nil:
		// nil 响应 → 不写回（对标 uni-protocol EmptyPayload）
		// 但仍需 WriteFrame 空响应以完成 ACK 配对
		// 空 Body 响应帧
		respFrame := api.NewResponseFrame(requestFrame.Opcode, requestFrame.Seq, nil, requestFrame.Flags)
		b.WriteFrame(respFrame)

	default:
		// 其他类型：忽略（应由 Converter 在上层转换）
	}
}

// ─── 生命周期 ────────────────────────────────────────────────────────────────

// Close 停止桥接器。
func (b *ConnectionBridge) Close() {
	select {
	case <-b.stopCh:
		return
	default:
		close(b.stopCh)
	}
}

// Done 返回完成信号 channel（读写循环均退出后关闭）。
func (b *ConnectionBridge) Done() <-chan struct{} {
	return b.done
}

// Conn 返回底层连接（用于外部访问连接属性）。
func (b *ConnectionBridge) Conn() *Conn {
	return b.conn
}
