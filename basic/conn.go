// Package protoq — 共享连接抽象
//
// Conn 封装了底层 net.Conn、帧编解码器、连接生命周期（context）、
// 消息队列、属性存储和编解码器绑定。
// 对标 Java uni-protocol 的 NettyConnection，实现 Connection 接口。
// 同时被服务端（通过 ConnContext）和客户端（通过 Client）复用。
package basic

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn 是 ProtoQ 连接的通用抽象，实现 Connection 接口。
// 对标 uni-protocol NettyConnection：持有底层 socket、帧解码、写锁、
// context 取消、消息队列、属性存储和编解码器。
type Conn struct {
	raw     net.Conn
	decoder *Decoder

	writeMu sync.Mutex
	closed  atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	// ── Connection 接口所需字段 ──

	// id 连接唯一标识（服务端分配，客户端为 0）
	id uint64
	// connectTime 连接建立时间
	connectTime time.Time
	// connType 连接类型标识（"tcp", "ws", "quic"）
	connType string
	// props 连接附加属性（对标 uni-protocol props）
	props map[string]interface{}
	// msgQueue ACK 等待队列
	msgQueue *MessageQueue
	// codec 当前绑定的编解码器（协商后设置）
	codec   Codec
	codecMu sync.RWMutex
}

// NewConn 从已建立的 net.Conn 创建 ProtoQ 连接。
// parentCtx 用于派生连接的 context：当 parentCtx 被取消时，连接自动取消。
// 服务端传入 server.ctx，客户端传入 context.Background()。
func NewConn(parentCtx context.Context, raw net.Conn) *Conn {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Conn{
		raw:         raw,
		decoder:     NewDecoder(raw),
		ctx:         ctx,
		cancel:      cancel,
		connectTime: time.Now(),
		connType:    "tcp",
		props:       make(map[string]interface{}),
		msgQueue:    NewMessageQueue(),
	}
}

// NewConnWithID 创建带 ID 的连接（服务端使用）。
func NewConnWithID(parentCtx context.Context, raw net.Conn, id uint64, connType string) *Conn {
	c := NewConn(parentCtx, raw)
	c.id = id
	c.connType = connType
	return c
}

// ─── 帧级读写 ──────────────────────────────────────────────────────────────

// Decode 从流中解码一个完整的 ProtoQ 帧。
func (c *Conn) Decode() (*Frame, error) {
	return c.decoder.Decode()
}

// WriteFrame 线程安全地向连接写入一个帧（实现 Connection 接口）。
// 写入前设置 5 秒写截止时间。
func (c *Conn) WriteFrame(f *Frame) error {
	if c.closed.Load() {
		return ErrConnClosed
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed.Load() {
		return ErrConnClosed
	}

	c.raw.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := EncodeTo(f, c.raw)
	return err
}

// ─── 生命周期 ──────────────────────────────────────────────────────────────

// Close 关闭连接（幂等）。先取消 context，再关闭底层连接。
func (c *Conn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.msgQueue.Clear()
	c.cancel()
	return c.raw.Close()
}

// IsClosed 返回连接是否已关闭（实现 Connection 接口）。
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

// Context 返回连接的 context.Context（连接关闭时自动取消）。
func (c *Conn) Context() context.Context {
	return c.ctx
}

// Raw 返回底层 net.Conn。
func (c *Conn) Raw() net.Conn {
	return c.raw
}

// ─── Connection 接口实现 ───────────────────────────────────────────────────

// ConnectionID 返回连接唯一标识（实现 Connection 接口）。
func (c *Conn) ConnectionID() uint64 {
	return c.id
}

// SetConnectionID 设置连接 ID（服务端 Accept 时分配）。
func (c *Conn) SetConnectionID(id uint64) {
	c.id = id
}

// ConnectTime 返回连接建立时间（实现 Connection 接口）。
func (c *Conn) ConnectTime() time.Time {
	return c.connectTime
}

// ConnectionType 返回连接类型标识（实现 Connection 接口）。
func (c *Conn) ConnectionType() string {
	return c.connType
}

// RemoteAddr 返回远程客户端地址（实现 Connection 接口）。
func (c *Conn) RemoteAddr() net.Addr {
	return c.raw.RemoteAddr()
}

// ─── 属性存储 ──────────────────────────────────────────────────────────────

// GetProperty 按字符串键获取连接属性（实现 Connection 接口）。
func (c *Conn) GetProperty(key string) (interface{}, bool) {
	v, ok := c.props[key]
	return v, ok
}

// SetProperty 按字符串键设置连接属性（实现 Connection 接口）。
func (c *Conn) SetProperty(key string, value interface{}) {
	c.props[key] = value
}

// GetStringProperty 按字符串键获取字符串属性（实现 Connection 接口）。
func (c *Conn) GetStringProperty(key string) (string, bool) {
	v, ok := c.props[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// ─── 消息队列 ──────────────────────────────────────────────────────────────

// MessageQueue 返回 ACK 等待队列（实现 Connection 接口）。
func (c *Conn) MessageQueue() *MessageQueue {
	return c.msgQueue
}

// ─── 编解码器 ──────────────────────────────────────────────────────────────

// Codec 获取当前连接绑定的编解码器（实现 Connection 接口）。
// 协商前返回 DefaultCodec（明文透传）。
func (c *Conn) Codec() Codec {
	c.codecMu.RLock()
	defer c.codecMu.RUnlock()
	if c.codec == nil {
		return &DefaultCodec{}
	}
	return c.codec
}

// SetCodec 设置当前连接绑定的编解码器。
func (c *Conn) SetCodec(codec Codec) {
	c.codecMu.Lock()
	defer c.codecMu.Unlock()
	c.codec = codec
}

// ─── 通信 ──────────────────────────────────────────────────────────────────

// nextSendSeq 为 Send() 生成序列号（原子自增，从 1 开始）。
// 客户端有更复杂的 SeqManager，Conn 级别提供简单自增作为兜底。
var nextSendSeq atomic.Uint64

// Send 向对端发送请求并返回响应 channel（实现 Connection 接口）。
//
// 对标 uni-protocol Connection.send(message)。
//
// 流程：
//  1. 生成序列号（原子自增）
//  2. 构建请求帧（RequiresAck=true）
//  3. 创建响应 channel 并注册到 MessageQueue
//  4. 写出帧
//  5. 返回响应 channel（调用方通过 select 等待）
//
// 响应帧由 MessageDispatcher.Dispatch 在收到响应时通过 MessageQueue.Complete 完成。
func (c *Conn) Send(ctx Context, opcode uint32, body []byte) (<-chan *Frame, error) {
	if c.IsClosed() {
		return nil, ErrConnClosed
	}

	// 生成序列号（对标 uni-protocol NEXT_OUT_SEQUENCE）
	seq := uint32(nextSendSeq.Add(1))
	if seq == 0 {
		seq = uint32(nextSendSeq.Add(1)) // 跳过 0
	}

	// 构建请求帧
	frame := NewRequestFrame(opcode, seq, body, true, false)

	// 创建响应 channel（缓冲 1，防止阻塞）
	ch := make(chan *Frame, 1)

	// 注册到 MessageQueue（对标 uni-protocol queue.put(sequence, future)）
	if err := c.msgQueue.Put(seq, ch, 0); err != nil {
		return nil, WrapError("send", err)
	}

	// 写出帧
	if err := c.WriteFrame(frame); err != nil {
		c.msgQueue.CompleteError(seq)
		return nil, WrapError("send", err)
	}

	return ch, nil
}

// Emit 向当前连接发送命名事件（实现 Connection 接口）。
// 对标 uni-protocol Connection.emit(event, message)。
func (c *Conn) Emit(event string, message interface{}) error {
	return nil // 由上层注册的 EventDispatcher 处理
}
