package protoq

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// ConnContext 表示一个 ProtoQ 连接上下文。
// 在连接建立时创建，连接关闭时销毁。
// 业务层可通过此对象获取连接标识、读写元数据、主动关闭连接。
type ConnContext struct {
	// Conn 共享连接抽象（嵌入）
	*Conn

	// ID 连接唯一标识（服务端内单调递增）
	ID uint64

	ctx    context.Context
	cancel context.CancelFunc

	// metadata 连接级元数据（业务层可读写）
	metadata map[string]interface{}
	metaMu   sync.RWMutex

	// 服务端引用
	server *Server
}

// ConnHandler 是 ProtoQ 服务端的请求处理函数。
// ctx: 连接上下文（包含连接 ID、元数据）
// opcode: 操作码
// body: 请求体
// 返回值：响应体、错误
type ConnHandler func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error)

// Server 是 ProtoQ 协议服务端。
// 监听指定地址，接受连接并为每个连接创建处理循环。
type Server struct {
	handlers map[uint32]ConnHandler
	mu       sync.RWMutex

	// 传输层工厂
	listenerFactory ListenerFactory

	// 配置
	opcodeLen int

	// OnConnect 连接建立后调用的钩子（可在构造后设置）
	OnConnect func(ctx *ConnContext)

	// OnClose 连接关闭前调用的钩子（可在构造后设置）
	OnClose func(ctx *ConnContext)

	// 活跃连接管理
	conns   map[*ConnContext]struct{}
	connsMu sync.Mutex

	// 停止信号
	ctx    context.Context
	cancel context.CancelFunc

	// 状态
	running atomic.Bool
}

// ServerOption 是 Server 的配置选项。
type ServerOption func(*Server)

// WithServerOpcodeLen 设置服务端 Opcode 字段长度。
func WithServerOpcodeLen(n int) ServerOption {
	return func(s *Server) { s.opcodeLen = n }
}

// WithOnConnect 设置连接建立时的回调。
func WithOnConnect(fn func(ctx *ConnContext)) ServerOption {
	return func(s *Server) { s.OnConnect = fn }
}

// WithOnClose 设置连接关闭时的回调。
func WithOnClose(fn func(ctx *ConnContext)) ServerOption {
	return func(s *Server) { s.OnClose = fn }
}

// NewServer 创建一个 ProtoQ 服务端。
func NewServer(factory ListenerFactory, opts ...ServerOption) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		handlers:        make(map[uint32]ConnHandler),
		listenerFactory: factory,
		opcodeLen:       DefaultOpcodeLen,
		conns:           make(map[*ConnContext]struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Handle 注册指定 Opcode 的处理函数。
// 多次注册同一个 Opcode 会覆盖之前的处理函数。
func (s *Server) Handle(opcode uint32, handler ConnHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[opcode] = handler
}

// ListenAndServe 开始监听并服务请求。
// 阻塞直到 ctx 被取消或监听失败。
func (s *Server) ListenAndServe(ctx context.Context, addr string) error {
	listener, err := s.listenerFactory.Listen(ctx, addr)
	if err != nil {
		return fmt.Errorf("protoq server listen: %w", err)
	}
	defer listener.Close()

	s.running.Store(true)
	defer s.running.Store(false)

	// 监听 context 取消
	go func() {
		select {
		case <-ctx.Done():
			listener.Close()
		case <-s.ctx.Done():
			listener.Close()
		}
	}()

	var connID atomic.Uint64

	for {
		rawConn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			case <-s.ctx.Done():
				return nil
			default:
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return fmt.Errorf("protoq server accept: %w", err)
			}
		}

		ctx := s.newConnContext(rawConn, connID.Add(1))

		s.connsMu.Lock()
		s.conns[ctx] = struct{}{}
		s.connsMu.Unlock()

		// 调用连接建立钩子
		if s.OnConnect != nil {
			s.OnConnect(ctx)
		}

		go ctx.serve()
	}
}

// newConnContext 为新连接创建 ConnContext。
func (s *Server) newConnContext(rawConn net.Conn, id uint64) *ConnContext {
	connCtx, connCancel := context.WithCancel(s.ctx)

	return &ConnContext{
		Conn:     NewConn(rawConn),
		ID:       id,
		ctx:      connCtx,
		cancel:   connCancel,
		metadata: make(map[string]interface{}),
		server:   s,
	}
}

// Shutdown 优雅关闭服务端。
func (s *Server) Shutdown() error {
	s.cancel()

	s.connsMu.Lock()
	conns := make([]*ConnContext, 0, len(s.conns))
	for ctx := range s.conns {
		conns = append(conns, ctx)
	}
	s.connsMu.Unlock()

	for _, ctx := range conns {
		ctx.Close()
	}

	return nil
}

// serve 处理单个连接的帧读取和分发。
func (ctx *ConnContext) serve() {
	defer ctx.cleanup()

	for {
		frame, err := ctx.Decode()
		if err != nil {
			if err == io.EOF {
				return
			}
			select {
			case <-ctx.ctx.Done():
				return
			default:
				continue
			}
		}

		if !frame.IsRequest() {
			continue
		}

		handler, ok := ctx.server.lookupHandler(frame.Opcode)
		if !ok {
			if frame.NeedsAck() {
				ctx.sendErrorResponse(frame, fmt.Errorf("unknown opcode: %d", frame.Opcode))
			}
			continue
		}

		if frame.NeedsAck() {
			go ctx.handleRequest(frame, handler)
		} else {
			go handler(ctx, frame.Opcode, frame.Body)
		}
	}
}

// lookupHandler 查找 Opcode 对应的处理函数（读锁保护）。
func (s *Server) lookupHandler(opcode uint32) (ConnHandler, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.handlers[opcode]
	return h, ok
}

// handleRequest 使用 ConnHandler 处理需要应答的请求。
func (ctx *ConnContext) handleRequest(frame *Frame, handler ConnHandler) {
	respBody, err := handler(ctx, frame.Opcode, frame.Body)
	if err != nil {
		// 若有响应体则发送它（如协商拒绝的 JSON），否则发送错误消息
		body := respBody
		if len(body) == 0 {
			body = []byte(err.Error())
		}
		resp := NewResponseFrame(frame.Opcode, frame.Seq, body, frame.Flags)
		resp.Flags = resp.Flags.SetOpcodeLen(ctx.server.opcodeLen)
		ctx.WriteFrame(resp)
		return
	}

	resp := NewResponseFrame(frame.Opcode, frame.Seq, respBody, frame.Flags)
	resp.Flags = resp.Flags.SetOpcodeLen(ctx.server.opcodeLen)
	ctx.WriteFrame(resp)
}

// sendErrorResponse 发送错误响应。
func (ctx *ConnContext) sendErrorResponse(reqFrame *Frame, err error) {
	errMsg := []byte(err.Error())
	resp := NewResponseFrame(reqFrame.Opcode, reqFrame.Seq, errMsg, reqFrame.Flags)
	resp.Flags = resp.Flags.SetOpcodeLen(ctx.server.opcodeLen)
	ctx.WriteFrame(resp)
}

// cleanup 清理连接资源。
func (ctx *ConnContext) cleanup() {
	// 调用关闭钩子
	if ctx.server.OnClose != nil {
		ctx.server.OnClose(ctx)
	}

	ctx.Conn.Close()
	if ctx.cancel != nil {
		ctx.cancel()
	}

	ctx.server.connsMu.Lock()
	delete(ctx.server.conns, ctx)
	ctx.server.connsMu.Unlock()
}

// ──────────────────────────────────────────────
// ConnContext 公开方法
// ──────────────────────────────────────────────

// Set 设置连接级元数据。
func (c *ConnContext) Set(key string, value interface{}) {
	c.metaMu.Lock()
	defer c.metaMu.Unlock()
	c.metadata[key] = value
}

// Get 获取连接级元数据。
func (c *ConnContext) Get(key string) (interface{}, bool) {
	c.metaMu.RLock()
	defer c.metaMu.RUnlock()
	v, ok := c.metadata[key]
	return v, ok
}

// GetString 获取字符串类型的连接元数据。
func (c *ConnContext) GetString(key string) (string, bool) {
	v, ok := c.Get(key)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// Close 主动关闭连接（幂等）。同时取消关联的 context。
func (c *ConnContext) Close() error {
	c.cancel()
	return c.Conn.Close()
}

// Context 返回连接的 context.Context（用于超时控制）。
func (c *ConnContext) Context() context.Context {
	return c.ctx
}

// ActiveConns 返回当前活跃连接数。
func (s *Server) ActiveConns() int {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	return len(s.conns)
}

// IsRunning 返回服务端是否正在运行。
func (s *Server) IsRunning() bool {
	return s.running.Load()
}
