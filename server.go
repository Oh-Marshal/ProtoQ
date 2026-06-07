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

// Handler 是 ProtoQ 服务端的请求处理函数（无连接上下文）。
// opcode: 操作码
// body: 请求体（可能为空）
// 返回值：响应体（nil 表示空响应）、错误（非 nil 时发送错误响应或不响应）
type Handler func(opcode uint32, body []byte) ([]byte, error)

// ConnContext 表示一个 ProtoQ 连接上下文。
// 在连接建立时创建，连接关闭时销毁。
// 业务层可通过此对象获取连接标识、读写元数据、主动关闭连接。
type ConnContext struct {
	// ID 连接唯一标识（服务端内单调递增）
	ID uint64

	conn    net.Conn
	decoder *Decoder

	writeMu sync.Mutex
	closed  atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc

	// metadata 连接级元数据（业务层可读写）
	metadata map[string]interface{}
	metaMu   sync.RWMutex

	// 服务端引用
	server *Server
}

// ConnHandler 是带连接上下文的请求处理函数。
// ctx: 连接上下文（包含连接 ID、元数据）
// opcode: 操作码
// body: 请求体
// 返回值：响应体、错误
type ConnHandler func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error)

// Server 是 ProtoQ 协议服务端。
// 监听指定地址，接受连接并为每个连接创建处理循环。
type Server struct {
	// 无连接上下文的处理器（兼容旧 API）
	handlers map[uint32]Handler

	// 带连接上下文的处理器
	connHandlers map[uint32]ConnHandler

	mu sync.RWMutex

	// 传输层工厂
	listenerFactory ListenerFactory

	// 配置
	opcodeLen int

	// OnConnect 连接建立后调用的钩子（可在构造后设置）
	OnConnect func(ctx *ConnContext)

	// OnClose 连接关闭前调用的钩子（可在构造后设置）
	OnClose func(ctx *ConnContext)

	// 活跃连接管理
	conns   map[*serverConn]struct{}
	connsMu sync.Mutex

	// 停止信号
	ctx    context.Context
	cancel context.CancelFunc

	// 状态
	running atomic.Bool
}

// serverConn 表示一个服务端连接（内部类型）。
type serverConn struct {
	id      uint64
	conn    net.Conn
	decoder *Decoder
	server  *Server

	// 写锁
	writeMu sync.Mutex

	// 停止
	ctx    context.Context
	cancel context.CancelFunc

	// 读循环完成
	readDone chan struct{}

	// 公开的连接上下文
	pubCtx *ConnContext
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
		handlers:        make(map[uint32]Handler),
		connHandlers:    make(map[uint32]ConnHandler),
		listenerFactory: factory,
		opcodeLen:       DefaultOpcodeLen,
		conns:           make(map[*serverConn]struct{}),
		ctx:             ctx,
		cancel:          cancel,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Handle 注册指定 Opcode 的处理函数（无连接上下文）。
// 多次注册同一个 Opcode 会覆盖之前的处理函数。
func (s *Server) Handle(opcode uint32, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[opcode] = handler
}

// HandleConn 注册指定 Opcode 的带连接上下文的处理函数。
// ConnHandler 优先级高于 Handler：若同一 Opcode 同时注册了两者，优先使用 ConnHandler。
func (s *Server) HandleConn(opcode uint32, handler ConnHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connHandlers[opcode] = handler
}

// getHandler 获取指定 Opcode 的处理函数（ConnHandler 优先）。
func (s *Server) getHandler(opcode uint32) (Handler, bool, ConnHandler, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ch, chOk := s.connHandlers[opcode]
	if chOk {
		return nil, false, ch, true
	}
	h, hOk := s.handlers[opcode]
	return h, hOk, nil, false
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
		conn, err := listener.Accept()
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

		sc := s.newServerConn(conn, connID.Add(1))
		s.connsMu.Lock()
		s.conns[sc] = struct{}{}
		s.connsMu.Unlock()

		// 调用连接建立钩子
		if s.OnConnect != nil {
			s.OnConnect(sc.pubCtx)
		}

		go sc.serve()
	}
}

// newServerConn 创建内部连接对象及其公开上下文。
func (s *Server) newServerConn(conn net.Conn, id uint64) *serverConn {
	connCtx, connCancel := context.WithCancel(s.ctx)

	pubCtx := &ConnContext{
		ID:       id,
		conn:     conn,
		decoder:  NewDecoder(conn),
		ctx:      connCtx,
		cancel:   connCancel,
		metadata: make(map[string]interface{}),
		server:   s,
	}

	return &serverConn{
		id:       id,
		conn:     conn,
		decoder:  pubCtx.decoder, // 共用解码器
		server:   s,
		ctx:      connCtx,
		cancel:   connCancel,
		readDone: make(chan struct{}),
		pubCtx:   pubCtx,
	}
}

// Shutdown 优雅关闭服务端。
func (s *Server) Shutdown() error {
	s.cancel()

	s.connsMu.Lock()
	conns := make([]*serverConn, 0, len(s.conns))
	for sc := range s.conns {
		conns = append(conns, sc)
	}
	s.connsMu.Unlock()

	for _, sc := range conns {
		sc.conn.Close()
	}

	return nil
}

// serve 处理单个连接。
func (sc *serverConn) serve() {
	defer sc.cleanup()

	for {
		frame, err := sc.decoder.Decode()
		if err != nil {
			if err == io.EOF {
				return
			}
			select {
			case <-sc.ctx.Done():
				return
			default:
				continue
			}
		}

		if !frame.IsRequest() {
			continue
		}

		// 优先查找 ConnHandler
		h, hOk, ch, chOk := sc.server.getHandler(frame.Opcode)

		if chOk {
			// 使用带连接上下文的处理器
			if frame.NeedsAck() {
				go sc.handleConnRequest(frame, ch)
			} else {
				go ch(sc.pubCtx, frame.Opcode, frame.Body)
			}
		} else if hOk {
			// 使用无连接上下文的处理器（兼容旧 API）
			if frame.NeedsAck() {
				go sc.handleRequest(frame, h)
			} else {
				go func(f *Frame, handler Handler) {
					handler(f.Opcode, f.Body)
				}(frame, h)
			}
		} else {
			// 无处理函数
			if frame.NeedsAck() {
				sc.sendErrorResponse(frame, fmt.Errorf("unknown opcode: %d", frame.Opcode))
			}
		}
	}
}

// handleConnRequest 使用 ConnHandler 处理需要应答的请求。
func (sc *serverConn) handleConnRequest(frame *Frame, handler ConnHandler) {
	respBody, err := handler(sc.pubCtx, frame.Opcode, frame.Body)
	if err != nil {
		// 若有响应体则发送它（如协商拒绝的 JSON），否则发送错误消息
		body := respBody
		if len(body) == 0 {
			body = []byte(err.Error())
		}
		resp := NewResponseFrame(frame.Opcode, frame.Seq, body, frame.Flags)
		resp.Flags = resp.Flags.SetOpcodeLen(sc.server.opcodeLen)
		sc.writeFrame(resp)
		return
	}

	resp := NewResponseFrame(frame.Opcode, frame.Seq, respBody, frame.Flags)
	resp.Flags = resp.Flags.SetOpcodeLen(sc.server.opcodeLen)

	sc.writeFrame(resp)
}

// handleRequest 处理需要应答的请求（传统 Handler）。
func (sc *serverConn) handleRequest(frame *Frame, handler Handler) {
	respBody, err := handler(frame.Opcode, frame.Body)
	if err != nil {
		sc.sendErrorResponse(frame, err)
		return
	}

	resp := NewResponseFrame(frame.Opcode, frame.Seq, respBody, frame.Flags)
	resp.Flags = resp.Flags.SetOpcodeLen(sc.server.opcodeLen)

	sc.writeFrame(resp)
}

// sendErrorResponse 发送错误响应。
func (sc *serverConn) sendErrorResponse(reqFrame *Frame, err error) {
	errMsg := []byte(err.Error())
	resp := NewResponseFrame(reqFrame.Opcode, reqFrame.Seq, errMsg, reqFrame.Flags)
	resp.Flags = resp.Flags.SetOpcodeLen(sc.server.opcodeLen)
	sc.writeFrame(resp)
}

// writeFrame 线程安全地写入一个帧。
func (sc *serverConn) writeFrame(f *Frame) error {
	sc.writeMu.Lock()
	defer sc.writeMu.Unlock()

	sc.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := EncodeTo(f, sc.conn)
	return err
}

// cleanup 清理连接资源。
func (sc *serverConn) cleanup() {
	// 调用关闭钩子（若 pubCtx 存在）
	if sc.pubCtx != nil && sc.server.OnClose != nil {
		sc.server.OnClose(sc.pubCtx)
	}

	sc.conn.Close()
	if sc.cancel != nil {
		sc.cancel()
	}
	close(sc.readDone)

	sc.server.connsMu.Lock()
	delete(sc.server.conns, sc)
	sc.server.connsMu.Unlock()
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

// Close 主动关闭连接（幂等）。
func (c *ConnContext) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.cancel()
	return c.conn.Close()
}

// WriteFrame 向连接写入一个帧（线程安全）。
func (c *ConnContext) WriteFrame(f *Frame) error {
	if c.closed.Load() {
		return ErrConnClosed
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := EncodeTo(f, c.conn)
	return err
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
