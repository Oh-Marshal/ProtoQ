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

// Handler 是 ProtoQ 服务端的请求处理函数。
// opcode: 操作码
// body: 请求体（可能为空）
// 返回值：响应体（nil 表示空响应）、错误（非 nil 时发送错误响应或不响应）
type Handler func(opcode uint32, body []byte) ([]byte, error)

// Server 是 ProtoQ 协议服务端。
// 监听指定地址，接受连接并为每个连接创建处理循环。
type Server struct {
	handlers map[uint32]Handler
	mu       sync.RWMutex

	// 传输层工厂
	listenerFactory ListenerFactory

	// 配置
	opcodeLen int

	// 活跃连接管理
	conns   map[*serverConn]struct{}
	connsMu sync.Mutex

	// 停止信号
	ctx    context.Context
	cancel context.CancelFunc

	// 状态
	running atomic.Bool
}

// serverConn 表示一个服务端连接。
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
}

// ServerOption 是 Server 的配置选项。
type ServerOption func(*Server)

// WithServerOpcodeLen 设置服务端 Opcode 字段长度。
func WithServerOpcodeLen(n int) ServerOption {
	return func(s *Server) { s.opcodeLen = n }
}

// NewServer 创建一个 ProtoQ 服务端。
func NewServer(factory ListenerFactory, opts ...ServerOption) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{
		handlers:        make(map[uint32]Handler),
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

// Handle 注册指定 Opcode 的处理函数。
// 多次注册同一个 Opcode 会覆盖之前的处理函数。
func (s *Server) Handle(opcode uint32, handler Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[opcode] = handler
}

// getHandler 获取指定 Opcode 的处理函数。
func (s *Server) getHandler(opcode uint32) (Handler, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	h, ok := s.handlers[opcode]
	return h, ok
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
				// 临时错误，继续接受
				if ne, ok := err.(net.Error); ok && ne.Temporary() {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				return fmt.Errorf("protoq server accept: %w", err)
			}
		}

		sc := &serverConn{
			id:      connID.Add(1),
			conn:    conn,
			decoder: NewDecoder(conn),
			server:  s,
			ctx:     s.ctx,
			readDone: make(chan struct{}),
		}

		s.connsMu.Lock()
		s.conns[sc] = struct{}{}
		s.connsMu.Unlock()

		go sc.serve()
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

	// 关闭所有连接
	for _, sc := range conns {
		sc.conn.Close()
	}

	return nil
}

// serve 处理单个连接。
func (sc *serverConn) serve() {
	defer func() {
		sc.conn.Close()
		close(sc.readDone)

		sc.server.connsMu.Lock()
		delete(sc.server.conns, sc)
		sc.server.connsMu.Unlock()
	}()

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
				// 解析错误，尝试继续
				continue
			}
		}

		// 只处理请求帧
		if !frame.IsRequest() {
			continue
		}

		// 查找处理函数
		handler, ok := sc.server.getHandler(frame.Opcode)
		if !ok {
			// 无处理函数，如果是需要应答的请求则发送错误响应
			if frame.NeedsAck() {
				sc.sendErrorResponse(frame, fmt.Errorf("unknown opcode: %d", frame.Opcode))
			}
			continue
		}

		if frame.NeedsAck() {
			// 需要应答：调用处理函数并发送响应
			go sc.handleRequest(frame, handler)
		} else {
			// 通知：异步处理
			go func(f *Frame, h Handler) {
				h(f.Opcode, f.Body)
			}(frame, handler)
		}
	}
}

// handleRequest 处理需要应答的请求。
func (sc *serverConn) handleRequest(frame *Frame, handler Handler) {
	respBody, err := handler(frame.Opcode, frame.Body)
	if err != nil {
		sc.sendErrorResponse(frame, err)
		return
	}

	resp := NewResponseFrame(frame.Opcode, frame.Seq, respBody, frame.Flags)
	resp.Flags = resp.Flags.SetOpcodeLen(sc.server.opcodeLen)

	if err := sc.writeFrame(resp); err != nil {
		// 写入失败，连接可能已断开
		return
	}
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
