// Package biz — 服务端实现
//
// 对标 Java uni-protocol org.facelang.unified.proto.netty.server.NettyMessageServer。
// MessageServer 聚合 BeanRegister，通过 Register() 注册 Codec/Filter/Handler，
// 为每个连接创建 ConnectionBridge 并启动读写循环。
//
// 使用方式：
//
//	srv := biz.NewMessageServer()
//	srv.RegisterMessageHandler(biz.constant.OpcodeNegotiate, negotiateHandler.Handle)
//	srv.RegisterMessageHandler(biz.constant.OpcodeHeartbeat, heartbeatHandler.Handle)
//	// 注册业务 Handler
//	srv.RegisterMessageHandler(0x0100, myBusinessHandler)
//	srv.Register(&myCustomFilter{}) // Filter 自动路由
//
//	// 启动监听
//	listener, _ := net.Listen("tcp", ":9090")
//	for {
//	    rawConn, _ := listener.Accept()
//	    go srv.Serve(rawConn)
//	}
//
// 对标 uni-protocol 还原要点：
//   - BeanRegister 聚合：CodecRegister + FilterChainRegister + MessageDispatcher + EventDispatcher
//   - Serve(conn)：创建 ConnectionBridge → 启动读写循环
//   - 内置注册 filter.NegotiateFilter + NegotiatePayloadHandler + HeartbeatPayloadHandler
package server

import (
	"net"
	"sync"
	"sync/atomic"

	api "github.com/oh-marshal/protoq"
	constant "github.com/oh-marshal/protoq/basic/constant"
	filter "github.com/oh-marshal/protoq/basic/filter"
	message "github.com/oh-marshal/protoq/basic/message"
	msghandler "github.com/oh-marshal/protoq/basic/message/handler"
	register "github.com/oh-marshal/protoq/basic/register"
	netty "github.com/oh-marshal/protoq/netty"
)

// MessageServer 协议消息服务器。
//
// 对标 uni-protocol NettyMessageServer（implements MessageServer）。
// 聚合 BeanRegister，管理 Codec/Filter/Handler 的注册，
// 为每个接受的连接创建 ConnectionBridge。
//
// 与旧 ServerRecipe 模式的区别：
//   - ServerRecipe 通过 Apply() 注入 Server（依赖外部 Server 实例）
//   - MessageServer 自包含：持有 BeanRegister，直接管理连接
type MessageServer struct {
	// beanRegister Bean 注册中心（聚合 CodecRegister + FilterChain + MessageDispatcher + EventDispatcher）
	beanRegister *register.BeanRegister

	// negotiator 协商策略（可插拔）
	negotiator message.Negotiator

	// negotiateHandler 协商消息处理器（由 NewMessageServer 自动创建并注册）
	negotiateHandler *msghandler.NegotiatePayloadHandler

	// heartbeatHandler 心跳消息处理器（由 NewMessageServer 自动创建并注册）
	heartbeatHandler *msghandler.HeartbeatPayloadHandler

	// 连接管理
	conns   map[*netty.ConnectionBridge]struct{}
	connsMu sync.Mutex

	// 状态
	running atomic.Bool

	// 连接 ID 自增
	connIDSeq atomic.Uint64
}

// MessageServerOption 服务器配置选项。
type MessageServerOption func(*MessageServer)

// WithNegotiator 设置协商策略。
func WithNegotiator(neg message.Negotiator) MessageServerOption {
	return func(s *MessageServer) { s.negotiator = neg }
}

// NewMessageServer 创建协议消息服务器。
//
// 对标 uni-protocol NettyMessageServer 构造器。
// 自动创建 BeanRegister 并注册内置 Filter 和 Handler：
//   - filter.NegotiateFilter：协商检查过滤器
//   - NegotiatePayloadHandler：协商消息处理（messageId=0x01）
//   - HeartbeatPayloadHandler：心跳消息处理（messageId=0x02）
//
// 可通过 Register() 追加 Codec/Filter，通过 RegisterMessageHandler() 注册业务 Handler。
func NewMessageServer(opts ...MessageServerOption) *MessageServer {
	srv := &MessageServer{
		beanRegister: register.NewBeanRegister(),
		negotiator:   &message.DefaultNegotiator{},
		conns:        make(map[*netty.ConnectionBridge]struct{}),
	}

	for _, opt := range opts {
		opt(srv)
	}

	// ── 注册内置 Filter（对标 uni-protocol MessageDispatcher 构造时的自动注册）──
	srv.beanRegister.Register(&filter.NegotiateFilter{})

	// ── 注册内置 Handler（对标 uni-protocol MessageDispatcher 构造时的自动注册）──
	srv.negotiateHandler = msghandler.NewNegotiatePayloadHandler(srv.negotiator)
	srv.heartbeatHandler = msghandler.NewHeartbeatPayloadHandler()

	srv.beanRegister.RegisterMessageHandler(constant.OpcodeNegotiate, srv.negotiateHandler.Handle)
	srv.beanRegister.RegisterMessageHandler(constant.OpcodeHeartbeat, srv.heartbeatHandler.Handle)

	return srv
}

// Register 按类型自动分发注册 Bean。
//
// 对标 uni-protocol BeanRegister.register(bean)。
//
// 支持的类型：
//   - api.Codec: 注册到 CodecRegister
//   - api.Filter: 注册到 FilterChain
//   - 其他类型: 忽略（需调用 RegisterMessageHandler / RegisterEventHandler）
func (s *MessageServer) Register(bean interface{}) {
	s.beanRegister.Register(bean)
}

// RegisterMessageHandler 按 messageID 注册消息处理器。
//
// 对标 uni-protocol BeanRegister.registerMessageHandler(messageId, handler)。
// 便捷方法，等价于 s.beanRegister.MessageDispatcher.RegisterHandler(messageID, handler)。
//
// messageID 使用 Opcode 的低字节（对标 uni-protocol 的 1 字节 messageId）。
// 示例：s.RegisterMessageHandler(0x0100, myHandler)
func (s *MessageServer) RegisterMessageHandler(messageID uint32, handler api.MessageHandler) {
	s.beanRegister.RegisterMessageHandler(messageID, handler)
}

// RegisterEventHandler 按事件名注册事件处理器。
//
// 对标 uni-protocol BeanRegister.registerEventHandler(event, handler)。
func (s *MessageServer) RegisterEventHandler(event string, handler api.EventHandler) {
	s.beanRegister.RegisterEventHandler(event, handler)
}

// BeanRegister 返回内部的 BeanRegister（用于直接访问子注册器）。
func (s *MessageServer) BeanRegister() *register.BeanRegister {
	return s.beanRegister
}

// Serve 为单个连接创建 ConnectionBridge 并启动读写循环。
//
// 对标 uni-protocol NettyMessageServer.channelActive 后的 bridge 挂载。
// 阻塞直到连接关闭。
//
// 流程：
//  1. 包装 rawConn 为 Conn（分配连接 ID）
//  2. 创建 ConnectionBridge（绑定 BeanRegister）
//  3. 注册连接到 BeanRegister
//  4. 启动 bridge.Serve()（读循环 + 写循环）
//  5. 连接关闭后清理
func (s *MessageServer) Serve(rawConn net.Conn) {
	connID := s.connIDSeq.Add(1)
	conn := netty.NewConnWithID(nil, rawConn, connID, "tcp")

	bridge := netty.NewConnectionBridge(conn, s.beanRegister)

	// 注册连接
	s.connsMu.Lock()
	s.conns[bridge] = struct{}{}
	s.connsMu.Unlock()

	// 注册到 BeanRegister 的连接表
	clientID := formatClientID(connID)
	s.beanRegister.AddConnection(clientID, conn)

	// 启动桥接器（阻塞直到连接关闭）
	bridge.Serve()

	// 清理
	s.beanRegister.RemoveConnection(clientID)
	s.connsMu.Lock()
	delete(s.conns, bridge)
	s.connsMu.Unlock()
}

// ServeConn 为已有的 protoq 连接创建 ConnectionBridge。
//
// 用于已建立的 Conn（如通过自定义传输层创建的连接）。
// 不会分配新的连接 ID，使用 Conn 已有的 ID。
func (s *MessageServer) ServeConn(conn *netty.Conn) {
	bridge := netty.NewConnectionBridge(conn, s.beanRegister)

	clientID := formatClientID(conn.ConnectionID())
	s.beanRegister.AddConnection(clientID, conn)

	s.connsMu.Lock()
	s.conns[bridge] = struct{}{}
	s.connsMu.Unlock()

	bridge.Serve()

	s.beanRegister.RemoveConnection(clientID)
	s.connsMu.Lock()
	delete(s.conns, bridge)
	s.connsMu.Unlock()
}

// ConnectionCount 返回当前活跃连接数。
//
// 对标 uni-protocol MessageServer.getConnectionCount()。
func (s *MessageServer) ConnectionCount() int {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	return len(s.conns)
}

// IsRunning 返回服务器是否正在运行。
//
// 对标 uni-protocol MessageServer.isRunning()。
func (s *MessageServer) IsRunning() bool {
	return s.running.Load()
}

// SetRunning 设置运行状态。
func (s *MessageServer) SetRunning(running bool) {
	s.running.Store(running)
}

// ─── 辅助函数 ────────────────────────────────────────────────────────────────

// formatClientID 格式化连接 ID 为 clientID 字符串。
func formatClientID(connID uint64) string {
	return "conn-" + formatUint64(connID)
}

// formatUint64 快速格式化 uint64 为字符串（避免 fmt.Sprintf 开销）。
func formatUint64(v uint64) string {
	if v == 0 {
		return "0"
	}
	buf := make([]byte, 20)
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}
