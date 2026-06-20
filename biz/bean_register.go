// Package biz — 统一 Bean 注册入口
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.register.BeanRegister。
// BeanRegister 聚合 CodecRegister、FilterChainRegister、MessageDispatcher、EventDispatcher、
// Connection 注册表等子注册器。通过 Register(bean) 方法按类型自动分发注册。
//
// Go 惯用法适配：
//   - 无注解机制，Codec/Filter 按类型自动路由
//   - MessageHandler/EventHandler 需通过 RegisterMessageHandler / RegisterEventHandler 显式注册
package biz

import (
	"sync"

	api "github.com/oh-marshal/protoq"
)

// ─── CodecRegister ──────────────────────────────────────────────────────────

// CodecRegister 编解码器注册表。
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.register.CodecRegister。
// 持有 Codec 列表，按 encryptType 匹配选取编解码器。
// 无匹配时返回 DefaultCodec（明文透传）。
//
// 匹配策略：遍历 codecList，调用 codec.Match(encryptType) 返回第一个匹配项；
// 都不匹配时返回 DefaultCodec。
type CodecRegister struct {
	mu        sync.RWMutex
	codecList []api.Codec
}

// NewCodecRegister 创建编解码器注册表。
func NewCodecRegister() *CodecRegister {
	return &CodecRegister{
		codecList: make([]api.Codec, 0),
	}
}

// Register 注册编解码器（去重，nil 忽略）。
//
// 对标 uni-protocol CodecRegister.register(codec)。
func (r *CodecRegister) Register(codec api.Codec) {
	if codec == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// 去重
	for _, existing := range r.codecList {
		if existing == codec {
			return
		}
	}
	r.codecList = append(r.codecList, codec)
}

// Match 按 encryptType 匹配编解码器。
//
// 对标 uni-protocol CodecRegister.match(encrypt)。
// 遍历已注册的 Codec 列表，返回首个 Match(encryptType) 返回 true 的 Codec。
// 都不匹配时返回 DefaultCodec（明文透传）。
func (r *CodecRegister) Match(encryptType uint16) api.Codec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, codec := range r.codecList {
		if codec.Match(encryptType) {
			return codec
		}
	}
	// 无匹配：返回 DefaultCodec 作为兜底（明文透传）
	return &api.DefaultCodec{}
}

// ─── BeanRegister ────────────────────────────────────────────────────────────

// BeanRegister 统一 Bean 注册入口。
//
// 对标 Java uni-protocol BeanRegister。
// 聚合所有子注册器，提供统一的注册入口。
// 通过 Register(bean) 按类型自动路由到对应的子注册器。
//
// 注册路由规则（Go type switch 适配）：
//   api.Codec      → CodecRegister.Register(codec)
//   api.Filter     → FilterChain.AddFilter(filter)
//   其他              → 忽略（需调用专用方法注册 Handler）
type BeanRegister struct {
	// CodecRegister 编解码器注册表
	CodecRegister *CodecRegister
	// FilterChain 过滤器链注册表
	FilterChain *FilterChainRegister
	// MessageDispatcher 消息分发器
	MessageDispatcher *MessageDispatcher
	// EventDispatcher 事件分发器
	EventDispatcher *EventDispatcher

	mu          sync.RWMutex
	connections map[string]api.Connection
}

// NewBeanRegister 创建 Bean 注册中心。
//
// 对标 uni-protocol BeanRegister 构造器。
// 初始化所有子注册器（CodecRegister、FilterChain、MessageDispatcher、EventDispatcher）
// 和连接注册表。
func NewBeanRegister() *BeanRegister {
	return &BeanRegister{
		CodecRegister:     NewCodecRegister(),
		FilterChain:       &FilterChainRegister{},
		MessageDispatcher: NewMessageDispatcher(),
		EventDispatcher:   NewEventDispatcher(),
		connections:       make(map[string]api.Connection),
	}
}

// Register 按类型自动分发注册 Bean。
//
// 对标 uni-protocol BeanRegister.register(bean)。
//
// Go 惯用法类型路由：
//   - api.Codec: 注册到 CodecRegister
//   - api.Filter: 注册到 FilterChain
//   - 其他类型: 忽略（MessageHandler/EventHandler 需调用专用注册方法）
func (r *BeanRegister) Register(bean interface{}) {
	switch v := bean.(type) {
	case api.Codec:
		r.CodecRegister.Register(v)
	case api.Filter:
		r.FilterChain.AddFilter(v)
	default:
		// 未知类型，忽略
		// Handler 需要通过 RegisterMessageHandler / RegisterEventHandler 显式注册
	}
}

// RegisterMessageHandler 按 messageID 注册消息处理器。
//
// 便捷方法，等价于 MessageDispatcher.RegisterHandler(messageID, handler)。
// messageID 使用 Frame.Opcode 的低字节（对标 uni-protocol 的 1 字节 messageId）。
func (r *BeanRegister) RegisterMessageHandler(messageID uint32, handler api.MessageHandler) {
	r.MessageDispatcher.RegisterHandler(messageID, handler)
}

// RegisterEventHandler 按事件名注册事件处理器。
//
// 便捷方法，等价于 EventDispatcher.RegisterHandler(event, handler)。
func (r *BeanRegister) RegisterEventHandler(event string, handler api.EventHandler) {
	r.EventDispatcher.RegisterHandler(event, handler)
}

// ─── 连接注册 ───────────────────────────────────────────────────────────────

// AddConnection 注册连接。
//
// 对标 uni-protocol ConnectionRegister.addClient(clientId, conn)。
func (r *BeanRegister) AddConnection(clientID string, conn api.Connection) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connections[clientID] = conn
}

// RemoveConnection 移除连接。
//
// 对标 uni-protocol ConnectionRegister.removeClient(clientId)。
func (r *BeanRegister) RemoveConnection(clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.connections, clientID)
}

// GetConnection 按 clientID 获取连接。未找到返回 nil。
func (r *BeanRegister) GetConnection(clientID string) api.Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.connections[clientID]
}

// GetAllConnections 获取所有连接（快照）。
//
// 对标 uni-protocol ConnectionRegister.getAllClients()。
func (r *BeanRegister) GetAllConnections() map[string]api.Connection {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := make(map[string]api.Connection, len(r.connections))
	for k, v := range r.connections {
		snapshot[k] = v
	}
	return snapshot
}

// ConnectionCount 返回已注册连接数。
//
// 对标 uni-protocol ConnectionRegister.getOnlineClientCount()。
func (r *BeanRegister) ConnectionCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.connections)
}
