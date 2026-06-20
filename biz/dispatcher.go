// Package biz — 消息与事件分发器
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.register.dispatcher。
// MessageDispatcher：按 messageID 将请求帧分发到已注册的 MessageHandler。
// EventDispatcher：按事件名将事件分发到已注册的 EventHandler。
//
// 响应帧直接完成 MessageQueue 中等待的 Future，不进入业务分发。
package biz

import (
	"fmt"
	"sync"

	api "github.com/oh-marshal/protoq"
)

// MessageDispatcher 消息分发器。
//
// 对标 Java uni-protocol MessageDispatcher。
// 组合 FilterChainRegister（过滤器链）和 HandlerRegistry（消息处理器注册表），
// 负责将解码后的请求帧经过滤器链后按 messageID 分发到对应的 MessageHandler。
//
// 分发逻辑（对标 uni-protocol dispatch 方法）：
//  1. conn 或 frame 为 nil → 返回 nil
//  2. frame 为响应帧：从 MessageQueue 中完成对应 sequence 的 Future → 返回 nil
//  3. frame 为请求帧：创建 MessageContext → 经过滤器链 → 按 messageID 查找 Handler → 执行 → 设置响应
type MessageDispatcher struct {
	mu              sync.RWMutex
	filterChain     *FilterChainRegister
	handlerRegistry map[uint32]api.MessageHandler
}

// NewMessageDispatcher 创建消息分发器。
//
// 对标 uni-protocol MessageDispatcher 构造器。
// 初始化空的过滤器链和 Handler 注册表。
// 内置处理器（如协商、心跳）需在构造后通过 RegisterHandler 或 BeanRegister 注册。
func NewMessageDispatcher() *MessageDispatcher {
	return &MessageDispatcher{
		filterChain:     &FilterChainRegister{},
		handlerRegistry: make(map[uint32]api.MessageHandler),
	}
}

// FilterChain 返回关联的过滤器链注册表。
//
// 对标 uni-protocol MessageDispatcher.getFilterChain()。
// 调用方可通过此方法添加自定义 Filter（如认证、鉴权、日志、限流）。
func (d *MessageDispatcher) FilterChain() *FilterChainRegister {
	return d.filterChain
}

// RegisterHandler 按 messageID 注册消息处理器。
//
// 对标 uni-protocol MessageHandlerRegister.register(commandCode, bean, method)。
// Go 惯用法：直接注册函数，无需反射/注解。
//
// messageID: 消息类型标识（使用 Frame.Opcode 的低字节，对标 uni-protocol 的 1 字节 messageId）
// handler: 消息处理函数，签名 func(ctx api.Context) ([]byte, error)
func (d *MessageDispatcher) RegisterHandler(messageID uint32, handler api.MessageHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlerRegistry[messageID] = handler
}

// GetHandler 按 messageID 获取已注册的处理器。未找到返回 nil。
func (d *MessageDispatcher) GetHandler(messageID uint32) api.MessageHandler {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.handlerRegistry[messageID]
}

// Dispatch 分发入站消息。
//
// 对标 uni-protocol MessageDispatcher.dispatch(conn, packet)。
//
// 分发流程：
//  1. conn 或 frame 为 nil → 返回 (nil, nil)
//  2. frame.IsResponse() → 完成 MessageQueue 中对应 sequence 的等待 channel → 返回 (nil, nil)
//  3. frame.IsRequest() → 创建 MessageContext → 经过滤器链 FilterChain.DoFilter
//     → 链尾回调中按 messageID 查找 Handler → handler(ctx) → 返回值非 nil 时 SetResponse
//
// 返回值：
//   - *MessageContext: 分发后的上下文（含 SetResponse 设置的回写数据），由 Bridge 写出响应帧
//   - error: 过滤器链或 Handler 执行中的错误
func (d *MessageDispatcher) Dispatch(conn api.Connection, frame *api.Frame) (*MessageContext, error) {
	if conn == nil || frame == nil {
		return nil, nil
	}

	// ── 响应帧：完成等待队列中的 Future ──
	if frame.IsResponse() {
		queue := conn.MessageQueue()
		if queue != nil {
			queue.Complete(frame.Seq, frame)
		}
		return nil, nil
	}

	// ── 请求帧：创建上下文并经过滤器链分发 ──
	ctx := NewMessageContext(conn, frame)

	// 提取 messageID（使用 Opcode 低字节，对标 uni-protocol 的 1 字节 messageId）
	messageID := frame.Opcode & 0xFF

	// 过滤器链完成回调：按 messageID 查找 Handler 并执行
	completed := api.FilterChainCompleted(func(ctx api.Context) error {
		d.mu.RLock()
		handler := d.handlerRegistry[messageID]
		d.mu.RUnlock()

		if handler == nil {
			return fmt.Errorf("protoq/biz: 未注册的消息处理器：messageID=0x%02X", messageID)
		}

		result, err := handler(ctx)
		if err != nil {
			return fmt.Errorf("protoq/biz: Handler 执行失败 (messageID=0x%02X): %w", messageID, err)
		}

		if result != nil {
			ctx.SetResponse(result)
		}
		return nil
	})

	err := d.filterChain.DoFilter(ctx, completed)
	if err != nil {
		return ctx, err
	}

	return ctx, nil
}

// EventDispatcher 事件分发器。
//
// 对标 Java uni-protocol EventDispatcher。
// 按事件名分发到已注册的 EventHandler。
// 事件通常由协商阶段（ADD_EVENT 协商项）或运行时 conn.Emit() 触发。
//
// 分发逻辑（对标 uni-protocol dispatch 方法）：
//  1. conn 或 event 为空 → 返回 nil
//  2. 按 event 查找 EventHandler
//  3. 未找到 → 返回错误
//  4. 找到 → 调用 handler(conn, message)
type EventDispatcher struct {
	mu              sync.RWMutex
	handlerRegistry map[string]api.EventHandler
}

// NewEventDispatcher 创建事件分发器。
func NewEventDispatcher() *EventDispatcher {
	return &EventDispatcher{
		handlerRegistry: make(map[string]api.EventHandler),
	}
}

// RegisterHandler 按事件名注册事件处理器。
//
// 对标 uni-protocol EventHandlerRegister.register(event, bean, method)。
// Go 惯用法：直接注册函数。
//
// event: 事件名称（对应 conn.Emit(event, message) 的第一个参数）
// handler: 事件处理函数，签名 func(conn api.Connection, message interface{}) error
func (d *EventDispatcher) RegisterHandler(event string, handler api.EventHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.handlerRegistry[event] = handler
}

// GetHandler 按事件名获取已注册的处理器。未找到返回 nil。
func (d *EventDispatcher) GetHandler(event string) api.EventHandler {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.handlerRegistry[event]
}

// Dispatch 分发事件。
//
// 对标 uni-protocol EventDispatcher.dispatch(conn, event, message)。
//
// 分发流程：
//  1. conn 或 event 为空 → 返回 nil
//  2. 按 event 查找 EventHandler
//  3. 未找到 → 返回错误
//  4. 找到 → 调用 handler(conn, message)
func (d *EventDispatcher) Dispatch(conn api.Connection, event string, message interface{}) error {
	if conn == nil || event == "" {
		return nil
	}

	d.mu.RLock()
	handler := d.handlerRegistry[event]
	d.mu.RUnlock()

	if handler == nil {
		return fmt.Errorf("protoq/biz: 未注册的事件处理器：event=%s", event)
	}

	return handler(conn, message)
}
