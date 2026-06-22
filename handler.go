// Package protoq — 消息处理器与事件处理器类型
//
// 对标 Java uni-protocol 的 @MessageHandler / @EventHandler 注解机制。
// Go 无注解，使用类型化函数签名 + 显式注册。
package protoq

// MessageHandler 消息处理器函数签名。对标 uni-protocol @MessageHandler。
//
// 按 messageId（Opcode 的低字节，对应 uni-protocol 的 1 字节 messageId）分发。
// ctx: 请求上下文（含 Connection + PacketData）
// 返回值：响应体（[]byte），由 Bridge 编码后写回。
type MessageHandler func(ctx Context) ([]byte, error)

// EventHandler 事件处理器函数签名。对标 uni-protocol @EventHandler。
//
// 按事件名分发，由 EventDispatcher 触发。
// conn: 触发事件的连接
// message: 事件携带的数据
type EventHandler func(conn Connection, message interface{}) error
