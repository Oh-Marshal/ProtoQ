// Package biz — Handler 类型定义
//
// biz 层提供类型安全的 Handler 签名：
//   - Handler: 接收 *Context 和原始 []byte，返回 []byte 或 error
//   - 序列化/反序列化由 Router 层通过 Codec 自动处理
package biz

// Handler 是业务层的请求处理函数。
// ctx: 请求上下文（连接信息、截止时间、元数据）
// body: 请求体（JSON 编码的字节）
// 返回值：响应体（JSON 编码的字节）和可能的错误。
// 若返回 error，Router 会将其编码为错误响应帧。
type Handler func(ctx *Context, body []byte) ([]byte, error)

// HandlerFunc 将命名函数适配为 Handler 类型。
type HandlerFunc func(ctx *Context, body []byte) ([]byte, error)

func (f HandlerFunc) Handle(ctx *Context, body []byte) ([]byte, error) {
	return f(ctx, body)
}

// Middleware 中间件函数：包装一个 Handler 并返回新的 Handler。
// 典型用法：日志、认证、限流、panic 恢复。
type Middleware func(next Handler) Handler
