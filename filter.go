// Package protoq — 过滤器与中间件链接口
//
// 对标 Java uni-protocol 的 Filter / FilterChain / FilterChainCompleted 设计。
// Filter 可注册到 FilterChainRegister，在消息分发到业务 Handler 之前执行。
// 典型用途：协商检查、认证鉴权、日志、限流等。
package protoq

// Filter 消息过滤器（中间件）。对标 uni-protocol org.facelang.unified.proto.api.Filter。
//
// 实现者通过 chain.DoFilter(ctx) 调用继续执行链；
// 不调用 chain.DoFilter() 即阻断消息，不会到达后续 Filter 和业务 Handler。
// 抛出的异常由 ConnectionBridge 统一处理。
type Filter interface {
	// DoFilter 执行过滤逻辑。
	// ctx: 当前请求上下文（含 Connection、Frame、响应载体）
	// chain: 过滤器链，调用 chain.DoFilter(ctx) 继续
	DoFilter(ctx Context, chain FilterChain) error
}

// FilterChain 过滤器链迭代器。对标 uni-protocol org.facelang.unified.proto.api.FilterChain。
//
// 由 FilterChainRegister 内部实现（Continuation），
// DoFilter 每次调用推进到下一个 Filter，全部通过后执行 FilterChainCompleted。
type FilterChain interface {
	// DoFilter 继续执行下一个 Filter 或触达链尾回调。
	DoFilter(ctx Context) error
}

// FilterChainCompleted 过滤器链全部通过后的回调函数。对标 uni-protocol FilterChainCompleted。
//
// 通常由 MessageDispatcher 注入，执行按 messageId 分发到业务 Handler 的逻辑。
type FilterChainCompleted func(ctx Context) error
