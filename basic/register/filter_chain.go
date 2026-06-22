// Package biz — 过滤器链注册与执行
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.register.FilterChainRegister。
// FilterChainRegister 持有 Filter 列表，在消息分发到业务 Handler 之前依次执行。
// 内部通过 Continuation 实现 FilterChain 接口，执行前拍 snapshot 避免并发问题。
package register

import (
	"sync"

	api "github.com/oh-marshal/protoq"
	dispatcher "github.com/oh-marshal/protoq/basic/register/dispatcher"
)

// FilterChainRegister 过滤器链注册表与执行器。
//
// 对标 uni-protocol FilterChainRegister：持有 Filter 列表，
// 在 MessageDispatcher.dispatch 时先经过滤器链，全部通过后执行业务分发。
//
// 设计要点：
//   - 执行前拍 snapshot（复制 filter 列表），避免运行时增删 Filter 导致并发问题
//   - 检查连接是否已关闭，提前终止链
//   - 通过内部结构体 continuation 实现 api.FilterChain 接口
//   - 全部 Filter 通过后调用 FilterChainCompleted（业务分发）
type FilterChainRegister struct {
	mu      sync.RWMutex
	filters []api.Filter
}

// AddFilter 向过滤器链末尾添加一个 Filter。
//
// 对标 uni-protocol FilterChainRegister.addFilter(filter)。
// Filter 按添加顺序依次执行；在链中不调用 chain.DoFilter() 即可阻断消息。
func (r *FilterChainRegister) AddFilter(f api.Filter) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.filters = append(r.filters, f)
}

// DoFilter 执行过滤器链。
//
// 对标 uni-protocol FilterChainRegister.doFilter(context, completed)。
// 先拍 snapshot，再通过 continuation 依次调用每个 Filter 的 DoFilter 方法。
// 全部 Filter 通过后调用 completed(ctx)。
//
// 参数：
//   - ctx: 当前请求上下文（含 Connection、PacketData、响应载体）
//   - completed: 过滤器链全部通过后的回调（通常由 MessageDispatcher 注入业务分发逻辑）
func (r *FilterChainRegister) DoFilter(ctx api.Context, completed api.FilterChainCompleted) error {
	r.mu.RLock()
	snapshot := make([]api.Filter, len(r.filters))
	copy(snapshot, r.filters)
	r.mu.RUnlock()

	continuation := &continuation{
		snapshot:  snapshot,
		completed: completed,
	}
	return continuation.DoFilter(ctx)
}

// continuation 是 FilterChain 接口的内部实现。
//
// 对标 Java uni-protocol FilterChainRegister.Continuation 内部类。
// 持有 snapshot 副本和执行序号 index，每次 DoFilter 推进到下一个 Filter。
// 全部 Filter 执行完毕后调用 FilterChainCompleted.run()（即 completed(ctx)）。
type continuation struct {
	snapshot  []api.Filter
	completed api.FilterChainCompleted
	index     int
}

// DoFilter 实现 api.FilterChain 接口。
//
// 对标 uni-protocol Continuation.doFilter(context)。
//
// 执行逻辑：
//  1. 连接已关闭 → 提前终止（返回 nil）
//  2. 还有未执行的 Filter → 取出当前 Filter，index++，调用 filter.DoFilter(ctx, this)
//  3. 全部执行完毕 → 调用 completed(ctx) 执行业务分发
func (c *continuation) DoFilter(ctx api.Context) error {
	// 连接已关闭则提前终止链
	if ctx.Connection() != nil && ctx.Connection().IsClosed() {
		return nil
	}

	if c.index < len(c.snapshot) {
		filter := c.snapshot[c.index]
		c.index++
		return filter.DoFilter(ctx, c)
	}

	// 全部 Filter 执行完毕，执行完成回调（业务分发）
	return c.completed(ctx)
}
