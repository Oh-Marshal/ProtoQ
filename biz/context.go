// Package biz — 请求上下文
package biz

import (
	"context"
	"time"
)

// Context 封装业务请求的上下文信息。
// 包含连接标识、元数据和截止时间。
type Context struct {
	// Ctx 标准库 context，用于超时和取消控制
	Ctx context.Context

	// ConnID 连接标识（服务端分配）
	ConnID uint64

	// SessionID 会话标识（协商阶段分配）
	SessionID string

	// Opcode 当前请求的操作码
	Opcode uint32

	// Deadline 请求截止时间
	Deadline time.Time

	// Metadata 附加元数据（中间件可读写）
	Metadata map[string]interface{}
}

// NewContext 创建一个新的请求上下文。
func NewContext(ctx context.Context, connID uint64, sessionID string, opcode uint32) *Context {
	if ctx == nil {
		ctx = context.Background()
	}
	bizCtx := &Context{
		Ctx:       ctx,
		ConnID:    connID,
		SessionID: sessionID,
		Opcode:    opcode,
		Metadata:  make(map[string]interface{}),
	}
	if deadline, ok := ctx.Deadline(); ok {
		bizCtx.Deadline = deadline
	}
	return bizCtx
}

// WithTimeout 创建带超时的子上下文。
func (c *Context) WithTimeout(timeout time.Duration) (*Context, context.CancelFunc) {
	childCtx, cancel := context.WithTimeout(c.Ctx, timeout)
	newCtx := *c // 浅拷贝
	newCtx.Ctx = childCtx
	if deadline, ok := childCtx.Deadline(); ok {
		newCtx.Deadline = deadline
	}
	return &newCtx, cancel
}

// Set 设置元数据。
func (c *Context) Set(key string, value interface{}) {
	c.Metadata[key] = value
}

// Get 获取元数据。
func (c *Context) Get(key string) (interface{}, bool) {
	v, ok := c.Metadata[key]
	return v, ok
}
