// Package protoq — 共享连接抽象
//
// Conn 封装了底层 net.Conn、帧编解码器和连接生命周期（context），
// 提供线程安全的读写操作。
// 同时被服务端（通过 ConnContext）和客户端（通过 Client）复用。
package protoq

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn 是 ProtoQ 连接的通用抽象。
// 拥有连接的完整生命周期：底层 socket、帧解码、写锁、context 取消。
type Conn struct {
	raw     net.Conn
	decoder *Decoder

	writeMu sync.Mutex
	closed  atomic.Bool

	ctx    context.Context
	cancel context.CancelFunc
}

// NewConn 从已建立的 net.Conn 创建 ProtoQ 连接。
// parentCtx 用于派生连接的 context：当 parentCtx 被取消时，连接自动取消。
// 服务端传入 server.ctx，客户端传入 context.Background()。
func NewConn(parentCtx context.Context, raw net.Conn) *Conn {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Conn{
		raw:     raw,
		decoder: NewDecoder(raw),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Decode 从流中解码一个完整的 ProtoQ 帧。
// 委托给内部流式解码器，处理粘包、半包和噪声同步。
func (c *Conn) Decode() (*Frame, error) {
	return c.decoder.Decode()
}

// WriteFrame 线程安全地向连接写入一个帧。
// 写入前设置 5 秒写截止时间。
func (c *Conn) WriteFrame(f *Frame) error {
	if c.closed.Load() {
		return ErrConnClosed
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed.Load() {
		return ErrConnClosed
	}

	c.raw.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := EncodeTo(f, c.raw)
	return err
}

// Close 关闭连接（幂等）。先取消 context，再关闭底层连接。
func (c *Conn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.cancel()
	return c.raw.Close()
}

// IsClosed 返回连接是否已关闭。
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

// Context 返回连接的 context.Context（连接关闭时自动取消）。
func (c *Conn) Context() context.Context {
	return c.ctx
}

// Raw 返回底层 net.Conn（供传输层等内部使用）。
func (c *Conn) Raw() net.Conn {
	return c.raw
}
