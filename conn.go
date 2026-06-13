// Package protoq — 共享连接抽象
//
// Conn 封装了底层 net.Conn 和帧编解码器，提供线程安全的读写操作。
// 同时被服务端（通过 ConnContext）和客户端（通过 Client）复用。
package protoq

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Conn 是 ProtoQ 连接的通用抽象，封装了底层网络连接和帧级读写。
// 服务端和客户端均使用此类型，各自在其上构建生命周期管理。
type Conn struct {
	raw     net.Conn
	decoder *Decoder

	writeMu sync.Mutex
	closed  atomic.Bool
}

// NewConn 从已建立的 net.Conn 创建 ProtoQ 连接。
func NewConn(raw net.Conn) *Conn {
	return &Conn{
		raw:     raw,
		decoder: NewDecoder(raw),
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

// Close 关闭连接（幂等）。
func (c *Conn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.raw.Close()
}

// IsClosed 返回连接是否已关闭。
func (c *Conn) IsClosed() bool {
	return c.closed.Load()
}

// Raw 返回底层 net.Conn（供传输层等内部使用）。
func (c *Conn) Raw() net.Conn {
	return c.raw
}
