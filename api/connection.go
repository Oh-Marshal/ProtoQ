// Package protoq — 连接抽象接口
//
// 对标 Java uni-protocol 的 Connection 接口。
// 隐藏底层网络实现，提供统一的连接属性、消息队列、编解码、发送/写回能力。
package api

import (
	"net"
	"time"
)

// Connection 连接抽象接口。对标 uni-protocol org.facelang.unified.proto.api.Connection。
//
// 服务端和客户端通过此接口操作连接，无需关心底层是 TCP/WS/QUIC。
// 具体实现由 Conn 结构体提供，通过 struct embedding 在 ConnContext 和 Client 中复用。
type Connection interface {
	// ─── 状态查询 ───

	// IsClosed 检查连接是否已关闭。
	IsClosed() bool

	// Codec 获取当前连接绑定的编解码器。
	// 协商完成后由 NegotiatePayloadHandler 设置 CODEC_TYPE 属性。
	Codec() Codec

	// ConnectionID 获取连接的唯一标识符（服务端内单调递增）。
	ConnectionID() uint64

	// ConnectTime 获取连接建立时间。
	ConnectTime() time.Time

	// ConnectionType 获取连接类型标识（如 "tcp", "ws", "quic"）。
	ConnectionType() string

	// RemoteAddr 获取远程客户端地址。
	RemoteAddr() net.Addr

	// ─── 属性存储 ───

	// GetProperty 按字符串键获取连接上的附加属性。
	// 对标 uni-protocol Connection.getProperty(key)。
	GetProperty(key string) (interface{}, bool)

	// SetProperty 按字符串键设置连接上的附加属性。
	// 对标 uni-protocol Connection.setProperty(key, content)。
	SetProperty(key string, value interface{})

	// GetStringProperty 按字符串键获取字符串属性（便捷方法）。
	GetStringProperty(key string) (string, bool)

	// ─── 消息队列 ───

	// MessageQueue 获取当前连接关联的消息队列。
	// 用于异步发送-确认配对，对标 uni-protocol Connection.getMessageQueue()。
	MessageQueue() *MessageQueue

	// ─── 生命周期 ───

	// Close 关闭当前连接并释放资源。
	Close() error

	// ─── 通信 ───

	// Send 向对端发送请求并返回响应 channel（需要 ACK）。
	// 对标 uni-protocol Connection.send(message)。
	// 返回值：响应 Frame 的 channel，由 MessageDispatcher 在收到响应时完成。
	Send(ctx Context, opcode uint32, body []byte) (<-chan *Frame, error)

	// Write 向当前连接写回一条响应消息（被动响应场景）。
	// 对标 uni-protocol Connection.write(message)。
	WriteFrame(frame *Frame) error

	// Emit 向当前连接发送命名事件。
	// 对标 uni-protocol Connection.emit(event, message)。
	Emit(event string, message interface{}) error
}
