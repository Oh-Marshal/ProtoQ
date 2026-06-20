package protoq

import (
	"context"
	"net"
)

// Transport 定义 ProtoQ 的底层传输协议接口。
// 支持 TCP、WebSocket 以及 QUIC（需第三方库）等传输层。
type Transport interface {
	// Dial 连接到指定地址并返回一个可用于 ProtoQ 通信的连接。
	Dial(ctx context.Context, addr string) (net.Conn, error)

	// Listen 在指定地址上监听并返回 Listener。
	Listen(ctx context.Context, addr string) (net.Listener, error)

	// Protocol 返回传输协议名称（如 "tcp", "ws", "quic"）。
	Protocol() string
}

// Dialer 是仅用于客户端的传输接口。
type Dialer interface {
	Dial(ctx context.Context, addr string) (net.Conn, error)
	Protocol() string
}

// ListenerFactory 是仅用于服务端的传输接口。
type ListenerFactory interface {
	Listen(ctx context.Context, addr string) (net.Listener, error)
	Protocol() string
}
