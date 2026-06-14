package transport

import (
	"context"
	"net"
)

// TCPTransport 基于标准 TCP 的传输实现。
type TCPTransport struct{}

// NewTCPTransport 创建一个 TCP 传输实例。
func NewTCPTransport() *TCPTransport {
	return &TCPTransport{}
}

// Dial 使用 TCP 连接到指定地址。
func (t *TCPTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

// Listen 使用 TCP 在指定地址上监听。
func (t *TCPTransport) Listen(ctx context.Context, addr string) (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(ctx, "tcp", addr)
}

// Protocol 返回传输协议名称。
func (t *TCPTransport) Protocol() string {
	return "tcp"
}
