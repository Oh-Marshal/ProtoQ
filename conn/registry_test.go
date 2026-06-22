package conn_test

import (
	"testing"

	protoq "github.com/oh-marshal/protoq"
	conn "github.com/oh-marshal/protoq/conn"
)

// TestTransportSatisfiesInterfaces 编译期验证传输实现满足接口约定。
func TestTransportSatisfiesInterfaces(t *testing.T) {
	var _ protoq.Transport = conn.NewTCPTransport()
	var _ protoq.Transport = conn.NewWSTransport()
	var _ protoq.Transport = conn.NewQUICTransport()

	var _ protoq.Dialer = conn.NewTCPTransport()
	var _ protoq.Dialer = conn.NewWSTransport()
	var _ protoq.Dialer = conn.NewQUICTransport()

	var _ protoq.ListenerFactory = conn.NewTCPTransport()
	var _ protoq.ListenerFactory = conn.NewWSTransport()
	var _ protoq.ListenerFactory = conn.NewQUICTransport()
}
