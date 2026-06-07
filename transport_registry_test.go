package protoq

import (
	"testing"

	"github.com/oh-marshal/protoq/transport"
)

// TestTransportSatisfiesInterfaces 编译期验证传输实现满足接口约定。
func TestTransportSatisfiesInterfaces(t *testing.T) {
	var _ Transport = transport.NewTCPTransport()
	var _ Transport = transport.NewWSTransport()
	var _ Transport = transport.NewQUICTransport()

	var _ Dialer = transport.NewTCPTransport()
	var _ Dialer = transport.NewWSTransport()
	var _ Dialer = transport.NewQUICTransport()

	var _ ListenerFactory = transport.NewTCPTransport()
	var _ ListenerFactory = transport.NewWSTransport()
	var _ ListenerFactory = transport.NewQUICTransport()
}
