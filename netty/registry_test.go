package netty_test

import (
	"testing"

	protoq "github.com/oh-marshal/protoq"
	netty "github.com/oh-marshal/protoq/netty"
)

// TestTransportSatisfiesInterfaces 编译期验证传输实现满足接口约定。
func TestTransportSatisfiesInterfaces(t *testing.T) {
	var _ protoq.Transport = netty.NewTCPTransport()
	var _ protoq.Transport = netty.NewWSTransport()
	var _ protoq.Transport = netty.NewQUICTransport()

	var _ protoq.Dialer = netty.NewTCPTransport()
	var _ protoq.Dialer = netty.NewWSTransport()
	var _ protoq.Dialer = netty.NewQUICTransport()

	var _ protoq.ListenerFactory = netty.NewTCPTransport()
	var _ protoq.ListenerFactory = netty.NewWSTransport()
	var _ protoq.ListenerFactory = netty.NewQUICTransport()
}
