// Package transport 提供 ProtoQ 协议的底层传输实现。
//
// 目前支持 TCP 和 WebSocket（纯标准库），QUIC 预留接口桩。
// 所有传输实现均满足 protoq.Transport / protoq.Dialer / protoq.ListenerFactory 接口。
//
// 使用方式：
//
//	import (
//	    "github.com/oh-marshal/protoq"
//	    "github.com/oh-marshal/protoq/transport"
//	)
//
//	// TCP
//	client, _ := protoq.Dial(ctx, transport.NewTCPTransport(), "127.0.0.1:9090")
//	server := protoq.NewServer(transport.NewTCPTransport())
//
//	// WebSocket
//	client, _ := protoq.Dial(ctx, transport.NewWSTransport(), "127.0.0.1:9090")
//	server := protoq.NewServer(transport.NewWSTransport())
package transport
