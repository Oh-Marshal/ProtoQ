// Package biz 提供 ProtoQ 协议之上的业务协议层。
//
// biz 层封装了 protoq.Client 和 protoq.Server，提供：
//   - 类型安全的 Handler 签名（参数为具体 Go 类型，而非 []byte）
//   - 基于 Opcode 的方法路由
//   - 可插拔的序列化/反序列化（默认 JSON）
//   - 请求上下文（连接信息、元数据、截止时间）
//   - 中间件链（认证、限流、日志、恢复等）
//
// 使用方式：
//
//	import (
//	    "github.com/oh-marshal/protoq"
//	    "github.com/oh-marshal/protoq/biz"
//	    "github.com/oh-marshal/protoq/transport"
//	)
//
//	// 服务端
//	server := biz.NewServer(protoq.NewServer(transport.NewTCPTransport()))
//	server.Handle("echo", func(ctx *biz.Context, req *EchoRequest) (*EchoResponse, error) {
//	    return &EchoResponse{Message: "ECHO: " + req.Message}, nil
//	})
//	server.ListenAndServe(ctx, ":9090")
//
//	// 客户端
//	client := biz.NewClient(protoq.Dial(ctx, transport.NewTCPTransport(), ":9090"))
//	var resp EchoResponse
//	client.Call(ctx, "echo", &EchoRequest{Message: "hello"}, &resp)
package biz
