// Package protoq 实现 ProtoQ 自定义网络协议。
//
// ProtoQ 是一种面向消息的二进制协议，支持请求-应答和单向通知两种通信模式。
// 协议帧采用变长编码，字段存在性由 Flags 位图控制，支持 CRC 校验和四字节对齐。
//
// # 协议帧格式
//
// 变体 A（有 Body）：
//
//	[Magic:1][Flags:1][Length:2][Opcode:0/2/4][Seq:0/2/4][Body:N][CRC:0/2][Padding:0-3]
//
// 变体 B（无 Body）：
//
//	[Magic:1][Flags:1][Opcode:0/2/4][Seq:0/2/4][CRC:0/2][Padding:0-3]
//
// # 架构分层
//
// 根包 protoq 提供核心协议接口与类型：
//   - Conn：连接抽象（底层 socket、帧编解码、context 生命周期）
//   - ConnContext：服务端连接上下文（Conn + 连接 ID + 元数据）
//   - ConnHandler：服务端请求处理函数签名
//   - Server / Client：服务端与客户端生命周期管理
//   - Frame / Flags / Decoder / Encoder：协议帧的编解码与表示
//   - SeqManager：序列号分配与请求-响应匹配
//
// 子包 transport/ 提供传输层实现（TCP、WebSocket、QUIC），
// 通过实现根包定义的 Transport / Dialer / ListenerFactory 接口接入。
//
// 子包 biz/ 提供业务协议层配置（Opcode 分区、协商、心跳），
// 通过 MessageServer（注册中心模式）+ ConnectionBridge（连接桥接）提供服务端能力，
// 通过 Negotiate() / StartHeartbeat() 提供客户端辅助函数。
//
// # 快速开始
//
// 服务端：
//
//	server := protoq.NewServer(transport.NewTCPTransport())
//	server.Handle(0x0001, func(ctx *protoq.ConnContext, opcode uint32, body []byte) ([]byte, error) {
//	    return []byte("ECHO: " + string(body)), nil
//	})
//	server.ListenAndServe(ctx, ":9090")
//
// 客户端：
//
//	client, _ := protoq.Dial(ctx, transport.NewTCPTransport(), ":9090")
//	resp, _ := client.SendRequest(ctx, 0x0001, []byte("hello"))
//	client.Close()
//
// # 传输层支持
//
//   - TCP（transport.NewTCPTransport()）
//   - WebSocket（transport.NewWSTransport()）
//   - QUIC（transport.NewQUICTransport()）
package api
