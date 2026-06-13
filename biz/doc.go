// Package biz 提供 ProtoQ 协议之上的业务协议层配置与辅助函数。
//
// biz 层是一组纯配置和编排规则，不管理连接生命周期。
// 核心启动仍由 protoq 包直接负责，biz 通过配置注入驱动 protoq 运行。
//
// 功能：
//   - Opcode 三区段划分（系统预留 / 业务自定义 / 系统异常）
//   - 内容协商协议（连接建立后自动执行）
//   - 心跳协议（客户端自动发送，服务端自动响应+超时检测）
//   - ServerRecipe：服务端业务协议配置（注入 protoq.Server）
//   - Negotiate / StartHeartbeat：客户端辅助函数
//   - 中间件接口（预留）
//
// Opcode 分区（2 字节）：
//
//	0x0000 – 0x00FF   系统预留（协商、心跳等）
//	0x0100 – 0xFEFF   业务自定义
//	0xFF00 – 0xFFFF   系统异常
//
// 服务端使用示例：
//
//	server := protoq.NewServer(transport.NewTCPTransport())
//	recipe := &biz.ServerRecipe{}  // 零值可用
//	recipe.Apply(server)
//
//	server.Handle(0x0100, echoHandler)        // 业务操作码
//	server.Handle(0x0101, connAwareHandler) // 带连接上下文的业务操作码
//
//	server.ListenAndServe(ctx, ":9090")
//
// 客户端使用示例：
//
//	client, _ := protoq.Dial(ctx, transport.NewTCPTransport(), ":9090")
//	biz.Negotiate(ctx, client, biz.WithAuth("token"))
//	stopHeartbeat := biz.StartHeartbeat(client, nil)
//	defer stopHeartbeat()
//
//	resp, _ := client.SendRequest(ctx, 0x0100, body)
//	client.Close()
package biz
