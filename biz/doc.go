// Package biz 提供 ProtoQ 协议之上的业务协议层配置与辅助函数。
//
// biz 层对标 Java uni-protocol protocol-basic，提供：
//   - Opcode 三区段划分（系统预留 / 业务自定义 / 系统异常）
//   - 内容协商协议（NegotiateFilter + NegotiatePayloadHandler）
//   - 心跳协议（HeartbeatPayloadHandler + StartHeartbeat）
//   - 消息分发器（MessageDispatcher）+ 过滤器链（FilterChainRegister）
//   - 注册中心（BeanRegister：CodecRegister + FilterChain + MessageDispatcher）
//   - ConnectionBridge：连接级消息桥接器（对标 NettyMessageBridge）
//   - MessageServer：协议消息服务器（对标 NettyMessageServer）
//   - Negotiate / StartHeartbeat：客户端辅助函数
//
// Opcode 分区（2 字节）：
//
//	0x0000 – 0x00FF   系统预留（协商、心跳等）
//	0x0100 – 0xFEFF   业务自定义
//	0xFF00 – 0xFFFF   系统异常
//
// 服务端使用示例（新 API）：
//
//	srv := biz.NewMessageServer(biz.WithNegotiator(myNegotiator))
//	srv.RegisterMessageHandler(0x0100, echoHandler)  // 业务 Handler
//
//	listener, _ := net.Listen("tcp", ":9090")
//	for {
//	    rawConn, _ := listener.Accept()
//	    go srv.Serve(rawConn)
//	}
//
// 客户端使用示例：
//
//	client, _ := protoq.Dial(ctx, transport.NewTCPTransport(), ":9090")
//	biz.Negotiate(ctx, client.Conn, biz.WithAuth("token"))
//	stopHeartbeat := biz.StartHeartbeat(client.Conn, nil)
//	defer stopHeartbeat()
//
//	resp, _ := client.SendRequest(ctx, 0x0100, body)
//	client.Close()
package biz
