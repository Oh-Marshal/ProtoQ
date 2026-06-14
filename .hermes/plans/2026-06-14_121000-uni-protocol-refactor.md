# ProtoQ 对标 uni-protocol 架构重构计划

> 目标: 将 ProtoQ 项目重构为与 uni-protocol Java 参考项目 98%+ 设计还原度
> 协议头差异保留（不可消除的根本差异），其余架构全部对齐

## 差异分析

### 不可消除的差异（协议帧格式）

| 对比点 | uni-protocol V3 | ProtoQ |
|--------|----------------|--------|
| 哨兵 | 0xFE...0xFF (双哨兵) | 0x51 单字节 Magic |
| 消息类型 | 1 字节 messageId | 2/4 字节 Opcode |
| 序列号 | 4 字节长整型 | 2/4 字节 |
| 标志位 | FLAG_RESPONSE/REQUIRE_ACK/RETRANSMIT/CRC32/ENCRYPTED+len | DIR/ACK_REQ/BODY_LEN/OPLEN/SEQLEN/CRCLEN |
| 长度编码 | 0/1/2/4 字节数据区长度 | 固定 2 字节 Length |
| CRC | CRC32 (4 字节) | CRC-16-IBM (2 字节) |
| 加解密 | 内置 FLAG_ENCRYPTED + Codec | 无（biz 层 JSON 协商） |

### 需完全对齐的设计

| uni-protocol 特性 | ProtoQ 现状 | 需实现 |
|-------------------|-----------|--------|
| Filter/FilterChain 中间件 | 无 | **新增** |
| FilterChainRegister | 无 | **新增** |
| BeanRegister 统一注册 | 无 | **新增** |
| CodecRegister | 无 | **新增** |
| MessageDispatcher | 无 | **新增** |
| MessageHandlerRegister | 无 | **新增** |
| EventDispatcher + EventHandlerRegister | 无 | **新增** |
| Codec/Converter 分层 | 无 | **新增** |
| MessageQueue (ACK队列) | PendingRequest (私下管理) | **重构** |
| Connection 接口抽象 | Conn 具体结构体 | **新增接口 + 实现** |
| Context (请求上下文) | ConnContext | **重构** |
| 协商过滤器 NegotiateFilter | biz 内联 handler | **重构为 Filter** |
| 内置 Handler (Negotiate/Heartbeat) | biz server.go | **重构为 HandlerEntry** |
| 响应 messageId=请求+1 | 无约定 | **新增** |
| 错误码系统 | 无 | **新增** |

## 实现步骤 (35 任务)

### Phase 1: API 接口层 (6 任务)
1. 重构 Connection 为接口，新增 MessageQueue
2. 新增 Filter + FilterChain 接口
3. 新增 Codec + Converter 接口
4. 重构 Context 接口（PacketData→Frame）
5. 新增 MessageHandler + EventHandler 类型
6. 新增 FilterChainCompleted 回调

### Phase 2: 注册中心 (6 任务)
7. 实现 FilterChainRegister (含 Continuation)
8. 实现 MessageHandlerRegister
9. 实现 EventHandlerRegister
10. 实现 CodecRegister
11. 实现 MessageDispatcher
12. 实现 EventDispatcher

### Phase 3: 编解码层 (3 任务)
13. 实现 DefaultCodec (明文透传)
14. 实现 JSONConverter
15. 实现 MessageContext

### Phase 4: 过滤器 + 内置处理器 (4 任务)
16. 实现 NegotiateFilter
17. 实现 NegotiatePayloadHandler
18. 实现 HeartbeatPayloadHandler
19. 实现 BeanRegister (统一注册)

### Phase 5: 连接 + 桥接 (4 任务)
20. 重构 Conn 为 DefaultConnection (实现 Connection 接口)
21. 实现 ConnectionBridge (入站/出站编排)
22. 重构 Server (使用 BeanRegister + ConnectionBridge)
23. 重构 Client (使用 Connection + MessageQueue)

### Phase 6: biz 层重构 (4 任务)
24. 重构 biz.ServerRecipe 使用 BeanRegister
25. 重构 biz.Negotiate 使用 Connection 接口
26. 重构 biz.StartHeartbeat 使用 Connection 接口
27. 删除过时的 biz Context/Handler/Middleware

### Phase 7: 示例 + 测试 (5 任务)
28. 更新 examples/echo 使用新 API
29. 更新 biz 测试
30. 更新 protoq 测试
31. 接口合规测试
32. 端到端冒烟测试

### Phase 8: 文档 + 清理 (3 任务)
33. 更新 DESIGN.md
34. 更新 doc.go
35. git commit + push
