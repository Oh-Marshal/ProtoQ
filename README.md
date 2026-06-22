# ProtoQ

**Pro**tocol + **Q** = ProtoQ。Q 一语双关：既是 **Queue（队列）**——协议的异步消息队列与 ACK 等待机制，也是 **Quick（快速）**——高效多路并发、低延迟的协议本质。ProtoQ 是一款纯 Go 标准库实现的自定义二进制网络协议，支持请求-应答与单向通知，内置 CRC 校验、序列号管理、超时重传、多路并发。

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev/)
[![Deps](https://img.shields.io/badge/deps-zero-blue)](.)

## 特性

- **可变长度帧**：Opcode、Seq、CRC 字段长度由 Flags 位图逐帧控制（0/2/4 字节）
- **两种帧变体**：变体 A 含 Length+Body，变体 B 无 Length+Body（轻量应答）
- **CRC 校验**：CRC-16-IBM（2 字节）和 CRC-32-IEEE（4 字节）
- **序列号管理**：16/32 位自增序列号，待确认队列，指数退避超时重传（最多 3 次）
- **流式解码器**：基于状态机处理 TCP 粘包、半包、噪声同步
- **多传输层**：TCP、WebSocket（纯标准库 RFC 6455）、QUIC（接口预留）
- **内容协商**：连接建立后客户端自动协商协议版本、加密方案、认证凭证
- **过滤器链**：对标 Servlet Filter 的中间件链，支持协商检查、认证鉴权、日志、限流
- **消息分发**：按 messageId 自动分发请求到注册的 Handler，响应帧自动完成 ACK
- **并发安全**：客户端多 goroutine 并发请求，服务端每连接独立 goroutine
- **零依赖**：仅使用 Go 标准库

## 快速开始

### 安装

```bash
git clone git@github.com:Oh-Marshal/ProtoQ.git
cd protoq
```

无需 `go get` 任何第三方包。

### 运行示例

终端 1 — 启动服务端：

```bash
# TCP 模式
go run ./examples/echo/ -mode server -transport tcp -addr :9090

# 或 WebSocket 模式
go run ./examples/echo/ -mode server -transport ws -addr :8080
```

终端 2 — 运行客户端：

```bash
go run ./examples/echo/ -mode client -transport tcp -addr :9090
```

### 运行测试

```bash
go test -v -race ./...
```

## 协议帧格式

```
变体 A（有 Body）:
[Magic:1][Flags:1][Length:2][Opcode:0/2/4][Seq:0/2/4][Body:N][CRC:0/2/4][Padding:0-3]

变体 B（无 Body）:
[Magic:1][Flags:1][Opcode:0/2/4][Seq:0/2/4][CRC:0/2/4][Padding:0-3]
```

- Magic = `0x51` ('Q')——呼应 ProtoQ 中的 Q
- Flags 位图控制各字段存在性和长度
- Length = Opcode + Seq + Body + CRC（不含 Magic/Flags/Length/Padding）
- Padding 使帧对齐到 4 字节边界

详见 [DESIGN.md](DESIGN.md)

## 项目结构

对标 Java uni-protocol 的多模块分层架构，适配 Go 单 module 多子包模式：

```
protoq/
├── go.mod
│
├── codec.go                 ← Codec + Converter 接口（protocol-api）
├── connection.go            ← Connection 接口
├── context.go               ← Context 接口
├── filter.go                ← Filter + FilterChain 接口
├── message_server.go        ← MessageServer 接口
├── message_queue.go         ← MessageQueue（ACK 等待队列）
├── packet.go                ← PacketData（帧数据对象）
├── packet_envelope.go       ← PacketEnvelope（出站消息信封）
├── address.go               ← NetworkAddress + NetworkConfig
├── flags.go                 ← Flags 位图
├── constants.go             ← 协议常量
├── errors.go                ← 错误定义
├── transport.go             ← Transport/Dialer/ListenerFactory 接口
│
├── basic/                   ← 业务协议层（protocol-basic）
│   ├── codec/               ← DefaultCodec + 转换器
│   │   └── convert/
│   ├── constant/            ← MessageId / ConnectionKey / 心跳常量
│   ├── exception/           ← 协议异常（NegotiateException 等）
│   ├── filter/              ← NegotiateFilter
│   ├── message/             ← 协商/心跳/事件负载
│   │   └── handler/         ← NegotiatePayloadHandler / HeartbeatPayloadHandler
│   └── register/            ← BeanRegister / MessageDispatcher / EventDispatcher
│
├── conn/                   ← 传输层实现（protocol-netty）
│   ├── connection.go        ← NettyConnection
│   ├── bridge.go            ← NettyMessageBridge（读写循环）
│   ├── decoder.go           ← NettyMessageDecoder（状态机解码）
│   ├── encoder.go           ← NettyMessageEncoder（帧编码）
│   ├── seq.go               ← SeqManager（序列号 + 重传）
│   ├── crc.go               ← CRC 校验
│   ├── tcp.go               ← TCP 传输
│   ├── ws.go                ← WebSocket 传输
│   └── quic.go              ← QUIC 桩（预留）
│
├── client/                  ← 客户端（protocol-client）
├── server/                  ← 服务端（protocol-server）
├── websocket/               ← WebSocket 专用桥接（protocol-websocket）
│
├── examples/echo/           ← Echo 示例（集成协商 + 心跳 + 认证）
└── apps/                    ← 应用入口（预留）
```

## API 概览

### 服务端

```go
// 创建服务端
server := serverpkg.NewServer(conn.NewTCPTransport())

// 注册协商 + 心跳 Handler（biz 层辅助）
negotiator := &message.DefaultNegotiator{}
server.Handle(constant.OpcodeNegotiate, makeNegotiateHandler(negotiator))
server.Handle(constant.OpcodeHeartbeat, makeHeartbeatHandler())

// 注册业务 Handler
server.Handle(0x0100, func(ctx *serverpkg.ConnContext, opcode uint32, body []byte) ([]byte, error) {
    return []byte("ECHO: " + string(body)), nil
})

// 启动监听
go server.ListenAndServe(ctx, ":9090")
```

### 客户端

```go
// 连接 + 协商 + 心跳
client, _ := client.Dial(ctx, conn.NewTCPTransport(), ":9090")
resp, _ := message.Negotiate(ctx, client, message.WithAuth("token"))
stopHB := message.StartHeartbeat(client, nil)
defer stopHB()

// 发送请求
respFrame, _ := client.SendRequest(ctx, 0x0100, []byte("hello"))
client.Close()
```

### 直接使用编解码

```go
// 编码
packet := protoq.NewRequestPacket(0x0001, 0x0001, []byte("data"), true, true)
data, _ := conn.Encode(packet)

// 解码（从 io.Reader 流式读取）
decoder := conn.NewDecoder(conn)
for {
    packet, err := decoder.DecodePacket()
    if err == io.EOF { break }
    // 处理 packet
}
```

## 命名由来

**ProtoQ** = **Proto**col + **Q**。

Q 承载双重含义：

- **Queue（队列）**——协议的 `MessageQueue` 是核心机制：每个连接持有 ACK 等待队列（`put → get → complete`），配合 `SeqManager` 的序列号分配与 `PendingRequest` 重传循环，实现异步请求-应答的可靠匹配。这与传统 RPC 的同步阻塞模型截然不同——ProtoQ 的队列机制让单连接可以同时承载多个未完成请求，真正实现多路并发。

- **Quick（快速）**——从线格式设计层面追求极致效率：可变长度字段由 Flags 位图逐帧控制（用多少占多少，不为零值浪费字节）；流式解码器基于状态机处理粘包/半包，零拷贝读取；CRC-16 轻量校验兼顾安全与速度。协议头最小仅 2 字节（Magic + Flags），足以完成简单通知的投递。

两者互为表里：Queue 保证了并发的秩序与可靠性，Quick 保证了单次交互的低延迟——这正是 ProtoQ 作为高效多路并发协议的设计哲学。

## 许可证

MIT
