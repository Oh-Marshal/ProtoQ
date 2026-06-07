# ProtoQ

纯 Go 标准库实现的自定义二进制网络协议，支持请求-应答和单向通知，内置 CRC 校验、序列号管理和超时重传。

[![Go Version](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev/)
[![Tests](https://img.shields.io/badge/tests-12/12%20pass-brightgreen)](.)
[![Race](https://img.shields.io/badge/race-clean-brightgreen)](.)
[![Deps](https://img.shields.io/badge/deps-zero-blue)](.)

## 特性

- **可变长度帧**：Opcode、Seq、CRC 字段长度由 Flags 位图逐帧控制（0/2/4 字节）
- **两种帧变体**：变体 A 含 Length+Body，变体 B 无 Length+Body（轻量应答）
- **CRC 校验**：CRC-16-IBM（2 字节）和 CRC-32-IEEE（4 字节）
- **序列号管理**：16/32 位自增序列号，待确认队列，指数退避超时重传（最多 3 次）
- **流式解码器**：基于状态机处理 TCP 粘包、半包、噪声同步
- **多传输层**：TCP、WebSocket（纯标准库 RFC 6455）、QUIC（接口预留）
- **并发安全**：客户端多 goroutine 并发请求，服务端每连接独立 goroutine
- **零依赖**：仅使用 Go 标准库

## 快速开始

### 安装

```bash
git clone <repo-url> protoq
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

输出示例：

```
已连接到 ProtoQ 服务端 [tcp] :9090
--- 测试 1: Echo 请求 ---
响应: ECHO: Hello ProtoQ!
--- 测试 2: 时间查询 ---
服务端时间: 2026-06-07T20:47:06+08:00
--- 测试 3: 单向通知 ---
通知已发送
--- 测试 4: 状态查询 ---
服务端状态: server up, conns=1
--- 测试 5: 并发请求 ---
  并发 #3: ECHO: 并发消息 #3
  并发 #4: ECHO: 并发消息 #4
  ...
--- 客户端统计 ---
  已发送请求: 8
  已收到响应: 8
  已发送通知: 1
  待确认请求: 0
```

### 运行测试

```bash
go test -v -race ./...
```

```
=== RUN   TestEncodeDecode
--- PASS: TestEncodeDecode (0.00s)
=== RUN   TestDecodeStickyPackets
--- PASS: TestDecodeStickyPackets (0.00s)
=== RUN   TestDecodeHalfPacket
--- PASS: TestDecodeHalfPacket (0.00s)
=== RUN   TestRequestResponseMatching
--- PASS: TestRequestResponseMatching (0.00s)
=== RUN   TestMultipleRequests
--- PASS: TestMultipleRequests (0.01s)
...
PASS
ok      github.com/oh-marshal/protoq   1.066s
```

## API 概览

### 服务端

```go
// 创建服务端（选择传输层）
server := protoq.NewServer(protoq.NewTCPTransport())

// 注册 Opcode 处理函数
server.Handle(0x0001, func(opcode uint32, body []byte) ([]byte, error) {
    return []byte("ECHO: " + string(body)), nil
})

// 启动监听（阻塞）
ctx, cancel := context.WithCancel(context.Background())
go server.ListenAndServe(ctx, ":9090")

// 优雅关闭
cancel()
server.Shutdown()
```

### 客户端

```go
// 连接到服务端
client, err := protoq.Dial(ctx, protoq.NewTCPTransport(), "127.0.0.1:9090")
defer client.Close()

// 发送请求（自动分配序列号，等待应答）
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
resp, err := client.SendRequest(ctx, 0x0001, []byte("hello"))
cancel()
// resp.Opcode, resp.Seq, resp.Body

// 发送通知（无应答，无序列号）
err := client.SendNotification(0x00FF, []byte("heartbeat"))

// 查看统计
stats := client.Stats()
// stats.RequestsSent, stats.ResponsesReceived, ...
```

### 自定义传输层

```go
type MyTransport struct{}

func (t *MyTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
    // 实现连接逻辑
}
func (t *MyTransport) Listen(ctx context.Context, addr string) (net.Listener, error) {
    // 实现监听逻辑
}
func (t *MyTransport) String() string { return "myproto" }

// 使用
client, _ := protoq.Dial(ctx, &MyTransport{}, "addr")
server := protoq.NewServer(&MyTransport{})
```

### 直接使用编解码

```go
// 编码
frame := protoq.NewRequestFrame(0x0001, 0x0001, []byte("data"), true, true)
data, err := protoq.Encode(frame)

// 解码（从 io.Reader 流式读取）
decoder := protoq.NewDecoder(conn)
for {
    frame, err := decoder.Decode()
    if err == io.EOF { break }
    // 处理 frame
}
```

## 协议帧格式

```
变体 A（有 Body）:
[Magic:1][Flags:1][Length:2][Opcode:0/2/4][Seq:0/2/4][Body:N][CRC:0/2/4][Padding:0-3]

变体 B（无 Body）:
[Magic:1][Flags:1][Opcode:0/2/4][Seq:0/2/4][CRC:0/2/4][Padding:0-3]
```

- Magic = `0x51` ('Q')
- Flags 位图控制各字段存在性和长度
- Length = Opcode + Seq + Body + CRC（不含 Magic/Flags/Length/Padding）
- Padding 使帧对齐到 4 字节边界

详见 [DESIGN.md](DESIGN.md)

## 项目结构

```
protoq/
├── go.mod                  # Go 模块定义
├── doc.go                  # 包文档
├── flags.go                # Flags 位图定义与操作
├── errors.go               # 错误类型
├── crc.go                  # CRC-16-IBM / CRC-32-IEEE
├── frame.go                # Frame 结构体与工厂方法
├── encoder.go              # 帧编码器（含对齐填充）
├── decoder.go              # 流式解码器状态机
├── seq.go                  # 序列号管理与重传
├── transport.go            # Transport 接口定义
├── transport_tcp.go        # TCP 传输实现
├── transport_ws.go         # WebSocket 传输实现（纯标准库）
├── transport_quic.go       # QUIC 传输桩
├── client.go               # 客户端
├── server.go               # 服务端
├── protoq_test.go          # 单元测试（12 项）
├── DESIGN.md               # 详细设计文档
├── README.md               # 本文件
└── examples/
    └── echo/
        └── main.go         # Echo 服务端/客户端示例
```

## 配置选项

### 客户端

```go
protoq.Dial(ctx, transport, addr,
    protoq.WithClientOpcodeLen(2),   // Opcode 字段长度 (0/2/4)
    protoq.WithClientSeqLen(2),      // Seq 字段长度 (2/4)
    protoq.WithClientCRC(true),      // 是否启用 CRC
)
```

### 服务端

```go
protoq.NewServer(transport,
    protoq.WithServerOpcodeLen(2),   // Opcode 字段长度
)
```

## 许可证

MIT
