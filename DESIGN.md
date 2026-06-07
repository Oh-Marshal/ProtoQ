# ProtoQ 协议设计文档

## 1. 概述

ProtoQ 是一种面向消息的二进制应用层协议，设计目标是在不可靠传输层（TCP/WebSocket/QUIC）之上提供可靠的请求-应答和单向通知通信。协议帧采用可变长度编码，字段存在性由单字节标志位控制，支持 CRC 校验、序列号管理和超时重传。

### 1.1 设计原则

- **零外部依赖**：纯 Go 标准库实现，可直接嵌入任何 Go 项目。
- **传输无关**：通过 Transport 接口抽象底层传输，已支持 TCP 和 WebSocket，预留 QUIC 扩展。
- **流式友好**：基于状态机的解码器自动处理 TCP 粘包、半包和噪声同步。
- **协议可扩展**：Opcode 字段支持 0/2/4 字节，Flags 位图允许逐帧协商字段存在性。

### 1.2 协议定位

```
┌─────────────────────────────────┐
│         业务应用层               │  ← 用户代码
├─────────────────────────────────┤
│         ProtoQ 协议层            │  ← 本项目
│  (编码/解码/序列号/重传/路由)     │
├─────────────────────────────────┤
│         传输抽象层               │
│  ┌──────┐ ┌──────────┐ ┌──────┐ │
│  │ TCP  │ │WebSocket │ │ QUIC │ │
│  └──────┘ └──────────┘ └──────┘ │
└─────────────────────────────────┘
```

ProtoQ 定位在传输层之上、业务逻辑之下，后续可基于 ProtoQ 构建 RPC 框架、消息队列、实时推送等具体业务。

---

## 2. 协议帧格式

### 2.1 变体 A：通用报文（有 Body，HAS_LEN=1）

```
 0               8              16              24              32
┌───────┬───────┬───────────────┬───────────────────────────────┐
│ Magic │ Flags │    Length     │          Opcode               │
│  1B   │  1B   │     2B       │       0 / 2 / 4 B             │
├───────┴───────┴───────────────┴───────────────────────────────┤
│                            Seq                                │
│                       0 / 2 / 4 B                             │
├───────────────────────────────────────────────────────────────┤
│                           Body                                │
│                        0 ~ 65527 B                            │
├───────────────────────────────────────────────────────────────┤
│                           CRC                                 │
│                       0 / 2 / 4 B                             │
├───────────────────────────────────────────────────────────────┤
│                         Padding                               │
│                         0 ~ 3 B                               │
└───────────────────────────────────────────────────────────────┘
```

**Length 定义**：Length = OpcodeLen + SeqLen + BodyLen + CRCLen（不含 Magic、Flags、Length 自身和 Padding）

### 2.2 变体 B：固定长度应答（无 Body，HAS_LEN=0）

```
 0               8              16              24              32
┌───────┬───────┬───────────────────────────────────────────────┐
│ Magic │ Flags │                  Opcode                       │
│  1B   │  1B   │               0 / 2 / 4 B                     │
├───────┴───────┴───────────────────────────────────────────────┤
│                            Seq                                │
│                       0 / 2 / 4 B                             │
├───────────────────────────────────────────────────────────────┤
│                           CRC                                 │
│                       0 / 2 / 4 B                             │
├───────────────────────────────────────────────────────────────┤
│                         Padding                               │
│                         0 ~ 3 B                               │
└───────────────────────────────────────────────────────────────┘
```

变体 B 无 Length 字段和 Body，适用于简单应答（如 ACK-only 响应）。接收端通过 HAS_LEN 标志区分变体。

### 2.3 帧字段详解

| 字段 | 字节数 | 说明 |
|---|---|---|
| Magic | 1 | 固定值 `0x51`（'Q'），用于帧起始同步 |
| Flags | 1 | 位图，控制后续字段的存在性和长度 |
| Length | 0 或 2 | 大端序 uint16，HAS_LEN=1 时存在 |
| Opcode | 0/2/4 | 操作码，大端序，由 OP_LEN 控制长度 |
| Seq | 0/2/4 | 序列号，大端序，由 SEQ_LEN 控制长度 |
| Body | 0~N | 消息体，N = Length − OpcodeLen − SeqLen − CRCLen |
| CRC | 0/2/4 | 校验值，覆盖范围：Flags + Length + Opcode + Seq + Body |
| Padding | 0~3 | 零填充，使 Magic 到 CRC 结束的总长对齐到 4 字节 |

---

## 3. Flags 位图

```
  bit7    bit6    bit5    bit4    bit3    bit2    bit1    bit0
┌───────┬───────┬───────┬───────┬───────┬───────┬───────┬───────┐
│  DIR  │ACK_REQ│HAS_LEN│  OP_LEN (2b) │  SEQ_LEN (2b) │CRC_LEN│
└───────┴───────┴───────┴───────┴───────┴───────┴───────┴───────┘
```

| 位 | 名称 | 值 | 含义 |
|---|---|---|---|
| bit7 | DIR | 0 | 请求 |
| | | 1 | 响应 |
| bit6 | ACK_REQ | 0 | 无需应答 |
| | | 1 | 需要应答（此时 SEQ_LEN > 0） |
| bit5 | HAS_LEN | 0 | 变体 B，无 Length 和 Body |
| | | 1 | 变体 A，有 Length 字段 |
| bit4-3 | OP_LEN | 00 | Opcode 0 字节（通常不推荐） |
| | | 01 | Opcode 2 字节（默认） |
| | | 10 | Opcode 4 字节 |
| | | 11 | 保留 |
| bit2-1 | SEQ_LEN | 00 | Seq 0 字节（通知/无需应答） |
| | | 01 | Seq 2 字节（默认，16 位序列号） |
| | | 10 | Seq 4 字节（32 位序列号） |
| | | 11 | 保留 |
| bit0 | CRC_LEN | 0 | 无 CRC |
| | | 1 | 2 字节 CRC-16-IBM |

### 3.1 标志位约束

```
ACK_REQ=1  →  SEQ_LEN ≠ 0          # 需要应答必须有序列号
DIR=1      →  ACK_REQ = 0          # 响应帧不能要求应答
HAS_LEN=0  →  无 Body，无 Length    # 变体 B 无消息体
```

编码时 `Flags.Validate()` 执行上述约束检查。

### 3.2 CRC_LEN 设计说明

Flags 仅 1 字节（8 位），6 位用于 DIR/ACK_REQ/HAS_LEN/OP_LEN/SEQ_LEN，仅剩 1 位给 CRC_LEN。因此当前实现仅支持：

- CRC_LEN=0：无 CRC
- CRC_LEN=1：2 字节 CRC-16-IBM

4 字节 CRC-32-IEEE 的代码已就绪（`crc.go` 中的 `CRC32IEEE`），若未来协议升级（如增加第二个 Flags 字节），可无缝启用。

---

## 4. CRC 校验

CRC 覆盖范围：Flags + [Length] + Opcode + Seq + Body（不含 Magic 字节）。

| CRC_LEN | 算法 | 多项式 | 初始值 | 输出字节序 |
|---|---|---|---|---|
| 2 | CRC-16-IBM (ARC) | 0x8005 | 0x0000 | 大端 |
| 4 | CRC-32-IEEE | 0x04C11DB7 | 0xFFFFFFFF | 大端 |

测试向量：`"123456789"` 的 CRC-16-IBM = `0xBB3D`。

---

## 5. 四字节对齐

发送前，从 Magic 字节到 CRC 末尾的总字节数填充零字节至 4 的倍数。

```
原始帧长（Magic→CRC 结束）= N
padding = (4 - N % 4) % 4
最终帧长 = N + padding
```

填充值恒为零字节，不影响 CRC 计算（CRC 覆盖范围不含 Padding）。

---

## 6. 序列号与重传

### 6.1 序列号分配

- 序列号从 1 开始单调递增（0 表示无序列号）
- 16 位模式：回绕到 1（跳过 0）
- 32 位模式：回绕到 1（跳过 0）
- 分配时检查与待确认队列的冲突

### 6.2 待确认队列

```
发送端                         接收端
  │                              │
  │  Allocate() → seq            │
  │  Enqueue(seq, frame)         │
  │  Write(frame)                │
  │─────────────────────────────→│
  │                              │  Handle(opcode, body)
  │                              │  Write(response, seq)
  │←─────────────────────────────│
  │  Resolve(seq, response)      │
  │  close(done)                 │
  │  ResponseC ← response        │
  │                              │
```

### 6.3 超时重传

```
重传次数    超时时间
   0         1s  (初始)
   1         2s  (1s × 2)
   2         4s  (1s × 4)
   3         放弃，返回 ErrMaxRetries
```

指数退避因子为 2，最多重试 3 次。重传通过 `SeqManager.SetOnRetransmit()` 回调执行，由 Client 的 `writeFrame()` 实现。

### 6.4 竞态设计

`retryLoop` 和 `WaitForResponse` 存在对 `ResponseC` 的消费竞争。通过引入 `done` 通道解决：

- `Resolve()` 先 `close(done)` 通知 `retryLoop` 退出，再向 `ResponseC` 发送响应
- `retryLoop` 仅监听 `done` 和 `timer.C`，不再消费 `ResponseC`
- `WaitForResponse` 安全地消费 `ResponseC`

---

## 7. 流式解码器

### 7.1 状态机

```
                    ┌────────────────────────────────────┐
                    │                                    │
                    ▼                                    │
  ┌──────────┐  ┌─────────┐  ┌──────────┐  ┌──────────┐ │
  │ State    │→│ State   │→│ State    │→│ State    │ │
  │ Magic    │ │ Flags   │ │ Length   │ │ Opcode   │ │
  └──────────┘ └─────────┘ └──────────┘ └──────────┘ │
       ↑                                               │
       │         ┌──────────┐  ┌──────────┐  ┌──────┐ │
       │         │ State    │←│ State    │←│ State│ │
       │         │ Seq      │ │ Body     │ │ CRC  │ │
       │         └──────────┘ └──────────┘ └──────┘ │
       │                                               │
       │         ┌──────────┐  ┌──────────┐           │
       └─────────│ State    │←│ State    │───────────┘
                 │ Padding  │  │ Done     │
                 └──────────┘  └──────────┘
```

### 7.2 粘包处理

多个完整帧在一次 `Read()` 中到达时，解码器依次解析每个帧。每完成一帧后，`compact()` 清理已消费字节并重置状态机。

### 7.3 半包处理

`ensureBytes(n)` 在数据不足时阻塞读取，而非返回错误。通过内部缓冲区累积数据，确保状态机始终有足够字节推进。

### 7.4 噪声同步

若 Magic 字节位置不是 `0x51`，跳过该字节并继续搜索。这使得解码器能从流中任意位置恢复同步（前提是 `0x51` 不会在帧内其他位置出现；若出现则需上层协议保证）。

---

## 8. 组件架构

```
┌─────────────────────────────────────────────────────┐
│                     Application                      │
│  ┌─────────────────────────────────────────────────┐│
│  │               Server / Client                    ││
│  │  ┌──────────┐  ┌──────────┐  ┌───────────────┐ ││
│  │  │ Handler  │  │ SeqMgr   │  │ Transport     │ ││
│  │  │ Registry │  │ (retry)  │  │ (TCP/WS/QUIC) │ ││
│  │  └──────────┘  └──────────┘  └───────────────┘ ││
│  │  ┌──────────┐  ┌──────────┐                     ││
│  │  │ Encoder  │  │ Decoder  │                     ││
│  │  │ (Frame→  │  │ (Stream→ │                     ││
│  │  │  bytes)  │  │  Frame)  │                     ││
│  │  └──────────┘  └──────────┘                     ││
│  │  ┌──────────┐  ┌──────────┐                     ││
│  │  │ CRC      │  │ Flags    │                     ││
│  │  │ (16/32)  │  │ (bitmap) │                     ││
│  │  └──────────┘  └──────────┘                     ││
│  └─────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────┘
```

### 8.1 数据流（请求-应答）

```
Client.SendRequest(ctx, opcode, body)
  │
  ├─ seqMgr.Allocate() → seq
  ├─ NewRequestFrame(opcode, seq, body, ACK_REQ=true)
  ├─ seqMgr.Enqueue(seq, frame) → PendingRequest
  │    └─ retryLoop(seq, pr)  [goroutine]
  ├─ Encode(frame) → bytes
  ├─ conn.Write(bytes)
  │
  ├─ WaitForResponse(ctx, pr)  [blocking]
  │    ├─ ctx.Done() → timeout
  │    └─ pr.ResponseC → Frame
  │
  └─ return response frame
```

### 8.2 服务端处理

```
Server.serve() [per-connection goroutine]
  │
  ├─ decoder.Decode() → Frame
  ├─ frame.IsRequest()?
  │    ├─ frame.NeedsAck()?
  │    │    └─ go handleRequest(frame, handler) [goroutine]
  │    │         ├─ handler(opcode, body) → (response, error)
  │    │         ├─ NewResponseFrame(opcode, seq, response)
  │    │         └─ EncodeTo(frame, conn)
  │    └─ go handler(opcode, body) [fire-and-forget]
  └─ loop
```

---

## 9. 传输层设计

### 9.1 Transport 接口

```go
type Transport interface {
    Dial(ctx context.Context, addr string) (net.Conn, error)
    Listen(ctx context.Context, addr string) (net.Listener, error)
    String() string
}
```

所有传输实现返回标准 `net.Conn`，ProtoQ 上层代码不感知底层协议差异。

### 9.2 TCP 传输

直接封装 `net.Dialer` 和 `net.ListenConfig`，零额外开销。

### 9.3 WebSocket 传输

纯标准库实现的 RFC 6455 最小子集：

- **握手**：客户端生成随机 `Sec-WebSocket-Key`，服务端计算 `Sec-WebSocket-Accept`（SHA-1 + Base64）
- **帧格式**：支持 FIN + 二进制帧 + 可变长度载荷（7/16/64 位）
- **掩码**：客户端→服务端帧执行 XOR 掩码（符合 RFC 强制要求）
- **控制帧**：支持 Ping/Pong/Close
- **`wsConn`**：实现 `net.Conn` 接口，包含 `Read/Write/Close/SetDeadline/SetReadDeadline/SetWriteDeadline`

当前限制：不支持分片消息、不支持文本帧（全部使用二进制帧）。

### 9.4 QUIC 桩

`transport_quic.go` 提供接口占位。集成 `quic-go` 时需将 `quic.Stream` 包装为 `net.Conn`。QUIC 的多路复用特性意味着一个 `quic.Connection` 可承载多个 ProtoQ 连接（每个对应一个 Stream）。

---

## 10. 并发模型

### 10.1 客户端

```
┌──────────────────────────────────────────┐
│ Client                                    │
│  ┌────────────┐  ┌──────────────────────┐│
│  │ readLoop() │  │ writeFrame()         ││
│  │ [goroutine]│  │ [writeMu protected]  ││
│  │            │  │                      ││
│  │ Decode()   │  │ SendRequest() calls  ││
│  │   ↓        │  │ SendNotification()   ││
│  │ Resolve()  │  │ retransmit()         ││
│  └────────────┘  └──────────────────────┘│
│  ┌──────────────────────────────────────┐│
│  │ SeqManager                            ││
│  │  ┌──────────┐  ┌──────────┐          ││
│  │  │retryLoop│  │retryLoop│  ...       ││
│  │  │[goroutine]│[goroutine]│           ││
│  │  └──────────┘  └──────────┘          ││
│  └──────────────────────────────────────┘│
└──────────────────────────────────────────┘
```

- **读**：单 goroutine（`readLoop`），将响应帧通过 `Resolve()` 投递到对应 `PendingRequest`
- **写**：多 goroutine 共享，`writeMu` 互斥锁保证帧完整性
- **重传**：每个待确认请求一个 `retryLoop` goroutine，`done` 通道协调退出

### 10.2 服务端

```
┌──────────────────────────────────────────┐
│ Server (per connection)                   │
│  ┌────────────────────────────────────┐  │
│  │ serve() [goroutine]                 │  │
│  │  Decode() → dispatch               │  │
│  │    ├─ ACK_REQ → go handleRequest() │  │
│  │    └─ no ACK → go handler()        │  │
│  └────────────────────────────────────┘  │
│  ┌────────────────────────────────────┐  │
│  │ handleRequest() [per-request gor.] │  │
│  │  handler() → NewResponseFrame()    │  │
│  │  → writeFrame() [writeMu]          │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
```

- **连接接受**：主 goroutine 循环 `Accept()`，每个连接启动 `serve()` goroutine
- **请求分发**：需要应答的请求启动独立 goroutine 调用 handler
- **写**：`writeMu` 保护，确保响应帧不交错

---

## 11. 错误处理

| 错误 | 触发条件 |
|---|---|
| `ErrInvalidMagic` | Magic 字节 ≠ 0x51（解码器自动跳过） |
| `ErrACKReqNeedsSeq` | ACK_REQ=1 但 SEQ_LEN=0 |
| `ErrResponseCantAckReq` | DIR=1 且 ACK_REQ=1 |
| `ErrBodyWithoutLength` | HAS_LEN=0 但有 Body |
| `ErrCRCMismatch` | CRC 校验不匹配 |
| `ErrTimeout` | 请求上下文超时 |
| `ErrMaxRetries` | 超过 3 次重传 |
| `ErrConnClosed` | 连接已关闭 |

`ProtoQError` 包装底层错误并附加操作上下文（`Op` 字段）。

---

## 12. 扩展指南

### 12.1 添加新的 Opcode 处理

```go
server.Handle(0x0100, func(opcode uint32, body []byte) ([]byte, error) {
    // 解析 body，执行业务逻辑
    return []byte("result"), nil
})
```

### 12.2 添加新的传输层

1. 实现 `Transport` 接口（`Dial` / `Listen` / `String`）
2. `Dial` 返回 `net.Conn`，`Listen` 返回 `net.Listener`
3. 注册到客户端/服务端：`protoq.Dial(ctx, myTransport, addr)`

### 12.3 上层协议封装

ProtoQ 提供原始帧传输。上层可基于 Opcode 实现：

- **RPC 路由**：Opcode 映射为方法名，Body 为序列化参数
- **发布/订阅**：Opcode 区分 topic，Body 为消息内容
- **流式传输**：利用 4 字节 Opcode 扩展流控语义

### 12.4 安全增强

ProtoQ 不包含加密。建议在生产环境中：

- 通过 TLS 包装 TCP 连接（`tls.Dial` / `tls.Listen`）
- 通过 WSS（WebSocket Secure）使用 WebSocket 传输
- QUIC 内置 TLS 1.3 加密

---

## 13. 测试覆盖

| 测试 | 覆盖内容 |
|---|---|
| `TestEncodeDecode` | 7 种帧变体的编码→解码往返 |
| `TestDecodeStickyPackets` | 3 帧拼接一次性解析 |
| `TestDecodeHalfPacket` | 3 字节分块解析 1 帧 |
| `TestDecodeNoiseBeforeMagic` | 帧前噪声自动跳过 |
| `TestCRCMismatch` | CRC 篡改检测 |
| `TestFlagsValidation` | 标志位合法性检查 |
| `TestCRC16IBM` | CRC 测试向量验证 |
| `TestRequestResponseMatching` | TCP 管道端到端请求-应答 |
| `TestMultipleRequests` | 10 并发请求-应答 |
| `TestSeqOverflow` | 16 位序列号回绕 |
| `TestEncoder4ByteAlignment` | 6 种 Body 长度的对齐验证 |
| `TestVariantB` | 变体 B（无 Body 无 Length）编解码 |

全部通过 `-race` 竞态检测。
