// Package protoq 实现 ProtoQ 自定义网络协议。
//
// ProtoQ 是一种面向消息的二进制协议，支持请求-应答和单向通知两种通信模式。
// 协议帧采用变长编码，字段存在性由 Flags 位图控制，支持 CRC 校验和四字节对齐。
//
// 协议格式（变体 A，有 Body）：
//
//	[Magic:1][Flags:1][Length:2][Opcode:0/2/4][Seq:0/2/4][Body:N][CRC:0/2/4][Padding:0-3]
//
// 协议格式（变体 B，无 Body）：
//
//	[Magic:1][Flags:1][Opcode:0/2/4][Seq:0/2/4][CRC:0/2/4][Padding:0-3]
//
// 传输层支持 TCP 和 WebSocket，通过 Transport 接口可扩展 QUIC 等其他传输协议。
package protoq
