// Package protoq — 协议常量
//
// 集中定义 ProtoQ 协议帧格式常量、标志位编码、默认配置、序列号及缓冲区参数。
// 参照 TCP_PROTOCOL_SPECIFICATION 帧结构定义。
package protoq

import (
	"math"
	"time"
)

// ─── 协议帧常量（与帧头布局一致）──────────────────────────────────────────

const (
	// MagicByte 帧起始哨兵字节，固定值 'Q' (0x51)
	MagicByte byte = 0x51

	// MaxBodyLen Length 字段（2 字节）可表示的最大 Body 长度
	// 65535 - Opcode + Seq + CRC 的预留空间
	MaxBodyLen = 65535 - 8
)

// ─── 标志位编码（Flags 字节位布局，见 flags.go）──────────────────────────

const (
	// FlagDIR bit7：方向位；0=请求，1=响应
	FlagDIR Flags = 1 << 7 // 0x80

	// FlagRequiresAck bit6：需要确认位；1=需要对端响应
	FlagRequiresAck Flags = 1 << 6 // 0x40

	// FlagHASLEN bit5：长度字段存在位；1=有 Length 字段（变体 A），0=无（变体 B）
	FlagHASLEN Flags = 1 << 5 // 0x20

	// ── Opcode 长度编码（bit4-3）─────────────────────────────────────────

	// FlagOPLENMask Opcode 长度掩码（bit4-3）
	FlagOPLENMask Flags = 0x18 // 0b00011000
	// FlagOPLEN0 无 Opcode 字段
	FlagOPLEN0 Flags = 0x00
	// FlagOPLEN2 Opcode 占 2 字节
	FlagOPLEN2 Flags = 0x08
	// FlagOPLEN4 Opcode 占 4 字节
	FlagOPLEN4 Flags = 0x10

	// ── Seq 长度编码（bit2-1）────────────────────────────────────────────

	// FlagSEQLENMask Seq 长度掩码（bit2-1）
	FlagSEQLENMask Flags = 0x06 // 0b00000110
	// FlagSEQLEN0 无 Seq 字段
	FlagSEQLEN0 Flags = 0x00
	// FlagSEQLEN2 Seq 占 2 字节
	FlagSEQLEN2 Flags = 0x02
	// FlagSEQLEN4 Seq 占 4 字节
	FlagSEQLEN4 Flags = 0x04

	// ── CRC 长度编码（bit0）──────────────────────────────────────────────

	// FlagCRCLENMask CRC 长度掩码（bit0）
	FlagCRCLENMask Flags = 0x01 // 0b00000001
	// FlagCRCLEN0 无 CRC 字段
	FlagCRCLEN0 Flags = 0x00
	// FlagCRCLEN2 CRC 占 2 字节（CRC-16-IBM）
	FlagCRCLEN2 Flags = 0x01
)

// ─── 默认配置值───────────────────────────────────────────────────────────

const (
	// DefaultOpcodeLen 默认 Opcode 字段长度（2 字节）
	DefaultOpcodeLen = 2

	// DefaultSeqLen 默认 Seq 字段长度（2 字节）
	DefaultSeqLen = 2
)

// ─── 长度编码辅助常量─────────────────────────────────────────────────────

const (
	// Len0 零字节字段
	Len0 = 0
	// Len2 两字节字段
	Len2 = 2
	// Len4 四字节字段
	Len4 = 4
)

// ─── 序列号与重传常量─────────────────────────────────────────────────────

const (
	// DefaultRetryTimeout 请求重传初始超时时间
	DefaultRetryTimeout = 1 * time.Second

	// MaxRetries 请求最大重传次数（超过后返回 ErrMaxRetries）
	MaxRetries = 3

	// MaxSeq16 16 位序列号最大值（回绕边界）
	MaxSeq16 = math.MaxUint16

	// MaxSeq32 32 位序列号最大值（回绕边界）
	MaxSeq32 = math.MaxUint32
)

// ─── 缓冲区与 I/O 常量────────────────────────────────────────────────────

const (
	// readBufferSize 解码器单次读取缓冲区大小
	readBufferSize = 4096
)
