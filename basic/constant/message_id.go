// Package biz — Opcode 分段定义
//
// ProtoQ 在线路上使用 2 字节操作码（uint16，范围 0x0000–0xFFFF）。
// 按功能划分为三个区段：
//
//	0x0000 – 0x00FF   系统预留（256 个）  — 协商、心跳等框架级协议
//	0x0100 – 0xFEFF   业务自定义          — 应用层自由使用
//	0xFF00 – 0xFFFF   系统异常（256 个）  — 框架级错误响应
package constant

import (
	message "github.com/oh-marshal/protoq/basic/message"
)
// ──────────────────────────────────────────────
// 区段边界
// ──────────────────────────────────────────────
const (
	// SysOpcodeStart  系统预留起始
	SysOpcodeStart uint32 = 0x0000
	// SysOpcodeEnd    系统预留结束（不含）
	SysOpcodeEnd uint32 = 0x0100

	// BizOpcodeStart  业务自定义起始
	BizOpcodeStart uint32 = 0x0100
	// BizOpcodeEnd    业务自定义结束（不含）
	BizOpcodeEnd uint32 = 0xFF00

	// ErrOpcodeStart  系统异常起始
	ErrOpcodeStart uint32 = 0xFF00
	// ErrOpcodeEnd    系统异常结束（不含）
	ErrOpcodeEnd uint32 = 0x10000
)

// ──────────────────────────────────────────────
// 系统预留操作码（0x0000–0x00FF）
// ──────────────────────────────────────────────
const (
	// OpcodeNegotiate 内容协商 — 连接建立后第一条报文
	// 请求体：NegotiateRequest（JSON）
	// 响应体：NegotiateResponse（JSON）
	OpcodeNegotiate uint32 = 0x0001

	// OpcodeHeartbeat 心跳 — 每 30 秒发送一次
	// 请求体：空（无 Body）
	// 响应体：空（无 Body）
	OpcodeHeartbeat uint32 = 0x0002

	// OpcodeDisconnect 主动断开通知
	// 请求体：DisconnectReason（JSON，可选）
	// 响应体：空
	OpcodeDisconnect uint32 = 0x0003
)

// ──────────────────────────────────────────────
// 系统异常操作码（0xFF00–0xFFFF）
// ──────────────────────────────────────────────
const (
	// OpcodeErrUnknown        未知错误
	OpcodeErrUnknown uint32 = 0xFF00
	// OpcodeErrNegotiateFailed 协商失败
	OpcodeErrNegotiateFailed uint32 = 0xFF01
	// OpcodeErrUnauthorized   未授权
	OpcodeErrUnauthorized uint32 = 0xFF02
	// OpcodeErrHeartbeatLost  心跳丢失
	OpcodeErrHeartbeatLost uint32 = 0xFF03
	// OpcodeErrInternal       内部错误
	OpcodeErrInternal uint32 = 0xFF04
	// OpcodeErrRateLimit      限流
	OpcodeErrRateLimit uint32 = 0xFF05
)

// ──────────────────────────────────────────────
// 辅助函数
// ──────────────────────────────────────────────

// IsSystemOpcode 判断是否为系统预留操作码。
func IsSystemOpcode(op uint32) bool {
	return op >= SysOpcodeStart && op < SysOpcodeEnd
}

// IsBizOpcode 判断是否为业务自定义操作码。
func IsBizOpcode(op uint32) bool {
	return op >= BizOpcodeStart && op < BizOpcodeEnd
}

// IsErrorOpcode 判断是否为系统异常操作码。
func IsErrorOpcode(op uint32) bool {
	return op >= ErrOpcodeStart && op < ErrOpcodeEnd
}
