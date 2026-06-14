// Package biz — 业务层错误定义
package basic

import "errors"

var (
	// ErrNegotiateRequired 尚未完成协商
	ErrNegotiateRequired = errors.New("biz: negotiate required before sending requests")

	// ErrNegotiateFailed 协商失败
	ErrNegotiateFailed = errors.New("biz: negotiate failed")

	// ErrHeartbeatLost 心跳丢失
	ErrHeartbeatLost = errors.New("biz: heartbeat lost")

	// ErrHeartbeatTimeout 心跳超时（服务端）
	ErrHeartbeatTimeout = errors.New("biz: heartbeat timeout, closing connection")

	// ErrUnknownOpcode 未注册的操作码
	ErrUnknownOpcode = errors.New("biz: unknown opcode")

	// ErrUnauthorized 未授权
	ErrUnauthorized = errors.New("biz: unauthorized")

	// ErrVersionMismatch 协议版本不匹配
	ErrVersionMismatch = errors.New("biz: protocol version mismatch")
)
