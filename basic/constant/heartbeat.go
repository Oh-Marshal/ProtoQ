// Package biz — 心跳协议
//
// 内容协商完成后，客户端每 30 秒自动发送心跳请求。
// 心跳使用请求-应答模式（ACK_REQ=1），但无请求体。
// 服务端收到心跳后返回空响应体。
//
// 心跳丢失处理：
//   - 客户端：连续 N 次心跳无响应 → 判定连接断开
//   - 服务端：超过 3 个心跳周期未收到心跳 → 主动关闭连接
package constant

import "time"

// ──────────────────────────────────────────────
// 心跳常量
// ──────────────────────────────────────────────

const (
	// HeartbeatInterval 心跳发送间隔
	HeartbeatInterval = 30 * time.Second

	// HeartbeatTimeout 单次心跳应答超时
	HeartbeatTimeout = 5 * time.Second

	// HeartbeatMaxMissed 最大连续丢失心跳数（超过此数判定连接断开）
	HeartbeatMaxMissed = 3

	// HeartbeatServerTimeout 服务端心跳超时（超过此时间未收到心跳则断开）
	HeartbeatServerTimeout = HeartbeatInterval * HeartbeatMaxMissed
)

// ──────────────────────────────────────────────
// 心跳状态
// ──────────────────────────────────────────────

// HeartbeatStatus 心跳连接状态。
type HeartbeatStatus int

const (
	// HeartbeatOK 心跳正常
	HeartbeatOK HeartbeatStatus = iota
	// HeartbeatLost 心跳丢失
	HeartbeatLost
)
