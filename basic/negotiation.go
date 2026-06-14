// Package biz — 内容协商协议
//
// 连接建立后，客户端必须先完成内容协商，才能发送业务请求。
// 协商阶段：
//   1. 客户端发送 OpcodeNegotiate 请求，携带 NegotiateRequest
//   2. 服务端验证协商参数，返回 NegotiateResponse
//   3. 若 accepted=true，进入业务通信阶段；否则连接关闭
//
// 协商内容包括：
//   - 协议版本号（用于向后兼容）
//   - 加密方案（当前仅支持 "none"）
//   - 授权凭证（token）
package basic

import (
	"encoding/json"
	"fmt"
)

// ──────────────────────────────────────────────
// 协商常量
// ──────────────────────────────────────────────

const (
	// ProtoVersion 当前协议版本
	ProtoVersion = 1

	// DefaultNegotiateTimeout 协商超时
	DefaultNegotiateTimeout = 10 // 秒
)

// ──────────────────────────────────────────────
// 协商消息体
// ──────────────────────────────────────────────

// NegotiateRequest 客户端发送的协商请求。
type NegotiateRequest struct {
	// Version 客户端支持的协议版本号
	Version int `json:"version"`

	// Encryption 加密方案，当前仅支持 "none"
	Encryption string `json:"encryption"`

	// Auth 授权信息
	Auth *AuthInfo `json:"auth,omitempty"`
}

// AuthInfo 授权凭证。
type AuthInfo struct {
	// Token 认证令牌
	Token string `json:"token,omitempty"`

	// Extra 扩展字段（预留）
	Extra map[string]string `json:"extra,omitempty"`
}

// NegotiateResponse 服务端返回的协商结果。
type NegotiateResponse struct {
	// Accepted 是否接受协商
	Accepted bool `json:"accepted"`

	// ServerVersion 服务端协议版本
	ServerVersion int `json:"server_version"`

	// SessionID 会话标识（协商成功后分配）
	SessionID string `json:"session_id,omitempty"`

	// ServerTime 服务端时间（Unix 秒）
	ServerTime int64 `json:"server_time,omitempty"`

	// Reason 拒绝原因（仅 accepted=false 时有意义）
	Reason string `json:"reason,omitempty"`

	// Extra 扩展字段
	Extra map[string]string `json:"extra,omitempty"`
}

// ──────────────────────────────────────────────
// 编解码
// ──────────────────────────────────────────────

// MarshalNegotiateRequest 序列化协商请求为 JSON。
func MarshalNegotiateRequest(req *NegotiateRequest) ([]byte, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("biz: marshal negotiate request: %w", err)
	}
	return data, nil
}

// UnmarshalNegotiateRequest 反序列化协商请求。
func UnmarshalNegotiateRequest(data []byte) (*NegotiateRequest, error) {
	var req NegotiateRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, fmt.Errorf("biz: unmarshal negotiate request: %w", err)
	}
	return &req, nil
}

// MarshalNegotiateResponse 序列化协商响应为 JSON。
func MarshalNegotiateResponse(resp *NegotiateResponse) ([]byte, error) {
	data, err := json.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("biz: marshal negotiate response: %w", err)
	}
	return data, nil
}

// UnmarshalNegotiateResponse 反序列化协商响应。
func UnmarshalNegotiateResponse(data []byte) (*NegotiateResponse, error) {
	var resp NegotiateResponse
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("biz: unmarshal negotiate response: %w", err)
	}
	return &resp, nil
}

// ──────────────────────────────────────────────
// 协商器接口（可插拔）
// ──────────────────────────────────────────────

// Negotiator 定义协商策略接口。
// 实现者可自定义版本校验、加密协商、授权验证逻辑。
type Negotiator interface {
	// Negotiate 处理客户端的协商请求，返回协商结果。
	// 返回的 response.Accepted 决定是否接受连接。
	Negotiate(req *NegotiateRequest) *NegotiateResponse
}

// DefaultNegotiator 默认协商器：接受任意版本、无加密、无认证。
type DefaultNegotiator struct{}

// Negotiate 实现 Negotiator 接口。
func (d *DefaultNegotiator) Negotiate(req *NegotiateRequest) *NegotiateResponse {
	return &NegotiateResponse{
		Accepted:      true,
		ServerVersion: ProtoVersion,
	}
}
