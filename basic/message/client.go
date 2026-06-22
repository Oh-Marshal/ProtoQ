// Package biz — 客户端辅助函数
//
// 对标 Java uni-protocol protocol-client。
// 提供协商、心跳等客户端辅助函数，操作 *Client。
package message

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	api "github.com/oh-marshal/protoq"
	constant "github.com/oh-marshal/protoq/basic/constant"
	exception "github.com/oh-marshal/protoq/basic/exception"
	netty "github.com/oh-marshal/protoq/netty"
)

// ─── 协商选项 ────────────────────────────────────────────────────────────────

// NegotiateOption 协商选项。
type NegotiateOption func(*NegotiateRequest)

// WithAuth 设置认证 token。
func WithAuth(token string) NegotiateOption {
	return func(r *NegotiateRequest) {
		if r.Auth == nil {
			r.Auth = &AuthInfo{}
		}
		r.Auth.Token = token
	}
}

// WithNegotiateEncryption 设置加密方案。
func WithNegotiateEncryption(enc string) NegotiateOption {
	return func(r *NegotiateRequest) { r.Encryption = enc }
}

// WithNegotiateExtra 设置协商扩展字段。
func WithNegotiateExtra(key, value string) NegotiateOption {
	return func(r *NegotiateRequest) {
		if r.Auth == nil {
			r.Auth = &AuthInfo{}
		}
		if r.Auth.Extra == nil {
			r.Auth.Extra = make(map[string]string)
		}
		r.Auth.Extra[key] = value
	}
}

// Negotiate 在已建立的 protoq 连接上执行内容协商。
// 对标 uni-protocol 客户端协商流程：发送协商请求 → 等待响应 → 设置 CODEC_TYPE。
//
// 必须在使用其他业务操作码之前调用。
func Negotiate(ctx context.Context, client *api.Client, opts ...NegotiateOption) (*NegotiateResponse, error) {
	// 1. 构建协商请求
	req := &NegotiateRequest{
		Version:    ProtoVersion,
		Encryption: "none",
	}
	for _, opt := range opts {
		opt(req)
	}

	reqBody, err := MarshalNegotiateRequest(req)
	if err != nil {
		return nil, fmt.Errorf("biz: 序列化协商请求失败: %w", err)
	}

	// 2. 通过 Client.SendRequest 发送
	respFrame, err := client.SendRequest(ctx, OpcodeNegotiate, reqBody)
	if err != nil {
		return nil, fmt.Errorf("biz: 协商失败: %w", err)
	}

	// 3. 反序列化响应
	resp, err := UnmarshalNegotiateResponse(respFrame.Body)
	if err != nil {
		return nil, fmt.Errorf("biz: 反序列化协商响应失败: %w", err)
	}

	if !resp.Accepted {
		return resp, ErrNegotiateFailed
	}

	// 4. 协商成功：设置 CODEC_TYPE 属性
	client.SetProperty(ConnectionKeyCODEC_TYPE, req.Encryption)
	if resp.SessionID != "" {
		client.SetProperty("session_id", resp.SessionID)
	}

	return resp, nil
}

// MustNegotiate 执行协商，失败时 panic（仅用于初始化阶段）。
func MustNegotiate(ctx context.Context, client *api.Client, opts ...NegotiateOption) *NegotiateResponse {
	resp, err := Negotiate(ctx, client, opts...)
	if err != nil {
		panic("biz: 协商失败: " + err.Error())
	}
	return resp
}

// ─── 心跳 ────────────────────────────────────────────────────────────────────

// HeartbeatLoopConfig 心跳循环配置。
type HeartbeatLoopConfig struct {
	// Interval 心跳间隔，默认 30s
	Interval time.Duration

	// Timeout 单次心跳超时，默认 5s
	Timeout time.Duration

	// MaxMissed 连续丢失心跳次数上限，超过后调用 OnLost，默认 3
	MaxMissed int

	// OnLost 心跳丢失时的回调（可选）
	OnLost func()
}

// DefaultHeartbeatLoopConfig 返回默认心跳循环配置。
func DefaultHeartbeatLoopConfig() *HeartbeatLoopConfig {
	return &HeartbeatLoopConfig{
		Interval:  HeartbeatInterval,
		Timeout:   HeartbeatTimeout,
		MaxMissed: HeartbeatMaxMissed,
	}
}

// StartHeartbeat 启动心跳发送循环。
//
// 对标 uni-protocol 客户端心跳机制。
// 使用 Client.SendRequest 发送心跳 PING，等待 PONG 响应。
// 返回一个 stop 函数，调用它即可停止心跳。
// 心跳丢失时，默认行为是调用 OnLost 回调。
func StartHeartbeat(client *api.Client, cfg *HeartbeatLoopConfig) (stop func()) {
	if cfg == nil {
		cfg = DefaultHeartbeatLoopConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	var stopped atomic.Bool

	go func() {
		defer close(done)

		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()

		missed := 0

		for {
			select {
			case <-ticker.C:
				if stopped.Load() {
					return
				}

				reqCtx, reqCancel := context.WithTimeout(ctx, cfg.Timeout)
				_, err := client.SendRequest(reqCtx, OpcodeHeartbeat, nil)
				reqCancel()

				if err != nil {
					missed++
					if missed >= cfg.MaxMissed {
						if cfg.OnLost != nil {
							cfg.OnLost()
						} else {
							client.Close()
						}
						return
					}
				} else {
					missed = 0
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return func() {
		stopped.Store(true)
		cancel()
		<-done
	}
}
