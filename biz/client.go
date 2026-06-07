// Package biz — 客户端辅助函数
//
// biz 客户端侧不提供包装类型，只提供方便函数，让用户直接使用 protoq.Client：
//
//	// 1. 建立连接
//	client, err := protoq.Dial(ctx, transport.NewTCPTransport(), ":9090")
//
//	// 2. 执行协商
//	resp, err := biz.Negotiate(ctx, client, biz.WithAuth("my-token"))
//
//	// 3. 启动心跳
//	stopHeartbeat := biz.StartHeartbeat(client, nil)
//	defer stopHeartbeat()
//
//	// 4. 正常使用
//	respFrame, err := client.SendRequest(ctx, 0x0100, body)
//	client.SendNotification(0x0101, body)
//
//	// 5. 关闭
//	client.Close()
package biz

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	protoq "github.com/oh-marshal/protoq"
)

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
// 返回服务端的协商响应。
// 必须在使用其他业务操作码之前调用。
func Negotiate(ctx context.Context, client *protoq.Client, opts ...NegotiateOption) (*NegotiateResponse, error) {
	req := &NegotiateRequest{
		Version:    ProtoVersion,
		Encryption: "none",
	}
	for _, opt := range opts {
		opt(req)
	}

	reqBody, err := MarshalNegotiateRequest(req)
	if err != nil {
		return nil, fmt.Errorf("biz: marshal negotiate request: %w", err)
	}

	respFrame, err := client.SendRequest(ctx, OpcodeNegotiate, reqBody)
	if err != nil {
		return nil, fmt.Errorf("biz: negotiate: %w", err)
	}

	resp, err := UnmarshalNegotiateResponse(respFrame.Body)
	if err != nil {
		return nil, fmt.Errorf("biz: unmarshal negotiate response: %w", err)
	}

	if !resp.Accepted {
		return resp, ErrNegotiateFailed
	}

	return resp, nil
}

// MustNegotiate 执行协商，失败时 panic（仅用于初始化阶段）。
func MustNegotiate(ctx context.Context, client *protoq.Client, opts ...NegotiateOption) *NegotiateResponse {
	resp, err := Negotiate(ctx, client, opts...)
	if err != nil {
		panic("biz: negotiate failed: " + err.Error())
	}
	return resp
}

// ──────────────────────────────────────────────
// 心跳
// ──────────────────────────────────────────────

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
// 返回一个 stop 函数，调用它即可停止心跳。
//
// 心跳丢失时，默认行为是关闭 client。可通过 cfg.OnLost 自定义。
func StartHeartbeat(client *protoq.Client, cfg *HeartbeatLoopConfig) (stop func()) {
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
