// Package biz — 服务端协议配置（Recipe 模式）
//
// ServerRecipe 是一组业务协议的编排规则。它本身不管理任何连接或
// 生命周期，而是通过 Apply() 向 protoq.Server 注册：
//   - 系统操作码的 ConnHandler（协商、心跳、断开）
//   - 连接生命周期钩子（OnConnect、OnClose）
//   - 心跳超时监控 goroutine
//
// 使用方式：
//
//	server := protoq.NewServer(transport.NewTCPTransport())
//	recipe := &biz.ServerRecipe{}  // 零值可用
//	recipe.Apply(server)
//
//	server.Handle(0x0100, handler) // 业务操作码仍直接注册
//	server.ListenAndServe(ctx, ":9090")
package biz

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	protoq "github.com/oh-marshal/protoq"
)

// ServerRecipe 服务端业务协议配置。
// 零值可用（默认协商器 + 默认心跳）。
type ServerRecipe struct {
	// Negotiator 协商策略，nil 时使用 DefaultNegotiator
	Negotiator Negotiator

	// Heartbeat 心跳配置，nil 时使用默认值（30s 间隔，3 次丢失判定断开）
	Heartbeat *HeartbeatConfig

	// 内部状态
	connCtxs   map[uint64]*protoq.ConnContext // connID → ConnContext
	connStates map[uint64]*connState          // connID → state
	statesMu   sync.Mutex

	sessionSeq atomic.Uint64

	// 心跳监控的停止信号
	hbStop chan struct{}
}

// HeartbeatConfig 心跳配置。
type HeartbeatConfig struct {
	// Interval 心跳间隔，默认 30s
	Interval time.Duration

	// Timeout 服务端心跳超时时间（超过此时间未收到心跳则关闭连接），默认 90s
	Timeout time.Duration

	// MaxMissed 最大连续丢失次数（用于替代固定超时），默认 3
	MaxMissed int
}

// connState 单连接的业务状态。
type connState struct {
	negotiated bool
	sessionID  string
	lastPing   time.Time
}

// ──────────────────────────────────────────────
// Apply
// ──────────────────────────────────────────────

// Apply 将本 Recipe 的配置注入到 protoq.Server。
// 必须在 server.ListenAndServe() 之前调用。
func (r *ServerRecipe) Apply(server *protoq.Server) {
	r.connCtxs = make(map[uint64]*protoq.ConnContext)
	r.connStates = make(map[uint64]*connState)

	// 协商器默认值
	neg := r.Negotiator
	if neg == nil {
		neg = &DefaultNegotiator{}
	}

	// 心跳配置默认值
	hbCfg := r.Heartbeat
	if hbCfg == nil {
		hbCfg = DefaultHeartbeatConfig()
	}

	// 注册系统操作码
	server.Handle(OpcodeNegotiate, r.makeNegotiateHandler(neg))
	server.Handle(OpcodeHeartbeat, r.makeHeartbeatHandler(hbCfg))
	server.Handle(OpcodeDisconnect, r.makeDisconnectHandler())

	// 注册连接钩子
	server.OnConnect = r.onConnect
	server.OnClose = r.onClose

	// 启动心跳超时监控
	r.hbStop = make(chan struct{})
	go r.heartbeatWatcher(hbCfg)
}

// Close 停止心跳监控 goroutine。
// 通常在 server.Shutdown() 之后调用。
func (r *ServerRecipe) Close() {
	if r.hbStop != nil {
		close(r.hbStop)
	}
}

// ──────────────────────────────────────────────
// 连接钩子
// ──────────────────────────────────────────────

func (r *ServerRecipe) onConnect(ctx *protoq.ConnContext) {
	r.statesMu.Lock()
	r.connCtxs[ctx.ID] = ctx
	r.connStates[ctx.ID] = &connState{lastPing: time.Now()}
	r.statesMu.Unlock()
}

func (r *ServerRecipe) onClose(ctx *protoq.ConnContext) {
	r.statesMu.Lock()
	delete(r.connCtxs, ctx.ID)
	delete(r.connStates, ctx.ID)
	r.statesMu.Unlock()
}

// ──────────────────────────────────────────────
// ConnHandler 工厂
// ──────────────────────────────────────────────

func (r *ServerRecipe) makeNegotiateHandler(neg Negotiator) protoq.ConnHandler {
	return func(ctx *protoq.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		r.statesMu.Lock()
		cs, exists := r.connStates[ctx.ID]
		if !exists {
			cs = &connState{}
			r.connStates[ctx.ID] = cs
			r.connCtxs[ctx.ID] = ctx
		}
		if cs.negotiated {
			r.statesMu.Unlock()
			return nil, fmt.Errorf("biz: already negotiated")
		}
		r.statesMu.Unlock()

		req, err := UnmarshalNegotiateRequest(body)
		if err != nil {
			return r.marshalReject(ctx.ID, 0, "invalid negotiate: "+err.Error()), ErrNegotiateFailed
		}

		resp := neg.Negotiate(req)
		if resp.Accepted && resp.SessionID == "" {
			resp.SessionID = fmt.Sprintf("sess-%d-%d", ctx.ID, r.sessionSeq.Add(1))
		}
		respBody, _ := MarshalNegotiateResponse(resp)

		r.statesMu.Lock()
		cs = r.connStates[ctx.ID]
		if cs == nil {
			cs = &connState{}
			r.connStates[ctx.ID] = cs
		}
		if resp.Accepted {
			cs.negotiated = true
			cs.sessionID = resp.SessionID
			cs.lastPing = time.Now()
		}
		r.statesMu.Unlock()

		ctx.Set("session_id", resp.SessionID)

		if !resp.Accepted {
			return respBody, ErrNegotiateFailed
		}
		return respBody, nil
	}
}

func (r *ServerRecipe) makeHeartbeatHandler(cfg *HeartbeatConfig) protoq.ConnHandler {
	return func(ctx *protoq.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		r.statesMu.Lock()
		cs, ok := r.connStates[ctx.ID]
		if ok {
			cs.lastPing = time.Now()
		}
		r.statesMu.Unlock()

		if !ok {
			return nil, nil // 未协商的连接忽略心跳
		}
		// 心跳响应无 Body
		return nil, nil
	}
}

func (r *ServerRecipe) makeDisconnectHandler() protoq.ConnHandler {
	return func(ctx *protoq.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		ctx.Close()
		return nil, nil
	}
}

// marshalReject 构造拒绝响应体。
func (r *ServerRecipe) marshalReject(connID uint64, seq uint32, reason string) []byte {
	resp := &NegotiateResponse{
		Accepted:      false,
		ServerVersion: ProtoVersion,
		Reason:        reason,
	}
	body, _ := MarshalNegotiateResponse(resp)
	return body
}

// ──────────────────────────────────────────────
// 心跳监控
// ──────────────────────────────────────────────

func (r *ServerRecipe) heartbeatWatcher(cfg *HeartbeatConfig) {
	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.statesMu.Lock()
			now := time.Now()
			var expired []*protoq.ConnContext
			for id, cs := range r.connStates {
				if cs.negotiated && now.Sub(cs.lastPing) > cfg.Timeout {
					if ctx, ok := r.connCtxs[id]; ok {
						expired = append(expired, ctx)
					}
				}
			}
			r.statesMu.Unlock()

			for _, ctx := range expired {
				ctx.Close()
			}
		case <-r.hbStop:
			return
		}
	}
}

// DefaultHeartbeatConfig 返回默认心跳配置。
func DefaultHeartbeatConfig() *HeartbeatConfig {
	return &HeartbeatConfig{
		Interval:  HeartbeatInterval,
		Timeout:   HeartbeatServerTimeout,
		MaxMissed: HeartbeatMaxMissed,
	}
}
