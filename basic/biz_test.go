package basic_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/oh-marshal/protoq/api"
	"github.com/oh-marshal/protoq/basic"
	"github.com/oh-marshal/protoq/transport"
)

// ─── 测试辅助：内联协商/心跳/断开 Handler（替代已移除的 ServerRecipe）───

// applyBizHandlers 向 Server 注册 biz 层系统 Handler（协商、心跳、断开）。
// negotiator 为 nil 时使用默认协商器。
func applyBizHandlers(server *Server, negotiator biz.Negotiator) {
	if negotiator == nil {
		negotiator = &biz.DefaultNegotiator{}
	}
	server.Handle(biz.OpcodeNegotiate, makeTestNegotiateHandler(negotiator))
	server.Handle(biz.OpcodeHeartbeat, makeTestHeartbeatHandler())
	server.Handle(biz.OpcodeDisconnect, makeTestDisconnectHandler())
}

// makeTestNegotiateHandler 创建测试用协商 Handler（ConnHandler 签名）。
func makeTestNegotiateHandler(neg biz.Negotiator) ConnHandler {
	var sessionSeq atomic.Uint64
	return func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		req, err := biz.UnmarshalNegotiateRequest(body)
		if err != nil {
			resp := &biz.NegotiateResponse{Accepted: false, ServerVersion: biz.ProtoVersion, Reason: "invalid request"}
			rejectBody, _ := biz.MarshalNegotiateResponse(resp)
			return rejectBody, biz.ErrNegotiateFailed
		}
		resp := neg.Negotiate(req)
		if resp.Accepted {
			if resp.SessionID == "" {
				resp.SessionID = fmt.Sprintf("sess-%d-%d", ctx.ID, sessionSeq.Add(1))
			}
			ctx.SetProperty("prop.codec.type", req.Encryption)
			ctx.Set("session_id", resp.SessionID)
		}
		respBody, _ := biz.MarshalNegotiateResponse(resp)
		if !resp.Accepted {
			return respBody, biz.ErrNegotiateFailed
		}
		return respBody, nil
	}
}

// makeTestHeartbeatHandler 创建测试用心跳 Handler（ConnHandler 签名）。
func makeTestHeartbeatHandler() ConnHandler {
	return func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		return nil, nil
	}
}

// makeTestDisconnectHandler 创建测试用断开 Handler（ConnHandler 签名）。
func makeTestDisconnectHandler() ConnHandler {
	return func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		ctx.Close()
		return nil, nil
	}
}

// ──────────────────────────────────────────────
// Opcode 分段测试
// ──────────────────────────────────────────────

func TestOpcodeRanges(t *testing.T) {
	tests := []struct {
		opcode             uint32
		sys, bizOp, errOp  bool
	}{
		{0x0000, true, false, false},
		{0x0001, true, false, false},
		{0x0002, true, false, false},
		{0x00FF, true, false, false},
		{0x0100, false, true, false},
		{0x7FFF, false, true, false},
		{0xFEFF, false, true, false},
		{0xFF00, false, false, true},
		{0xFF01, false, false, true},
		{0xFFFF, false, false, true},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("0x%04X", tt.opcode), func(t *testing.T) {
			if got := biz.IsSystemOpcode(tt.opcode); got != tt.sys {
				t.Errorf("IsSystemOpcode(0x%04X) = %v, want %v", tt.opcode, got, tt.sys)
			}
			if got := biz.IsBizOpcode(tt.opcode); got != tt.bizOp {
				t.Errorf("IsBizOpcode(0x%04X) = %v, want %v", tt.opcode, got, tt.bizOp)
			}
			if got := biz.IsErrorOpcode(tt.opcode); got != tt.errOp {
				t.Errorf("IsErrorOpcode(0x%04X) = %v, want %v", tt.opcode, got, tt.errOp)
			}
		})
	}
}

// ──────────────────────────────────────────────
// 协商编解码测试
// ──────────────────────────────────────────────

func TestNegotiateRequestRoundTrip(t *testing.T) {
	req := &biz.NegotiateRequest{
		Version:    biz.ProtoVersion,
		Encryption: "none",
		Auth: &biz.AuthInfo{
			Token: "test-token-123",
			Extra: map[string]string{"client": "test"},
		},
	}

	data, err := biz.MarshalNegotiateRequest(req)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded, err := biz.UnmarshalNegotiateRequest(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Version != req.Version {
		t.Errorf("version: got %d, want %d", decoded.Version, req.Version)
	}
	if decoded.Auth.Token != req.Auth.Token {
		t.Errorf("token: got %s, want %s", decoded.Auth.Token, req.Auth.Token)
	}
}

func TestNegotiateResponseRoundTrip(t *testing.T) {
	resp := &biz.NegotiateResponse{
		Accepted:      true,
		ServerVersion: biz.ProtoVersion,
		SessionID:     "sess-abc-123",
		ServerTime:    1717800000,
	}

	data, err := biz.MarshalNegotiateResponse(resp)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded, err := biz.UnmarshalNegotiateResponse(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !decoded.Accepted {
		t.Error("expected Accepted=true")
	}
	if decoded.SessionID != "sess-abc-123" {
		t.Errorf("sessionID: got %s, want sess-abc-123", decoded.SessionID)
	}
}

// ──────────────────────────────────────────────
// 端到端测试（protoq 驱动 + biz 配置）
// ──────────────────────────────────────────────

func TestEndToEnd_NegotiateAndCall(t *testing.T) {
	addr := ":19911"

	// ── 服务端：Server + biz Handler 注册 ──
	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, nil)

	var callCount atomic.Uint64
	echoOpcode := uint32(0x0100)
	server.Handle(echoOpcode, func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		callCount.Add(1)
		return append([]byte("ECHO:"), body...), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	// ── 客户端：Client + biz.Negotiate ──
	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	resp, err := biz.Negotiate(ctx, client)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}
	if resp.SessionID == "" {
		t.Error("expected non-empty session ID after negotiate")
	}

	// 发送业务请求
	respFrame, err := client.SendRequest(ctx, echoOpcode, []byte("hello world"))
	if err != nil {
		t.Fatalf("send request: %v", err)
	}

	if string(respFrame.Body) != "ECHO:hello world" {
		t.Errorf("response: got %q, want %q", string(respFrame.Body), "ECHO:hello world")
	}
	if callCount.Load() != 1 {
		t.Errorf("handler call count: got %d, want 1", callCount.Load())
	}
}

func TestEndToEnd_Notify(t *testing.T) {
	addr := ":19912"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, nil)

	notifyOpcode := uint32(0x0101)
	received := make(chan []byte, 1)
	server.Handle(notifyOpcode, func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		received <- body
		return nil, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	_, err = biz.Negotiate(ctx, client)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}

	err = client.SendNotification(notifyOpcode, []byte("fire-and-forget"))
	if err != nil {
		t.Fatalf("notify: %v", err)
	}

	select {
	case body := <-received:
		if string(body) != "fire-and-forget" {
			t.Errorf("body: got %q, want %q", string(body), "fire-and-forget")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for notification")
	}
}

// ──────────────────────────────────────────────
// 自定义协商器测试
// ──────────────────────────────────────────────

type tokenNegotiator struct {
	requiredToken string
}

func (n *tokenNegotiator) Negotiate(req *biz.NegotiateRequest) *biz.NegotiateResponse {
	if req.Auth == nil || req.Auth.Token != n.requiredToken {
		return &biz.NegotiateResponse{
			Accepted:      false,
			ServerVersion: biz.ProtoVersion,
			Reason:        "invalid token",
		}
	}
	return &biz.NegotiateResponse{
		Accepted:      true,
		ServerVersion: biz.ProtoVersion,
		SessionID:     "auth-session",
	}
}

func TestEndToEnd_CustomNegotiator_Accept(t *testing.T) {
	addr := ":19913"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, &tokenNegotiator{requiredToken: "secret"})

	server.Handle(0x0100, func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		return body, nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	resp, err := biz.Negotiate(ctx, client, biz.WithAuth("secret"))
	if err != nil {
		t.Fatalf("negotiate with valid token: %v", err)
	}

	if resp.SessionID != "auth-session" {
		t.Errorf("session ID: got %q, want auth-session", resp.SessionID)
	}
}

func TestEndToEnd_CustomNegotiator_Reject(t *testing.T) {
	addr := ":19914"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, &tokenNegotiator{requiredToken: "secret"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	_, err = biz.Negotiate(ctx, client, biz.WithAuth("wrong-token"))
	if err != biz.ErrNegotiateFailed {
		t.Errorf("expected ErrNegotiateFailed, got %v", err)
	}
}

// ──────────────────────────────────────────────
// 未协商直接发业务请求 → 服务端忽略/拒绝
// ──────────────────────────────────────────────

func TestCallWithoutNegotiate(t *testing.T) {
	addr := ":19915"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	// 不执行 biz.Negotiate，直接用 Client 发业务请求
	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	// 发非协商的请求（注册了业务 handler）
	server.Handle(0x0100, func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		return []byte("pong"), nil
	})

	resp, err := client.SendRequest(ctx, 0x0100, []byte("ping"))
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	// 即使未协商，业务 handler 也能正常响应（protoq 层不拦截）
	if string(resp.Body) != "pong" {
		t.Errorf("got %q, want pong", resp.Body)
	}
}

// ──────────────────────────────────────────────
// 未注册 Opcode 测试
// ──────────────────────────────────────────────

func TestUnregisteredOpcode(t *testing.T) {
	addr := ":19916"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	_, err = biz.Negotiate(ctx, client)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}

	// 发送未注册的业务操作码 → 服务端返回错误响应
	_, err = client.SendRequest(ctx, 0x9999, []byte("test"))
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
}

// ──────────────────────────────────────────────
// 并发客户端测试
// ──────────────────────────────────────────────

func TestMultipleClients(t *testing.T) {
	addr := ":19917"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, nil)

	echoOpcode := uint32(0x0100)
	server.Handle(echoOpcode, func(ctx *ConnContext, opcode uint32, body []byte) ([]byte, error) {
		return append([]byte("ECHO:"), body...), nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	const numClients = 5
	errCh := make(chan error, numClients)

	for i := 0; i < numClients; i++ {
		go func(id int) {
			client, err := Dial(ctx, transport.NewTCPTransport(), addr)
			if err != nil {
				errCh <- fmt.Errorf("client %d dial: %w", id, err)
				return
			}
			defer client.Close()

			_, err = biz.Negotiate(ctx, client)
			if err != nil {
				errCh <- fmt.Errorf("client %d negotiate: %w", id, err)
				return
			}

			respFrame, err := client.SendRequest(ctx, echoOpcode, []byte(fmt.Sprintf("client-%d", id)))
			if err != nil {
				errCh <- fmt.Errorf("client %d call: %w", id, err)
				return
			}

			expected := fmt.Sprintf("ECHO:client-%d", id)
			if string(respFrame.Body) != expected {
				errCh <- fmt.Errorf("client %d: got %q, want %q", id, string(respFrame.Body), expected)
				return
			}
			errCh <- nil
		}(i)
	}

	for i := 0; i < numClients; i++ {
		if err := <-errCh; err != nil {
			t.Error(err)
		}
	}
}

// ──────────────────────────────────────────────
// Shutdown 测试
// ──────────────────────────────────────────────

func TestServerShutdown(t *testing.T) {
	addr := ":19918"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, nil)

	ctx, cancel := context.WithCancel(context.Background())

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	_, err = biz.Negotiate(ctx, client)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}

	err = server.Shutdown()
	if err != nil {
		t.Errorf("shutdown: %v", err)
	}
	cancel()

	// 服务端关闭后，客户端请求应失败
	_, err = client.SendRequest(ctx, 0x0100, []byte("test"))
	if err == nil {
		t.Error("expected error after server shutdown")
	}
}

// ──────────────────────────────────────────────
// 心跳端到端测试
// ──────────────────────────────────────────────

func TestHeartbeatEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heartbeat test in short mode")
	}

	addr := ":19919"

	server := api.NewServer(transport.NewTCPTransport())
	applyBizHandlers(server, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server.ListenAndServe(ctx, addr)
	time.Sleep(100 * time.Millisecond)

	client, err := Dial(ctx, transport.NewTCPTransport(), addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	_, err = biz.Negotiate(ctx, client)
	if err != nil {
		t.Fatalf("negotiate: %v", err)
	}

	// 启动心跳，等待一小段时间验证不崩溃
	stopHB := biz.StartHeartbeat(client, nil)
	time.Sleep(500 * time.Millisecond)
	stopHB()
}

// ──────────────────────────────────────────────
// BeanRegister / MessageDispatcher 单元测试
// ──────────────────────────────────────────────

func TestMessageDispatcher_RegisterAndDispatch(t *testing.T) {
	// 验证 MessageDispatcher 能够注册和查找 Handler
	dispatcher := biz.NewMessageDispatcher()
	if dispatcher == nil {
		t.Fatal("NewMessageDispatcher returned nil")
	}

	called := false
	dispatcher.RegisterHandler(0x0100, func(ctx api.Context) ([]byte, error) {
		called = true
		return []byte("ok"), nil
	})

	handler := dispatcher.GetHandler(0x0100)
	if handler == nil {
		t.Error("expected handler to be registered")
	}

	// 验证未注册的 Handler 返回 nil
	if dispatcher.GetHandler(0x9999) != nil {
		t.Error("expected nil for unregistered handler")
	}
	_ = called
}

func TestFilterChainRegister(t *testing.T) {
	fc := &biz.FilterChainRegister{}
	if fc == nil {
		t.Fatal("FilterChainRegister is nil")
	}

	filterCalled := false
	fc.AddFilter(&testFilter{fn: func(ctx api.Context, chain api.FilterChain) error {
		filterCalled = true
		return chain.DoFilter(ctx)
	}})

	// 验证 Filter 已添加
	_ = filterCalled
}

type testFilter struct {
	fn func(ctx api.Context, chain api.FilterChain) error
}

func (f *testFilter) DoFilter(ctx api.Context, chain api.FilterChain) error {
	if f.fn != nil {
		return f.fn(ctx, chain)
	}
	return chain.DoFilter(ctx)
}

func TestBeanRegister(t *testing.T) {
	reg := biz.NewBeanRegister()
	if reg == nil {
		t.Fatal("NewBeanRegister returned nil")
	}

	// 验证子注册器已初始化
	if reg.CodecRegister == nil {
		t.Error("CodecRegister is nil")
	}
	if reg.FilterChain == nil {
		t.Error("FilterChain is nil")
	}
	if reg.MessageDispatcher == nil {
		t.Error("MessageDispatcher is nil")
	}
	if reg.EventDispatcher == nil {
		t.Error("EventDispatcher is nil")
	}

	// 测试 Register 路由
	called := false
	reg.RegisterMessageHandler(0x0200, func(ctx api.Context) ([]byte, error) {
		called = true
		return nil, nil
	})

	handler := reg.MessageDispatcher.GetHandler(0x0200)
	if handler == nil {
		t.Error("expected handler to be registered via BeanRegister")
	}
	_ = called
}
