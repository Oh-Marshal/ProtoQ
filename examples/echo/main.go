// ProtoQ 示例：Echo 服务端和客户端（集成 biz 业务协议层）
//
// 演示内容：
//   1. 服务端：biz.ServerRecipe 注入协商 + 心跳 + 断开处理
//   2. 客户端：message.Negotiate 协商 + message.StartHeartbeat 心跳 + 业务调用
//   3. 自定义认证协商器（token 验证）
//   4. 业务操作码使用 biz 区段（0x0100+）
//
// 运行方式：
//
//	# 终端 1 - 启动 TCP 服务端
//	go run . -mode server -transport tcp -addr :9090
//
//	# 终端 2 - 启动 TCP 客户端
//	go run . -mode client -transport tcp -addr :9090
//
//	# WebSocket 模式
//	go run . -mode server -transport ws -addr :8080
//	go run . -mode client -transport ws -addr :8080
//
//	# 带 token 认证
//	go run . -mode server -transport tcp -addr :9090 -token my-secret
//	go run . -mode client -transport tcp -addr :9090 -token my-secret
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	protoq "github.com/oh-marshal/protoq"
	"github.com/oh-marshal/protoq/basic/constant"
	exception "github.com/oh-marshal/protoq/basic/exception"
	"github.com/oh-marshal/protoq/basic/message"
	client "github.com/oh-marshal/protoq/client"
	protoqtransport "github.com/oh-marshal/protoq/netty"
	serverpkg "github.com/oh-marshal/protoq/server"
)

var (
	mode          = flag.String("mode", "server", "运行模式: server 或 client")
	transportName = flag.String("transport", "tcp", "传输协议: tcp 或 ws")
	addr          = flag.String("addr", ":9090", "监听/连接地址")
	token         = flag.String("token", "", "认证 token（为空则跳过认证）")
)

// ─── 业务操作码（biz 区段 0x0100–0xFEFF）─────────────────────────────────

const (
	OpEcho   uint32 = 0x0100 // Echo 回显
	OpTime   uint32 = 0x0101 // 时间查询
	OpStatus uint32 = 0x0102 // 服务端状态
	OpNotify uint32 = 0x0103 // 单向通知
)

func main() {
	flag.Parse()

	switch *mode {
	case "server":
		runServer()
	case "client":
		runClient()
	default:
		log.Fatalf("未知模式: %s (可选: server, client)", *mode)
	}
}

// ─── 服务端协商/心跳 Handler（内联实现，替代 ServerRecipe）────────────────

// makeServerNegotiateHandler 创建协商处理器（ConnHandler 签名）。
func makeServerNegotiateHandler(neg message.Negotiator) serverpkg.ConnHandler {
	return func(ctx *serverpkg.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		req, err := message.UnmarshalNegotiateRequest(body)
		if err != nil {
			resp := &message.NegotiateResponse{Accepted: false, ServerVersion: message.ProtoVersion, Reason: "invalid request"}
			rejectBody, _ := message.MarshalNegotiateResponse(resp)
			return rejectBody, exception.ErrNegotiateFailed
		}
		resp := neg.Negotiate(req)
		if resp.Accepted {
			ctx.Set("prop.codec.type", req.Encryption)
			if resp.SessionID == "" {
				resp.SessionID = fmt.Sprintf("sess-%d", ctx.ID)
			}
			ctx.Set("session_id", resp.SessionID)
		}
		respBody, _ := message.MarshalNegotiateResponse(resp)
		if !resp.Accepted {
			return respBody, exception.ErrNegotiateFailed
		}
		return respBody, nil
	}
}

// makeServerHeartbeatHandler 创建心跳处理器（ConnHandler 签名）。
func makeServerHeartbeatHandler() serverpkg.ConnHandler {
	return func(ctx *serverpkg.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		return nil, nil // PONG 无 Body
	}
}

// tokenNegotiator 基于 token 的认证协商器。
type tokenNegotiator struct {
	requiredToken string // 为空则跳过认证
}

func (n *tokenNegotiator) Negotiate(req *message.NegotiateRequest) *message.NegotiateResponse {
	// 版本校验：仅接受版本 1
	if req.Version != 1 {
		return &message.NegotiateResponse{
			Accepted:      false,
			ServerVersion: message.ProtoVersion,
			Reason:        fmt.Sprintf("unsupported protocol version: %d", req.Version),
		}
	}

	// 加密校验：仅接受 "none"
	if req.Encryption != "" && req.Encryption != "none" {
		return &message.NegotiateResponse{
			Accepted:      false,
			ServerVersion: message.ProtoVersion,
			Reason:        fmt.Sprintf("unsupported encryption: %s", req.Encryption),
		}
	}

	// Token 校验（如果配置了）
	if n.requiredToken != "" {
		if req.Auth == nil || req.Auth.Token != n.requiredToken {
			return &message.NegotiateResponse{
				Accepted:      false,
				ServerVersion: message.ProtoVersion,
				Reason:        "invalid or missing token",
			}
		}
	}

	return &message.NegotiateResponse{
		Accepted:      true,
		ServerVersion: message.ProtoVersion,
		ServerTime:    time.Now().Unix(),
	}
}

// ─── 服务端 ────────────────────────────────────────────────────────────────

func runServer() {
	// 1. 选择传输层
	var factory protoq.ListenerFactory
	switch *transportName {
	case "tcp":
		factory = protoqtransport.NewTCPTransport()
	case "ws":
		factory = protoqtransport.NewWSTransport()
	default:
		log.Fatalf("不支持的传输协议: %s", *transportName)
	}

	// 2. 创建 protoq 服务端
	server := serverpkg.NewServer(factory, serverpkg.WithServerOpcodeLen(2))

	// 3. 创建 biz 配置：自定义协商器
	negotiator := &tokenNegotiator{requiredToken: *token}

	// 注册系统操作码（协商、心跳）通过 server.Handle 直接注册
	server.Handle(constant.OpcodeNegotiate, makeServerNegotiateHandler(negotiator))
	server.Handle(constant.OpcodeHeartbeat, makeServerHeartbeatHandler())

	// 4. 注册业务操作码（ConnHandler 可获取连接上下文 + 会话元数据）
	// Echo: 回显请求体
	server.Handle(OpEcho, func(ctx *serverpkg.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		sessionID, _ := ctx.GetString("session_id")
		log.Printf("[Echo] conn=%d session=%s body=%q", ctx.ID, sessionID, string(body))
		return []byte("ECHO: " + string(body)), nil
	})

	// Time: 返回服务端当前时间
	server.Handle(OpTime, func(ctx *serverpkg.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		now := time.Now().Format(time.RFC3339)
		log.Printf("[Time] conn=%d → %s", ctx.ID, now)
		return []byte(now), nil
	})

	// Status: 返回服务端运行状态
	server.Handle(OpStatus, func(ctx *serverpkg.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		status := fmt.Sprintf("server up | protocol=%s | addr=%s | active_conns=%d",
			factory.Protocol(), *addr, server.ActiveConns())
		return []byte(status), nil
	})

	// Notify: 单向通知（仅打印，不返回响应体）
	server.Handle(OpNotify, func(ctx *serverpkg.ConnContext, opcode uint32, body []byte) ([]byte, error) {
		sessionID, _ := ctx.GetString("session_id")
		log.Printf("[Notify] conn=%d session=%s body=%q", ctx.ID, sessionID, string(body))
		return nil, nil
	})

	authInfo := "无认证"
	if *token != "" {
		authInfo = fmt.Sprintf("token=%q", *token)
	}
	log.Printf("ProtoQ Echo 服务端启动 [%s] %s (%s)", *transportName, *addr, authInfo)

	// 5. 优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("收到关闭信号，正在停止...")
		cancel()
	}()

	if err := server.ListenAndServe(ctx, *addr); err != nil {
		log.Fatalf("服务端错误: %v", err)
	}
	log.Println("服务端已关闭")
}

// ─── 客户端 ────────────────────────────────────────────────────────────────

func runClient() {
	// 1. 选择传输层
	var dialer protoq.Dialer
	switch *transportName {
	case "tcp":
		dialer = protoqtransport.NewTCPTransport()
	case "ws":
		dialer = protoqtransport.NewWSTransport()
	default:
		log.Fatalf("不支持的传输协议: %s", *transportName)
	}

	// 2. 建立连接
	ctx := context.Background()
	client, err := client.Dial(ctx, dialer, *addr,
		client.WithClientOpcodeLen(2),
		client.WithClientSeqLen(2),
		client.WithClientCRC(true),
	)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	log.Printf("已连接到 ProtoQ 服务端 [%s] %s", *transportName, *addr)

	// 3. 内容协商
	log.Println("--- 步骤 1: 内容协商 ---")
	var negotiateOpts []message.NegotiateOption
	if *token != "" {
		negotiateOpts = append(negotiateOpts, message.WithAuth(*token))
	}
	negResp, err := message.Negotiate(ctx, client, negotiateOpts...)
	if err != nil {
		log.Fatalf("协商失败: %v", err)
	}
	log.Printf("  协商成功: session_id=%s server_version=%d server_time=%s",
		negResp.SessionID, negResp.ServerVersion,
		time.Unix(negResp.ServerTime, 0).Format(time.RFC3339))

	// 4. 启动心跳
	log.Println("--- 步骤 2: 启动心跳 ---")
	// 演示用短间隔心跳（生产环境使用默认 30s）
	hbCfg := &message.HeartbeatLoopConfig{
		Interval:  3 * time.Second,
		Timeout:   2 * time.Second,
		MaxMissed: 3,
	}
	stopHeartbeat := message.StartHeartbeat(client, hbCfg)
	defer stopHeartbeat()
	log.Printf("  心跳已启动（间隔=%v，超时=%v，最大丢失=%d）",
		hbCfg.Interval, hbCfg.Timeout, hbCfg.MaxMissed)

	// 5. 业务调用

	// 5a. Echo 请求
	log.Println("--- 测试 1: Echo 请求 ---")
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := client.SendRequest(reqCtx, OpEcho, []byte("Hello ProtoQ!"))
	cancel()
	if err != nil {
		log.Printf("  Echo 请求失败: %v", err)
	} else {
		log.Printf("  响应: %s", string(resp.Body))
	}

	// 5b. 时间查询
	log.Println("--- 测试 2: 时间查询 ---")
	reqCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	resp, err = client.SendRequest(reqCtx, OpTime, nil)
	cancel()
	if err != nil {
		log.Printf("  时间查询失败: %v", err)
	} else {
		log.Printf("  服务端时间: %s", string(resp.Body))
	}

	// 5c. 单向通知
	log.Println("--- 测试 3: 单向通知 ---")
	if err := client.SendNotification(OpNotify, []byte("hello from client")); err != nil {
		log.Printf("  通知发送失败: %v", err)
	} else {
		log.Println("  通知已发送")
	}

	// 5d. 查询服务端状态
	log.Println("--- 测试 4: 状态查询 ---")
	reqCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	resp, err = client.SendRequest(reqCtx, OpStatus, nil)
	cancel()
	if err != nil {
		log.Printf("  状态查询失败: %v", err)
	} else {
		log.Printf("  服务端状态: %s", string(resp.Body))
	}

	// 5e. 并发 Echo 请求
	log.Println("--- 测试 5: 并发请求 ---")
	type result struct {
		idx int
		msg string
		err error
	}
	results := make(chan result, 5)
	for i := 0; i < 5; i++ {
		go func(idx int) {
			msg := fmt.Sprintf("并发消息 #%d", idx)
			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			resp, err := client.SendRequest(reqCtx, OpEcho, []byte(msg))
			cancel()
			if err != nil {
				results <- result{idx: idx, err: err}
			} else {
				results <- result{idx: idx, msg: string(resp.Body)}
			}
		}(i)
	}
	for i := 0; i < 5; i++ {
		r := <-results
		if r.err != nil {
			log.Printf("  并发 #%d 失败: %v", r.idx, r.err)
		} else {
			log.Printf("  并发 #%d: %s", r.idx, r.msg)
		}
	}

	// 6. 打印统计
	stats := client.Stats()
	log.Println("--- 客户端统计 ---")
	log.Printf("  传输协议: %s", dialer.Protocol())
	log.Printf("  会话 ID:  %s", negResp.SessionID)
	log.Printf("  已发送请求: %d", stats.RequestsSent)
	log.Printf("  已收到响应: %d", stats.ResponsesReceived)
	log.Printf("  已发送通知: %d", stats.NotificationsSent)
	log.Printf("  待确认请求: %d", stats.PendingRequests)
}
