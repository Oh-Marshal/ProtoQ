// ProtoQ 示例：Echo 服务端和客户端
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

	"github.com/oh-marshal/protoq"
	protoqtransport "github.com/oh-marshal/protoq/transport"
)

var (
	mode      = flag.String("mode", "server", "运行模式: server 或 client")
	transport = flag.String("transport", "tcp", "传输协议: tcp 或 ws")
	addr      = flag.String("addr", ":9090", "监听/连接地址")
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

func runServer() {
	// 选择传输层
	var factory protoq.ListenerFactory
	switch *transport {
	case "tcp":
		factory = protoqtransport.NewTCPTransport()
	case "ws":
		factory = protoqtransport.NewWSTransport()
	default:
		log.Fatalf("不支持的传输协议: %s", *transport)
	}

	server := protoq.NewServer(factory, protoq.WithServerOpcodeLen(2))

	// 注册 Echo 处理函数 (Opcode 0x0001)
	server.Handle(0x0001, func(opcode uint32, body []byte) ([]byte, error) {
		log.Printf("[Echo] 收到请求: %s", string(body))
		return []byte("ECHO: " + string(body)), nil
	})

	// 注册时间查询处理函数 (Opcode 0x0002)
	server.Handle(0x0002, func(opcode uint32, body []byte) ([]byte, error) {
		now := time.Now().Format(time.RFC3339)
		log.Printf("[Time] 返回时间: %s", now)
		return []byte(now), nil
	})

	// 注册状态查询 (Opcode 0x0003)
	server.Handle(0x0003, func(opcode uint32, body []byte) ([]byte, error) {
		status := fmt.Sprintf("server up, conns=%d", server.ActiveConns())
		return []byte(status), nil
	})

	log.Printf("ProtoQ 服务端启动 [%s] %s", *transport, *addr)

	// 优雅关闭
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("收到关闭信号...")
		cancel()
	}()

	if err := server.ListenAndServe(ctx, *addr); err != nil {
		log.Fatalf("服务端错误: %v", err)
	}
	log.Println("服务端已关闭")
}

func runClient() {
	// 选择传输层
	var dialer protoq.Dialer
	switch *transport {
	case "tcp":
		dialer = protoqtransport.NewTCPTransport()
	case "ws":
		dialer = protoqtransport.NewWSTransport()
	default:
		log.Fatalf("不支持的传输协议: %s", *transport)
	}

	ctx := context.Background()
	client, err := protoq.Dial(ctx, dialer, *addr,
		protoq.WithClientOpcodeLen(2),
		protoq.WithClientSeqLen(2),
		protoq.WithClientCRC(true),
	)
	if err != nil {
		log.Fatalf("连接失败: %v", err)
	}
	defer client.Close()

	log.Printf("已连接到 ProtoQ 服务端 [%s] %s", *transport, *addr)

	// 1. 发送 Echo 请求
	log.Println("--- 测试 1: Echo 请求 ---")
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	resp, err := client.SendRequest(reqCtx, 0x0001, []byte("Hello ProtoQ!"))
	cancel()
	if err != nil {
		log.Printf("Echo 请求失败: %v", err)
	} else {
		log.Printf("响应: %s", string(resp.Body))
	}

	// 2. 查询时间
	log.Println("--- 测试 2: 时间查询 ---")
	reqCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	resp, err = client.SendRequest(reqCtx, 0x0002, nil)
	cancel()
	if err != nil {
		log.Printf("时间查询失败: %v", err)
	} else {
		log.Printf("服务端时间: %s", string(resp.Body))
	}

	// 3. 发送通知（无需应答）
	log.Println("--- 测试 3: 单向通知 ---")
	if err := client.SendNotification(0x00FF, []byte("heartbeat")); err != nil {
		log.Printf("通知发送失败: %v", err)
	} else {
		log.Println("通知已发送")
	}

	// 4. 查询服务端状态
	log.Println("--- 测试 4: 状态查询 ---")
	reqCtx, cancel = context.WithTimeout(ctx, 5*time.Second)
	resp, err = client.SendRequest(reqCtx, 0x0003, nil)
	cancel()
	if err != nil {
		log.Printf("状态查询失败: %v", err)
	} else {
		log.Printf("服务端状态: %s", string(resp.Body))
	}

	// 5. 并发发送多个 Echo 请求
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
			resp, err := client.SendRequest(reqCtx, 0x0001, []byte(msg))
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

	// 打印统计
	stats := client.Stats()
	log.Printf("--- 客户端统计 ---")
	log.Printf("  已发送请求: %d", stats.RequestsSent)
	log.Printf("  已收到响应: %d", stats.ResponsesReceived)
	log.Printf("  已发送通知: %d", stats.NotificationsSent)
	log.Printf("  待确认请求: %d", stats.PendingRequests)
}
