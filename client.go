package protoq

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
)

// Client 是 ProtoQ 协议客户端。
// 支持通过 TCP 或 WebSocket 连接到服务端，发送请求和通知。
type Client struct {
	// Conn 共享连接抽象（嵌入），拥有连接的生命周期
	*Conn

	seqMgr *SeqManager

	// 读循环完成信号
	readDone chan struct{}

	// 关闭同步（Close 需等待 readLoop 退出）
	closeMu sync.RWMutex

	// 配置
	opcodeLen int // Opcode 字段长度（0/2/4）
	seqLen    int // Seq 字段长度（2/4，需要 ACK 时）
	useCRC    bool

	// 指标
	requestsSent      atomic.Uint64
	responsesReceived atomic.Uint64
	notificationsSent atomic.Uint64
}

// ClientOption 是 Client 的配置选项。
type ClientOption func(*Client)

// WithClientOpcodeLen 设置客户端 Opcode 字段长度。
func WithClientOpcodeLen(n int) ClientOption {
	return func(c *Client) { c.opcodeLen = n }
}

// WithClientSeqLen 设置客户端 Seq 字段长度。
func WithClientSeqLen(n int) ClientOption {
	return func(c *Client) { c.seqLen = n }
}

// WithClientCRC 启用 CRC 校验。
func WithClientCRC(enable bool) ClientOption {
	return func(c *Client) { c.useCRC = enable }
}

// NewClient 创建一个 ProtoQ 客户端。
// rawConn 是已建立的连接（由 Transport.Dial 返回）。
func NewClient(rawConn net.Conn, opts ...ClientOption) *Client {
	c := &Client{
		Conn:      NewConn(context.Background(), rawConn),
		seqMgr:    NewSeqManager(DefaultSeqLen),
		readDone:  make(chan struct{}),
		opcodeLen: DefaultOpcodeLen,
		seqLen:    DefaultSeqLen,
		useCRC:    true,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.seqMgr = NewSeqManager(c.seqLen)
	c.seqMgr.SetOnRetransmit(c.retransmit)

	// 启动读循环
	go c.readLoop()

	return c
}

// SendRequest 发送一个需要应答的请求。
// 自动分配序列号，等待响应或超时。
// ctx 用于请求级别的超时控制。
func (c *Client) SendRequest(ctx context.Context, opcode uint32, body []byte) (*Frame, error) {
	if c.IsClosed() {
		return nil, ErrConnClosed
	}

	seq := c.seqMgr.Allocate()
	if seq == 0 {
		return nil, ErrConnClosed
	}

	frame := NewRequestFrame(opcode, seq, body, true, c.useCRC)
	frame.Flags = frame.Flags.SetOpcodeLen(c.opcodeLen)
	frame.Flags = frame.Flags.SetSeqLen(c.seqLen)

	// 入队
	pr := c.seqMgr.Enqueue(seq, frame)

	// 发送
	if err := c.WriteFrame(frame); err != nil {
		c.seqMgr.Remove(seq)
		return nil, WrapError("send request", err)
	}
	c.requestsSent.Add(1)

	// 等待响应
	resp, err := WaitForResponse(ctx, pr)
	if err != nil {
		c.seqMgr.Remove(seq)
		return nil, err
	}
	c.responsesReceived.Add(1)
	return resp, nil
}

// SendNotification 发送一个无需应答的单向通知。
func (c *Client) SendNotification(opcode uint32, body []byte) error {
	if c.IsClosed() {
		return ErrConnClosed
	}

	frame := NewNotificationFrame(opcode, body, c.useCRC)
	frame.Flags = frame.Flags.SetOpcodeLen(c.opcodeLen)

	if err := c.WriteFrame(frame); err != nil {
		return WrapError("send notification", err)
	}
	c.notificationsSent.Add(1)
	return nil
}

// retransmit 重传帧（由 SeqManager 在超时时调用）。
func (c *Client) retransmit(f *Frame) error {
	return c.WriteFrame(f)
}

// readLoop 持续读取响应帧并分发。
func (c *Client) readLoop() {
	defer close(c.readDone)

	for {
		frame, err := c.Decode()
		if err != nil {
			if err == io.EOF || c.IsClosed() {
				return
			}
			continue
		}

		if frame.IsResponse() {
			c.seqMgr.Resolve(frame.Seq, frame)
		}
	}
}

// Close 关闭客户端连接。
// Conn.Close() 取消 context 并关闭底层连接（幂等）。
func (c *Client) Close() error {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.IsClosed() {
		return nil
	}

	c.seqMgr.Close()
	err := c.Conn.Close()
	<-c.readDone
	return err
}

// Stats 返回客户端统计信息。
func (c *Client) Stats() ClientStats {
	return ClientStats{
		RequestsSent:      c.requestsSent.Load(),
		ResponsesReceived: c.responsesReceived.Load(),
		NotificationsSent: c.notificationsSent.Load(),
		PendingRequests:   c.seqMgr.PendingCount(),
	}
}

// ClientStats 客户端统计信息。
type ClientStats struct {
	RequestsSent      uint64
	ResponsesReceived uint64
	NotificationsSent uint64
	PendingRequests   int
}

// Dial 使用指定传输协议连接到 ProtoQ 服务端并返回 Client。
// 示例：
//
//	client, err := protoq.Dial(ctx, protoq.NewTCPTransport(), "127.0.0.1:9090")
func Dial(ctx context.Context, transport Dialer, addr string, opts ...ClientOption) (*Client, error) {
	conn, err := transport.Dial(ctx, addr)
	if err != nil {
		return nil, fmt.Errorf("protoq dial %s: %w", transport.String(), err)
	}
	return NewClient(conn, opts...), nil
}
