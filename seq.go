package protoq

import (
	"context"
	"sync"
	"time"
)

// PendingRequest 表示一个等待应答的请求。
// ResponseC 由 WaitForResponse 独占消费；ErrC 接收超时/传输错误。
// done 通道在请求被解决（响应到达或放弃）时关闭，通知 retryLoop 退出。
type PendingRequest struct {
	Seq       uint32
	Frame     *Frame      // 原始请求帧（用于重传）
	ResponseC chan *Frame // 响应通道，仅由 WaitForResponse 消费
	ErrC      chan error  // 错误通道（超时、重传耗尽、连接关闭）
	Retries   int
	CreatedAt time.Time
	done      chan struct{} // 关闭时表示请求已解决，retryLoop 据此退出
}

// SeqManager 管理序列号分配和待确认请求队列。
// 发送端使用：分配序列号 → 入队 → 等待响应或超时重传。
//
// onRetransmit 在 retryLoop 检测到超时且未超过最大重传次数时调用（无锁回调）。
type SeqManager struct {
	mu      sync.Mutex
	next    uint32
	pending map[uint32]*PendingRequest
	closed  bool

	// 配置
	seqLen       int           // 序列号长度（2 或 4）
	retryTimeout time.Duration // 初始重传超时

	// 回调：当需要重传时调用
	onRetransmit func(frame *Frame) error
}

// NewSeqManager 创建一个新的序列号管理器。
// seqLen: 序列号长度（2 或 4 字节）
func NewSeqManager(seqLen int) *SeqManager {
	if seqLen != 2 && seqLen != 4 {
		seqLen = DefaultSeqLen
	}
	return &SeqManager{
		next:         1, // 从 1 开始，0 表示无序列号
		pending:      make(map[uint32]*PendingRequest),
		seqLen:       seqLen,
		retryTimeout: DefaultRetryTimeout,
	}
}

// SetOnRetransmit 设置重传回调。
func (sm *SeqManager) SetOnRetransmit(fn func(frame *Frame) error) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onRetransmit = fn
}

// Allocate 分配一个新的序列号。
// 返回 0 表示管理器已关闭。
func (sm *SeqManager) Allocate() uint32 {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.closed {
		return 0
	}

	seq := sm.next
	sm.next++
	if sm.seqLen == 2 {
		sm.next %= MaxSeq16
	} else {
		sm.next %= MaxSeq32
	}
	if sm.next == 0 {
		sm.next = 1
	}

	// 避免与 pending 中的序列号冲突
	if _, exists := sm.pending[seq]; exists {
		// 简单处理：跳过冲突
		seq = sm.next
		sm.next++
	}

	return seq
}

// Enqueue 将请求加入待确认队列并启动超时定时器。
func (sm *SeqManager) Enqueue(seq uint32, frame *Frame) *PendingRequest {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	pr := &PendingRequest{
		Seq:       seq,
		Frame:     frame,
		ResponseC: make(chan *Frame, 1),
		ErrC:      make(chan error, 1),
		Retries:   0,
		CreatedAt: time.Now(),
		done:      make(chan struct{}),
	}
	sm.pending[seq] = pr

	// 启动超时 goroutine
	go sm.retryLoop(seq, pr)

	return pr
}

// Resolve 处理收到的响应，将响应交付给等待者。
// 返回 true 表示找到了对应的待确认请求。
func (sm *SeqManager) Resolve(seq uint32, response *Frame) bool {
	sm.mu.Lock()
	pr, ok := sm.pending[seq]
	if ok {
		delete(sm.pending, seq)
	}
	sm.mu.Unlock()

	if !ok {
		return false
	}

	// 先关闭 done 通知 retryLoop 停止
	close(pr.done)

	select {
	case pr.ResponseC <- response:
	default:
	}
	return true
}

// retryLoop 超时重传循环。
func (sm *SeqManager) retryLoop(seq uint32, pr *PendingRequest) {
	for {
		timeout := sm.computeTimeout(pr.Retries)

		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			// 检查是否已被解决
			sm.mu.Lock()
			_, stillPending := sm.pending[seq]
			if !stillPending {
				sm.mu.Unlock()
				timer.Stop()
				return
			}
			pr.Retries++
			currentRetries := pr.Retries
			onRetransmit := sm.onRetransmit
			sm.mu.Unlock()

			if currentRetries > MaxRetries {
				// 超过最大重传次数
				sm.mu.Lock()
				delete(sm.pending, seq)
				sm.mu.Unlock()
				close(pr.done)
				select {
				case pr.ErrC <- ErrMaxRetries:
				default:
				}
				timer.Stop()
				return
			}

			// 执行重传
			if onRetransmit != nil {
				if err := onRetransmit(pr.Frame); err != nil {
					sm.mu.Lock()
					delete(sm.pending, seq)
					sm.mu.Unlock()
					close(pr.done)
					select {
					case pr.ErrC <- WrapError("retransmit", err):
					default:
					}
					timer.Stop()
					return
				}
			}

		case <-pr.done:
			// 请求已被解决（响应到达或出错）
			timer.Stop()
			return
		}
	}
}

// computeTimeout 计算重传超时时间（指数退避）。
// retry 从 0 开始。
func (sm *SeqManager) computeTimeout(retry int) time.Duration {
	// 指数退避：1s, 2s, 4s
	multiplier := 1 << retry // 1, 2, 4
	return sm.retryTimeout * time.Duration(multiplier)
}

// Remove 从待确认队列中移除指定序列号。
func (sm *SeqManager) Remove(seq uint32) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if pr, ok := sm.pending[seq]; ok {
		close(pr.done)
	}
	delete(sm.pending, seq)
}

// PendingCount 返回待确认请求数量。
func (sm *SeqManager) PendingCount() int {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return len(sm.pending)
}

// Close 关闭序列号管理器，取消所有待确认请求。
func (sm *SeqManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.closed = true
	for seq, pr := range sm.pending {
		delete(sm.pending, seq)
		close(pr.done)
		select {
		case pr.ErrC <- ErrConnClosed:
		default:
		}
	}
	return nil
}

// WaitForResponse 等待指定序列号的响应（带 context 超时）。
func WaitForResponse(ctx context.Context, pr *PendingRequest) (*Frame, error) {
	select {
	case resp := <-pr.ResponseC:
		return resp, nil
	case err := <-pr.ErrC:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
