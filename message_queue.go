// Package protoq — 消息等待队列
//
// 对标 Java uni-protocol 的 MessageQueue 类。
// 管理需要 ACK 的请求的 pending Future（Go 中用 channel），
// 服务端响应到达时通过 sequence 匹配并完成回调。
package protoq

import (
	"fmt"
	"sync"
)

// MessageQueue ACK 等待队列。对标 uni-protocol org.facelang.unified.proto.model.MessageQueue。
//
// 每个连接持有一个 MessageQueue 实例，按序列号索引等待中的请求。
// sequence → chan *Frame 映射，完成时自动清理。
type MessageQueue struct {
	mu      sync.Mutex
	pending map[uint32]chan *Frame
	count   int
}

// NewMessageQueue 创建新的消息队列。
func NewMessageQueue() *MessageQueue {
	return &MessageQueue{
		pending: make(map[uint32]chan *Frame),
	}
}

// Count 返回当前等待 ACK 的请求数。对标 uni-protocol MessageQueue.getCount()。
func (q *MessageQueue) Count() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.count
}

// Get 获取指定 sequence 的等待 channel。对标 uni-protocol MessageQueue.get(sequence)。
// 不存在时返回 nil。
func (q *MessageQueue) Get(seq uint32) chan *Frame {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending[seq]
}

// Put 注册一个等待 ACK 的请求。对标 uni-protocol MessageQueue.put(sequence, future)。
// 当响应到达时，由 Dispatcher 调用 Complete 完成 channel。
// 若队列已达容量上限则返回错误。
func (q *MessageQueue) Put(seq uint32, ch chan *Frame, capacity int) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if capacity > 0 && q.count >= capacity {
		return fmt.Errorf("protoq: ACK queue full: count=%d capacity=%d", q.count, capacity)
	}

	q.pending[seq] = ch
	q.count++
	return nil
}

// Complete 完成指定 sequence 的等待：向 channel 发送响应帧并从队列中移除。
// 对标 uni-protocol MessageQueue 中 Future.complete() 的 Go channel 等价操作。
func (q *MessageQueue) Complete(seq uint32, frame *Frame) {
	q.mu.Lock()
	ch, ok := q.pending[seq]
	if ok {
		delete(q.pending, seq)
		q.count--
	}
	q.mu.Unlock()

	if ok && ch != nil {
		ch <- frame
	}
}

// CompleteError 完成指定 sequence 的等待：关闭 channel（表示错误）。
func (q *MessageQueue) CompleteError(seq uint32) {
	q.mu.Lock()
	ch, ok := q.pending[seq]
	if ok {
		delete(q.pending, seq)
		q.count--
	}
	q.mu.Unlock()

	if ok && ch != nil {
		close(ch)
	}
}

// Clear 清空队列，关闭所有等待 channel。
func (q *MessageQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()

	for seq, ch := range q.pending {
		close(ch)
		delete(q.pending, seq)
	}
	q.count = 0
}
