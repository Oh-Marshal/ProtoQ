// Package biz — 协商消息处理器
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.message.handler.NegotiatePayloadHandler。
// NegotiatePayloadHandler 处理 messageId=0x01 的协商请求，验证协商参数，
// 调用 Negotiator 执行协商逻辑，并在协商成功后设置 CODEC_TYPE 属性和 session_id。
package biz

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/oh-marshal/protoq"
)

// NegotiatePayloadHandler 协商消息处理器。
//
// 对标 uni-protocol NegotiatePayloadHandler（@MessageSubscriber + @MessageHandler(command=0x01)）。
// 作为 protoq.MessageHandler 函数（签名 func(ctx protoq.Context) ([]byte, error)），
// 注册到 MessageDispatcher.RegisterHandler(OpcodeNegotiate, handler)。
//
// 处理流程：
//  1. 校验 Body 非空
//  2. 反序列化 NegotiateRequest（JSON）
//  3. 调用 Negotiator.Negotiate(req) 获取协商结果
//  4. 若 accepted：
//     - 设置 CODEC_TYPE 属性（标记协商完成）
//     - 设置 session_id 元数据（通过 Connection.SetProperty）
//     - 返回序列化后的 NegotiateResponse
//  5. 若 rejected：
//     - 返回序列化后的拒绝响应 + ErrNegotiateFailed
type NegotiatePayloadHandler struct {
	// Negotiator 协商策略（可插拔），nil 时使用 DefaultNegotiator
	Negotiator Negotiator

	// sessionSeq 会话序列号生成器（原子自增）
	sessionSeq atomic.Uint64
}

// NewNegotiatePayloadHandler 创建协商消息处理器。
//
// negotiator: 协商策略实现，nil 时使用 DefaultNegotiator。
func NewNegotiatePayloadHandler(negotiator Negotiator) *NegotiatePayloadHandler {
	if negotiator == nil {
		negotiator = &DefaultNegotiator{}
	}
	return &NegotiatePayloadHandler{
		Negotiator: negotiator,
	}
}

// Handle 实现 protoq.MessageHandler 函数签名。
//
// 对标 uni-protocol NegotiatePayloadHandler.handle(ctx, payload)。
// 处理协商请求：JSON 解码请求体 → 协商 → 设置连接属性 → 返回响应。
//
// 返回值：
//   - []byte: 序列化后的 NegotiateResponse JSON
//   - error: 协商失败时返回 ErrNegotiateFailed
func (h *NegotiatePayloadHandler) Handle(ctx protoq.Context) ([]byte, error) {
	frame := ctx.Frame()
	if frame == nil {
		return nil, fmt.Errorf("biz: 协商请求帧为空")
	}

	// 1. 校验 Body 非空
	if len(frame.Body) == 0 {
		return h.marshalReject("协商请求体为空"), ErrNegotiateFailed
	}

	// 2. 反序列化请求
	req, err := UnmarshalNegotiateRequest(frame.Body)
	if err != nil {
		return h.marshalReject("无效的协商请求: " + err.Error()), ErrNegotiateFailed
	}

	// 3. 执行协商
	neg := h.Negotiator
	if neg == nil {
		neg = &DefaultNegotiator{}
	}
	resp := neg.Negotiate(req)

	// 4. 若接受，分配 session_id 并设置连接属性
	conn := ctx.Connection()
	if resp.Accepted {
		if resp.SessionID == "" {
			resp.SessionID = fmt.Sprintf("sess-%d-%d", conn.ConnectionID(), h.sessionSeq.Add(1))
		}
		resp.ServerTime = time.Now().Unix()

		// 设置 CODEC_TYPE 属性（标记协商完成，对标 uni-protocol）
		// 使用客户端的加密方案请求值（req.Encryption）作为 CODEC_TYPE
		conn.SetProperty(ConnectionKeyCODEC_TYPE, req.Encryption)

		// 设置 session_id 元数据
		conn.SetProperty("session_id", resp.SessionID)
	}

	// 5. 序列化响应
	respBody, err := MarshalNegotiateResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("biz: 序列化协商响应失败: %w", err)
	}

	// 6. 设置响应到上下文（由 Bridge 写回）
	ctx.SetResponse(respBody)

	if !resp.Accepted {
		return respBody, ErrNegotiateFailed
	}

	return respBody, nil
}

// marshalReject 构造拒绝响应体（便捷方法）。
func (h *NegotiatePayloadHandler) marshalReject(reason string) []byte {
	resp := &NegotiateResponse{
		Accepted:      false,
		ServerVersion: ProtoVersion,
		Reason:        reason,
	}
	body, _ := MarshalNegotiateResponse(resp)
	return body
}
