// Package biz — 内容协商过滤器
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.filter.NegotiateFilter。
// NegotiateFilter 在消息分发到业务 Handler 之前检查连接是否已完成协商。
// 未协商的连接仅允许发送协商请求（messageId=0x01），其他消息直接返回错误。
//
// 协商状态通过 Connection.SetProperty(CODEC_TYPE, ...) 存储，
// 对标 uni-protocol 的 ConnectionKey.CODEC_TYPE 属性机制。
package biz

import (
	"fmt"

	api "github.com/oh-marshal/protoq"
)

// ConnectionKeyCODEC_TYPE 连接属性键：编解码/加密类型。
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.constant.ConnectionKey.CODEC_TYPE。
// 协商完成后由 NegotiatePayloadHandler 写入，NegotiateFilter 读取以判断协商状态。
const ConnectionKeyCODEC_TYPE = "prop.codec.type"

// NegotiateFilter 内容协商过滤器，实现 api.Filter 接口。
//
// 对标 uni-protocol NegotiateFilter：
//   - 协商前仅允许 messageId=0x01（协商请求）通过
//   - 协商完成后（CODEC_TYPE 属性已设置），所有消息正常通过
//   - 未协商时发送非 0x01 消息 → 返回 ErrNegotiateRequired
//
// 设计要点：
//   - 不调用 chain.DoFilter() 即可阻断消息
//   - 协商状态通过 Connection 的 Property 机制存储
type NegotiateFilter struct{}

// DoFilter 实现 api.Filter 接口。
//
// 对标 uni-protocol NegotiateFilter.doFilter(context, chain)。
//
// 过滤逻辑：
//  1. frame 为 nil → 直接放行（边缘情况）
//  2. messageId == 0x01（协商请求）→ 直接放行
//  3. 检查 CODEC_TYPE 属性：已设置 → 放行，未设置 → 返回错误
func (f *NegotiateFilter) DoFilter(ctx api.Context, chain api.FilterChain) error {
	frame := ctx.Frame()
	if frame == nil {
		// 无帧数据，直接放行
		return chain.DoFilter(ctx)
	}

	// 提取 messageId（Opcode 低字节，对标 uni-protocol 的 1 字节 messageId）
	messageID := frame.Opcode & 0xFF

	// 协商请求 (messageId=0x01) 总是放行
	if messageID == 0x01 {
		return chain.DoFilter(ctx)
	}

	// 非协商请求：检查是否已完成协商
	conn := ctx.Connection()
	if conn != nil {
		codecType, hasCodec := conn.GetProperty(ConnectionKeyCODEC_TYPE)
		if !hasCodec || codecType == nil {
			return fmt.Errorf("%w: messageId=0x%02X 在协商完成前到达", ErrNegotiateRequired, messageID)
		}
	}

	return chain.DoFilter(ctx)
}
