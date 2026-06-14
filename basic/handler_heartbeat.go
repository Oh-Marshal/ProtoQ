// Package biz — 心跳消息处理器
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.message.handler.HeartbeatPayloadHandler。
// HeartbeatPayloadHandler 处理 messageId=0x03 (PING) 的心跳请求，
// 更新连接的最后心跳时间戳，返回空响应体（nil body）。
//
// uni-protocol 中 HEARTBEAT = 0x03, PONG = 0x04。
// ProtoQ 中 OpcodeHeartbeat = 0x0002（低字节 0x02）。
// 对标还原使用 ProtoQ 的操作码常量。
package basic

import (
	"time"

	api "github.com/oh-marshal/protoq/api"
)

// HeartbeatPayloadHandler 心跳消息处理器。
//
// 对标 uni-protocol HeartbeatPayloadHandler（@MessageSubscriber + @MessageHandler(command=0x03)）。
// 作为 api.MessageHandler 函数（签名 func(ctx api.Context) ([]byte, error)），
// 注册到 MessageDispatcher.RegisterHandler(OpcodeHeartbeat, handler)。
//
// 处理逻辑：
//  1. 更新连接属性 "last_heartbeat" 为当前时间戳
//  2. 返回 nil body（PONG 响应的 Body 为空）
//
// 心跳设计（对标 uni-protocol）：
//   - PING (0x03): 客户端发送，无 Body
//   - PONG (0x04): 服务端响应，无 Body
//   - 服务端收到 PING 后仅更新心跳时间戳，由心跳监控器检测超时
type HeartbeatPayloadHandler struct{}

// NewHeartbeatPayloadHandler 创建心跳消息处理器。
func NewHeartbeatPayloadHandler() *HeartbeatPayloadHandler {
	return &HeartbeatPayloadHandler{}
}

// Handle 实现 api.MessageHandler 函数签名。
//
// 对标 uni-protocol HeartbeatPayloadHandler.handle(context)。
// 处理心跳 PING：更新最后心跳时间 → 返回 nil（空响应体）。
//
// 返回值：
//   - []byte: nil（PONG 无 Body，对标 uni-protocol EmptyPayload）
//   - error: 始终返回 nil（心跳处理不应失败）
func (h *HeartbeatPayloadHandler) Handle(ctx api.Context) ([]byte, error) {
	conn := ctx.Connection()
	if conn == nil {
		return nil, nil
	}

	// 更新最后心跳时间戳
	conn.SetProperty("last_heartbeat", time.Now())

	// 心跳响应无 Body（对标 uni-protocol EmptyPayload.INSTANCE）
	// ctx.SetResponse(nil) 表示空响应体，Bridge 将构建空 Body 的响应帧
	return nil, nil
}
