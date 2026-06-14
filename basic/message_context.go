// Package biz — 消息上下文实现
//
// 对标 Java uni-protocol org.facelang.unified.proto.basic.register.context.MessageContext。
// MessageContext 实现 api.Context 接口，承载单次请求的"连接 + 当前帧报文 + 响应载体"。
// 在解码 → 过滤链 → 业务分发的 Pipeline 中传递，不提供发送能力（发送由 Connection 负责）。
package basic

import api "github.com/oh-marshal/protoq/api"

// MessageContext 是 api.Context 接口的具体实现。
//
// 对标 uni-protocol MessageContext：轻量级上下文，仅承载：
//   - connection: 本帧所属的连接
//   - frame: 当前请求的解码后协议帧
//   - response: 待回写的响应对象（由业务 Handler 通过 SetResponse 设置）
//
// 设计要点：
//   - 与 Connection 职责分离：Context 关注单次请求，Connection 关注连接生命周期
//   - 不提供发送能力，保持上下文单一职责
//   - response 字段为 volatile 等效（Go 中通过指针传递，调用方持有引用）
type MessageContext struct {
	conn     api.Connection
	frame    *api.Frame
	response interface{}
}

// NewMessageContext 创建新的消息上下文。
//
// conn: 本次请求所属的连接
// frame: 解码后的请求帧
func NewMessageContext(conn api.Connection, frame *api.Frame) *MessageContext {
	return &MessageContext{
		conn:  conn,
		frame: frame,
	}
}

// Connection 返回当前请求所属的连接。
//
// 对标 uni-protocol Context.getConnection()。
func (c *MessageContext) Connection() api.Connection {
	return c.conn
}

// Frame 返回当前请求的解码后协议帧。
//
// 对标 uni-protocol Context.getPacketData()。
// Go 中 PacketData 对应 api.Frame 结构体。
func (c *MessageContext) Frame() *api.Frame {
	return c.frame
}

// Response 获取待回写的响应对象。
//
// 对标 uni-protocol Context.getResponse()。
// 由业务 Handler 通过 SetResponse 设置。
// 返回值可以是：nil（无响应）、*Frame（直接帧）、[]byte（二进制体）、
// 或任意业务对象（由 Converter 序列化后作为响应体）。
func (c *MessageContext) Response() interface{} {
	return c.response
}

// SetResponse 设置待回写的响应对象。
//
// 对标 uni-protocol Context.setResponse(data)。
//
// data 类型约定：
//   - nil: 无响应（框架不写回任何数据）
//   - *Frame: 直接作为响应帧写出
//   - []byte: 作为响应体（由 Bridge 封装为响应帧）
//   - 其他: 由 Converter.Write 序列化后作为响应体
func (c *MessageContext) SetResponse(data interface{}) {
	c.response = data
}
