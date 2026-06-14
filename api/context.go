// Package protoq — 请求上下文
//
// 对标 Java uni-protocol 的 Context 接口：
// 承载"连接 + 当前帧报文 + 响应载体"，在 Pipeline 中传递。
// 与 Connection 接口职责分离：Context 关注单次请求，Connection 关注连接。
package api

// Context 请求上下文接口。对标 uni-protocol org.facelang.unified.proto.api.Context。
//
// 在解码 → 过滤链 → 业务分发的 Pipeline 中传递。
// 不提供发送能力（发送由 Connection 负责），保持上下文单一职责。
type Context interface {
	// Connection 返回当前请求所属的连接。
	Connection() Connection

	// Frame 返回当前请求的解码后帧报文。
	Frame() *Frame

	// Response 获取待回写的响应对象。
	// 由业务 Handler 通过 context.SetResponse() 设置。
	Response() interface{}

	// SetResponse 设置待回写的响应对象。
	// 类型可以是：nil（无响应）、*Frame（直接帧）、[]byte（二进制体）、
	// 或任意业务对象（由 Converter 序列化后作为响应体）。
	SetResponse(data interface{})
}
