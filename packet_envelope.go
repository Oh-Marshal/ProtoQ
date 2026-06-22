// Package protoq — 出站消息信封
//
// 对标 Java uni-protocol 的 PacketEnvelope。
// 携带原始消息与用于 ACK 的 callback channel。
package protoq

// PacketEnvelope 出站消息信封。对标 uni-protocol org.facelang.unified.proto.model.PacketEnvelope。
//
// 由 Connection.Send() 传入 pipeline，Bridge 在 write 中根据 PacketData.RequireAck()
// 决定是否登记 pending 并写出。
type PacketEnvelope struct {
	// Message 原始消息（*PacketData 或业务负载对象）。
	Message interface{}

	// Callback 需要 ACK 时的响应 channel；响应到达后 close。
	// 不需要 ACK 时可为 nil。
	Callback chan *PacketData
}
