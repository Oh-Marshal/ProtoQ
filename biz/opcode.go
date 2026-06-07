package biz

// Opcode 业务操作码常量定义。
//
// Opcode 范围规划：
//
//	0x0001 - 0x00FF   系统级（Echo、健康检查、状态等）
//	0x0100 - 0x0FFF   通用业务
//	0x1000 - 0xFFFF   应用自定义
const (
	// 系统级 Opcode
	OpcodeEcho       uint32 = 0x0001 // Echo 回显
	OpcodeHealth     uint32 = 0x0002 // 健康检查
	OpcodeServerInfo uint32 = 0x0003 // 服务端信息

	// 通用业务 Opcode 预留
	OpcodeAuth  uint32 = 0x0100 // 认证
	OpcodeHeartbeat uint32 = 0x0101 // 心跳
)
