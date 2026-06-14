package protoq

import (
	"encoding/binary"
	"fmt"
)

// Frame 表示一个 ProtoQ 协议帧。
type Frame struct {
	Flags  Flags
	Opcode uint32
	Seq    uint32
	Body   []byte
}

// NewRequestFrame 创建一个请求帧。
// opcode: 操作码
// seq: 序列号（仅当需要应答时有效）
// body: 消息体
// requiresAck: 是否需要应答
// crc: 是否附加 CRC
func NewRequestFrame(opcode uint32, seq uint32, body []byte, requiresAck bool, crc bool) *Frame {
	f := &Frame{
		Flags:  0,
		Opcode: opcode,
		Seq:    seq,
		Body:   body,
	}
	// 确定字段长度
	opLen := fieldLen(opcode)
	seqLen := fieldLen(seq)

	f.Flags = f.Flags.SetDir(false) // 请求
	f.Flags = f.Flags.SetOpcodeLen(opLen)
	f.Flags = f.Flags.SetBodyLen(len(body) > 0) // 有 Body 时需要 Body 长度字段

	if requiresAck {
		if seqLen == 0 {
			seqLen = 2 // 需要应答时至少 2 字节 Seq
		}
		f.Flags = f.Flags.SetRequiresAck(true)
	}
	f.Flags = f.Flags.SetSeqLen(seqLen)

	if crc {
		f.Flags = f.Flags.SetCRCLen(2) // 2 字节 CRC-16
	}

	return f
}

// NewResponseFrame 创建一个响应帧。
// 响应帧自动设置 DIR=1，ACK_REQ=0，并原样带回请求的 Seq。
func NewResponseFrame(opcode uint32, seq uint32, body []byte, requestFlags Flags) *Frame {
	f := &Frame{
		Flags:  0,
		Opcode: opcode,
		Seq:    seq,
		Body:   body,
	}
	opLen := fieldLen(opcode)
	seqLen := requestFlags.SeqLen() // 使用请求中的 Seq 长度

	f.Flags = f.Flags.SetDir(true)     // 响应
	f.Flags = f.Flags.SetRequiresAck(false) // 响应不能要求应答
	f.Flags = f.Flags.SetOpcodeLen(opLen)
	f.Flags = f.Flags.SetSeqLen(seqLen)
	f.Flags = f.Flags.SetBodyLen(len(body) > 0)

	// 如果请求有 CRC，响应也带 CRC
	if requestFlags.CRCLen() > 0 {
		f.Flags = f.Flags.SetCRCLen(requestFlags.CRCLen())
	}

	return f
}

// NewNotificationFrame 创建一个通知帧（无需应答的单向消息）。
func NewNotificationFrame(opcode uint32, body []byte, crc bool) *Frame {
	f := &Frame{
		Flags:  0,
		Opcode: opcode,
		Seq:    0,
		Body:   body,
	}
	opLen := fieldLen(opcode)

	f.Flags = f.Flags.SetDir(false)
	f.Flags = f.Flags.SetRequiresAck(false)
	f.Flags = f.Flags.SetOpcodeLen(opLen)
	f.Flags = f.Flags.SetSeqLen(0) // 无需序列号
	f.Flags = f.Flags.SetBodyLen(len(body) > 0)

	if crc {
		f.Flags = f.Flags.SetCRCLen(2)
	}

	return f
}

// IsRequest 返回是否为请求帧。
func (f *Frame) IsRequest() bool { return f.Flags.IsRequest() }

// IsResponse 返回是否为响应帧。
func (f *Frame) IsResponse() bool { return f.Flags.IsResponse() }

// RequiresAck 返回是否需要应答。
func (f *Frame) RequiresAck() bool { return f.Flags.RequiresAck() }

// String 返回帧的简要描述。
func (f *Frame) String() string {
	dir := "REQ"
	if f.IsResponse() {
		dir = "RESP"
	}
	ack := ""
	if f.RequiresAck() {
		ack = " ACK"
	}
	return fmt.Sprintf("[%s%s Opcode=%d Seq=%d BodyLen=%d]",
		dir, ack, f.Opcode, f.Seq, len(f.Body))
}

// fieldLen 根据值大小返回合适的字段长度（0、2 或 4 字节）。
func fieldLen(v uint32) int {
	if v == 0 {
		return 0
	}
	if v <= 0xFFFF {
		return 2
	}
	return 4
}

// FrameSize 计算帧在网络上占用的总字节数（含 Magic、Flags、Padding）。
func (f *Frame) FrameSize() int {
	size := 1 + 1 // Magic + Flags
	if f.Flags.HasBodyLen() {
		size += 2 // Length
	}
	size += f.Flags.OpcodeLen()
	size += f.Flags.SeqLen()
	size += len(f.Body)
	size += f.Flags.CRCLen()
	// 四字节对齐填充
	pad := (4 - size%4) % 4
	size += pad
	return size
}

// HeaderSize 返回帧头大小（不含 Body 和 Padding）。
func (f *Frame) HeaderSize() int {
	size := 1 + 1 // Magic + Flags
	if f.Flags.HasBodyLen() {
		size += 2
	}
	size += f.Flags.OpcodeLen()
	size += f.Flags.SeqLen()
	return size
}

// putUintN 以 big-endian 写入 n 字节的无符号整数到 buf。
func putUintN(buf []byte, v uint32, n int) {
	switch n {
	case 2:
		binary.BigEndian.PutUint16(buf, uint16(v))
	case 4:
		binary.BigEndian.PutUint32(buf, v)
	}
}

// readUintN 从 buf 中以 big-endian 读取 n 字节的无符号整数。
func readUintN(buf []byte, n int) uint32 {
	switch n {
	case 2:
		return uint32(binary.BigEndian.Uint16(buf))
	case 4:
		return binary.BigEndian.Uint32(buf)
	default:
		return 0
	}
}
