package protoq

// Flags 是 ProtoQ 协议帧的 1 字节标志位。
// 位布局（从高位到低位）：
//
//	bit7   DIR      0=请求, 1=响应
//	bit6   ACK_REQ  1=需要应答
//	bit5   HAS_LEN  1=有 Length 字段（变体 A），0=无 Length 字段（变体 B）
//	bit4-3 OP_LEN   00=0字节, 01=2字节, 10=4字节
//	bit2-1 SEQ_LEN  00=0字节, 01=2字节, 10=4字节
//	bit0   CRC_LEN  0=无CRC,  1=2字节CRC-16-IBM
//
// 注：由于 Flags 仅 1 字节，CRC_LEN 仅占 1 位，因此当前只支持 0 或 2 字节 CRC。
// 4 字节 CRC-32 预留未来协议版本扩展。
type Flags byte

// 标志位掩码和常量
const (
	FlagDIR    Flags = 1 << 7 // 0x80 方向位
	FlagACKREQ Flags = 1 << 6 // 0x40 需要应答
	FlagHASLEN Flags = 1 << 5 // 0x20 有长度字段

	// OP_LEN 占 bit4-3
	FlagOPLENMask Flags = 0x18 // 0b00011000
	FlagOPLEN0    Flags = 0x00 // 0 字节 Opcode
	FlagOPLEN2    Flags = 0x08 // 2 字节 Opcode
	FlagOPLEN4    Flags = 0x10 // 4 字节 Opcode

	// SEQ_LEN 占 bit2-1
	FlagSEQLENMask Flags = 0x06 // 0b00000110
	FlagSEQLEN0    Flags = 0x00 // 0 字节 Seq
	FlagSEQLEN2    Flags = 0x02 // 2 字节 Seq
	FlagSEQLEN4    Flags = 0x04 // 4 字节 Seq

	// CRC_LEN 占 bit0
	FlagCRCLENMask Flags = 0x01 // 0b00000001
	FlagCRCLEN0    Flags = 0x00 // 无 CRC
	FlagCRCLEN2    Flags = 0x01 // 2 字节 CRC-16-IBM
)

// 协议常量
const (
	MagicByte  byte = 0x51 // 'Q'
	MaxBodyLen      = 65535 - 8 // Length 字段 2 字节可表示的最大值减去 Opcode+Seq+CRC 开销

	DefaultOpcodeLen = 2 // 默认 Opcode 2 字节
	DefaultSeqLen    = 2 // 默认 Seq 2 字节
)

// 长度编码辅助常量
const (
	Len0 = 0
	Len2 = 2
	Len4 = 4
)

// IsRequest 返回是否为请求帧（DIR=0）。
func (f Flags) IsRequest() bool { return f&FlagDIR == 0 }

// IsResponse 返回是否为响应帧（DIR=1）。
func (f Flags) IsResponse() bool { return f&FlagDIR != 0 }

// AckReq 返回是否需要应答。
func (f Flags) AckReq() bool { return f&FlagACKREQ != 0 }

// HasLength 返回是否有 Length 字段。
func (f Flags) HasLength() bool { return f&FlagHASLEN != 0 }

// OpcodeLen 返回 Opcode 字段的字节数（0、2 或 4）。
func (f Flags) OpcodeLen() int {
	switch f & FlagOPLENMask {
	case FlagOPLEN2:
		return 2
	case FlagOPLEN4:
		return 4
	default:
		return 0
	}
}

// SeqLen 返回 Seq 字段的字节数（0、2 或 4）。
func (f Flags) SeqLen() int {
	switch f & FlagSEQLENMask {
	case FlagSEQLEN2:
		return 2
	case FlagSEQLEN4:
		return 4
	default:
		return 0
	}
}

// CRCLen 返回 CRC 字段的字节数（0 或 2）。
func (f Flags) CRCLen() int {
	if f&FlagCRCLENMask != 0 {
		return 2
	}
	return 0
}

// SetDir 设置方向位。
func (f Flags) SetDir(resp bool) Flags {
	if resp {
		return f | FlagDIR
	}
	return f &^ FlagDIR
}

// SetAckReq 设置应答请求位。
func (f Flags) SetAckReq(v bool) Flags {
	if v {
		return f | FlagACKREQ
	}
	return f &^ FlagACKREQ
}

// SetHasLen 设置长度字段存在位。
func (f Flags) SetHasLen(v bool) Flags {
	if v {
		return f | FlagHASLEN
	}
	return f &^ FlagHASLEN
}

// SetOpcodeLen 设置 Opcode 字段长度（0、2 或 4）。
func (f Flags) SetOpcodeLen(n int) Flags {
	f &^= FlagOPLENMask
	switch n {
	case 2:
		return f | FlagOPLEN2
	case 4:
		return f | FlagOPLEN4
	default:
		return f | FlagOPLEN0
	}
}

// SetSeqLen 设置 Seq 字段长度（0、2 或 4）。
func (f Flags) SetSeqLen(n int) Flags {
	f &^= FlagSEQLENMask
	switch n {
	case 2:
		return f | FlagSEQLEN2
	case 4:
		return f | FlagSEQLEN4
	default:
		return f | FlagSEQLEN0
	}
}

// SetCRCLen 设置 CRC 字段长度（0 或 2）。
func (f Flags) SetCRCLen(n int) Flags {
	f &^= FlagCRCLENMask
	if n > 0 {
		return f | FlagCRCLEN2
	}
	return f | FlagCRCLEN0
}

// Validate 验证标志位的合法性。
// 约束：
//   - ACK_REQ=1 时 SEQ_LEN 不能为 0
//   - 响应包（DIR=1）中 ACK_REQ 必须为 0
//   - HAS_LEN=0 时不能有 Body
func (f Flags) Validate(hasBody bool) error {
	if f.AckReq() && f.SeqLen() == 0 {
		return ErrACKReqNeedsSeq
	}
	if f.IsResponse() && f.AckReq() {
		return ErrResponseCantAckReq
	}
	if !f.HasLength() && hasBody {
		return ErrBodyWithoutLength
	}
	return nil
}

// EncodeOpcodeLen 将长度值编码为标志位。
func EncodeOpcodeLen(n int) Flags {
	switch n {
	case 2:
		return FlagOPLEN2
	case 4:
		return FlagOPLEN4
	default:
		return FlagOPLEN0
	}
}

// EncodeSeqLen 将长度值编码为标志位。
func EncodeSeqLen(n int) Flags {
	switch n {
	case 2:
		return FlagSEQLEN2
	case 4:
		return FlagSEQLEN4
	default:
		return FlagSEQLEN0
	}
}

// EncodeCRCLen 将 CRC 长度编码为标志位。
func EncodeCRCLen(n int) Flags {
	if n > 0 {
		return FlagCRCLEN2
	}
	return FlagCRCLEN0
}
