// Package protoq — 帧标志位类型与方法
//
// Flags 是 ProtoQ 协议帧的 1 字节标志位字段。
// 位布局（从高位到低位）：
//
//	bit7   DIR       0=请求, 1=响应
//	bit6   ACK_REQ   1=需要应答
//	bit5   HAS_LEN   1=有 Length 字段（变体 A），0=无（变体 B）
//	bit4-3 OP_LEN    00=0字节, 01=2字节, 10=4字节
//	bit2-1 SEQ_LEN   00=0字节, 01=2字节, 10=4字节
//	bit0   CRC_LEN   0=无CRC, 1=2字节CRC-16-IBM
//
// 注：由于 Flags 仅 1 字节，CRC_LEN 仅占 1 位，因此当前只支持 0 或 2 字节 CRC。
// 4 字节 CRC-32 预留未来协议版本扩展。
//
// 标志位常量定义见 constants.go。
package protoq

// Flags 是 ProtoQ 协议帧的 1 字节标志位。
type Flags byte

// IsRequest 返回是否为请求帧（DIR=0）。
func (f Flags) IsRequest() bool { return f&FlagDIR == 0 }

// IsResponse 返回是否为响应帧（DIR=1）。
func (f Flags) IsResponse() bool { return f&FlagDIR != 0 }

// RequiresAck 返回是否需要应答（ACK_REQ=1）。
func (f Flags) RequiresAck() bool { return f&FlagRequiresAck != 0 }

// HasLength 返回是否有 Length 字段（HAS_LEN=1）。
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

// SetRequiresAck 设置应答请求位。
func (f Flags) SetRequiresAck(v bool) Flags {
	if v {
		return f | FlagRequiresAck
	}
	return f &^ FlagRequiresAck
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
//
// 约束：
//   - ACK_REQ=1 时 SEQ_LEN 不能为 0（需要序列号来匹配响应）
//   - 响应包（DIR=1）中 ACK_REQ 必须为 0（响应不能要求再次应答）
//   - HAS_LEN=0 时不能有 Body（变体 B 必须无载荷）
func (f Flags) Validate(hasBody bool) error {
	if f.RequiresAck() && f.SeqLen() == 0 {
		return ErrRequiresAckNoSeq
	}
	if f.IsResponse() && f.RequiresAck() {
		return ErrResponseRequiresAck
	}
	if !f.HasLength() && hasBody {
		return ErrBodyWithoutLength
	}
	return nil
}

// EncodeOpcodeLen 将长度值编码为标志位 Opcode 长度字段。
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

// EncodeSeqLen 将长度值编码为标志位 Seq 长度字段。
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

// EncodeCRCLen 将 CRC 长度编码为标志位 CRC 长度字段。
func EncodeCRCLen(n int) Flags {
	if n > 0 {
		return FlagCRCLEN2
	}
	return FlagCRCLEN0
}
