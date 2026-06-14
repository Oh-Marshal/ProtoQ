package protoq

import (
	"encoding/binary"
)

// Encode 将 Frame 编码为网络字节序的完整帧（含 Magic、Flags、Padding）。
// 返回的字节切片包含完整的可发送帧。
func Encode(f *Frame) ([]byte, error) {
	if err := f.Flags.Validate(len(f.Body) > 0); err != nil {
		return nil, err
	}

	opcodeLen := f.Flags.OpcodeLen()
	seqLen := f.Flags.SeqLen()
	crcLen := f.Flags.CRCLen()

	// 计算各段大小
	// headerBeforeCRC = Magic(1) + Flags(1) + [Length(2)] + Opcode + Seq + Body
	var lengthField int
	if f.Flags.HasBodyLen() {
		lengthField = 2
	}
	headerBeforeCRC := 1 + 1 + lengthField + opcodeLen + seqLen + len(f.Body)

	// 四字节对齐填充
	// 总大小 = headerBeforeCRC + crcLen, 对齐到 4 字节
	totalSize := headerBeforeCRC + crcLen
	padLen := (4 - totalSize%4) % 4

	buf := make([]byte, totalSize+padLen)
	offset := 0

	// Magic
	buf[offset] = MagicByte
	offset++

	// Flags
	buf[offset] = byte(f.Flags)
	offset++

	// Length (变体 A)
	if f.Flags.HasBodyLen() {
		// Length = Opcode + Seq + Body + CRC 的长度
		payloadLen := opcodeLen + seqLen + len(f.Body) + crcLen
		binary.BigEndian.PutUint16(buf[offset:], uint16(payloadLen))
		offset += 2
	}

	// Opcode
	putUintN(buf[offset:], f.Opcode, opcodeLen)
	offset += opcodeLen

	// Seq
	putUintN(buf[offset:], f.Seq, seqLen)
	offset += seqLen

	// Body
	copy(buf[offset:], f.Body)
	offset += len(f.Body)

	// 记录 CRC 覆盖范围结束位置
	crcDataStart := 1 // 从 Flags 开始（跳过 Magic）
	crcDataEnd := offset

	// CRC (覆盖 Flags + Length + Opcode + Seq + Body, 不含 Magic)
	if crcLen > 0 {
		crcData := buf[crcDataStart:crcDataEnd]
		crcBytes := ComputeCRC(crcData, crcLen)
		copy(buf[offset:], crcBytes)
		offset += crcLen
	}

	// Padding (清零)
	// 已经在 make 时零初始化，无需额外操作
	_ = padLen

	return buf, nil
}

// EncodeTo 将 Frame 编码后写入 Writer（如 net.Conn 或 bytes.Buffer）。
// 返回写入的字节数和可能的错误。
func EncodeTo(f *Frame, w interface{ Write([]byte) (int, error) }) (int, error) {
	data, err := Encode(f)
	if err != nil {
		return 0, err
	}
	return w.Write(data)
}

// MustEncode 编码帧，失败时 panic（仅用于测试）。
func MustEncode(f *Frame) []byte {
	data, err := Encode(f)
	if err != nil {
		panic(err)
	}
	return data
}
