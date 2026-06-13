package protoq

import (
	"encoding/binary"
	"fmt"
	"io"
)

// 解码器状态
const (
	stateMagic   = iota // 等待 Magic 字节
	stateFlags          // 等待 Flags 字节
	stateLength         // 等待 Length 字段（2 字节）
	stateOpcode         // 等待 Opcode 字段
	stateSeq            // 等待 Seq 字段
	stateBody           // 等待 Body
	stateCRC            // 等待 CRC 字段
	statePadding        // 等待 Padding 字节
)

// Decoder 是基于 TCP 流的 ProtoQ 帧解码器。
// 使用状态机处理粘包和半包问题，从 io.Reader 中持续读取并解析完整帧。
type Decoder struct {
	reader io.Reader

	// 内部缓冲区，积累从 reader 读取的原始字节
	buf    []byte
	bufPos int // 已消费位置

	// 当前帧解析状态
	state int

	// 当前帧的元数据
	curFlags      Flags
	curOpcodeLen  int
	curSeqLen     int
	curCRCLen     int
	curHasLen     bool
	curLengthVal  uint16 // Length 字段值
	curBodyLen    int    // 计算出的 Body 长度
	curCRCOffset  int    // CRC 在帧中的起始偏移（相对于 buf 起始）
	curTotalLen   int    // 帧总长度（含 Magic、Padding）
	curNeedBytes  int    // 当前状态还需要读取的字节数
	curFrameStart int    // 当前帧在 buf 中的起始位置

	// 统计
	framesDecoded uint64
	bytesConsumed uint64
}

// NewDecoder 创建一个新的流式解码器。
func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{
		reader: r,
		state:  stateMagic,
	}
}

// Decode 从流中读取并解码一个完整的 ProtoQ 帧。
// 在遇到 io.EOF 且缓冲区无完整帧时返回 io.EOF。
// 其他 I/O 错误直接返回。
func (d *Decoder) Decode() (*Frame, error) {
	for {
		switch d.state {
		case stateMagic:
			if err := d.ensureBytes(1); err != nil {
				return nil, err
			}
			if d.buf[d.bufPos] != MagicByte {
				// 不是 Magic 字节，跳过并重试（用于从流中同步）
				d.bufPos++
				d.bytesConsumed++
				continue
			}
			d.curFrameStart = d.bufPos
			d.bufPos++
			d.bytesConsumed++
			d.state = stateFlags

		case stateFlags:
			if err := d.ensureBytes(1); err != nil {
				return nil, err
			}
			// 需要重新计算，因为可能 shift 了
			flagsPos := d.curFrameStart + 1
			if flagsPos >= len(d.buf) {
				if err := d.ensureBytes(1); err != nil {
					return nil, err
				}
			}
			d.curFlags = Flags(d.buf[flagsPos])
			d.bufPos = flagsPos + 1
			d.bytesConsumed += uint64(d.bufPos - flagsPos)

			d.curHasLen = d.curFlags.HasLength()
			d.curOpcodeLen = d.curFlags.OpcodeLen()
			d.curSeqLen = d.curFlags.SeqLen()
			d.curCRCLen = d.curFlags.CRCLen()

			if d.curHasLen {
				d.state = stateLength
			} else {
				// 变体 B：无 Length，直接进入 Opcode
				d.state = stateOpcode
			}

		case stateLength:
			if err := d.ensureBytes(2); err != nil {
				return nil, err
			}
			// Length 从 curFrameStart + 2 开始（Magic + Flags）
			lenStart := d.curFrameStart + 2
			d.curLengthVal = binary.BigEndian.Uint16(d.buf[lenStart:])
			d.bufPos = lenStart + 2
			d.bytesConsumed += 2

			// 验证 Length >= OpcodeLen + SeqLen + CRCLen
			minPayload := d.curOpcodeLen + d.curSeqLen + d.curCRCLen
			if int(d.curLengthVal) < minPayload {
				// 无效长度，跳过 Magic 重新同步
				d.resetFrame()
				continue
			}
			d.curBodyLen = int(d.curLengthVal) - minPayload
			d.state = stateOpcode

		case stateOpcode:
			if err := d.ensureBytes(d.curOpcodeLen); err != nil {
				return nil, err
			}
			d.bufPos += d.curOpcodeLen
			d.bytesConsumed += uint64(d.curOpcodeLen)
			d.state = stateSeq

		case stateSeq:
			if err := d.ensureBytes(d.curSeqLen); err != nil {
				return nil, err
			}
			d.bufPos += d.curSeqLen
			d.bytesConsumed += uint64(d.curSeqLen)
			if d.curBodyLen > 0 {
				d.state = stateBody
			} else {
				d.state = stateCRC
			}

		case stateBody:
			if err := d.ensureBytes(d.curBodyLen); err != nil {
				return nil, err
			}
			d.bufPos += d.curBodyLen
			d.bytesConsumed += uint64(d.curBodyLen)
			d.state = stateCRC

		case stateCRC:
			if err := d.ensureBytes(d.curCRCLen); err != nil {
				return nil, err
			}
			d.bufPos += d.curCRCLen
			d.bytesConsumed += uint64(d.curCRCLen)
			d.state = statePadding

		case statePadding:
			// 计算需要的 Padding 字节数
			// 帧总大小 = curFrameStart 到当前位置
			frameLen := d.bufPos - d.curFrameStart
			padLen := (4 - frameLen%4) % 4
			if padLen > 0 {
				if err := d.ensureBytes(padLen); err != nil {
					return nil, err
				}
				d.bufPos += padLen
				d.bytesConsumed += uint64(padLen)
			}

			// 帧完整，构建 Frame 对象
			frame, err := d.buildFrame()
			if err != nil {
				d.resetFrame()
				continue
			}

			// 清理已消费的缓冲区
			d.compact()
			d.resetFrame()
			d.framesDecoded++
			return frame, nil
		}
	}
}

// ensureBytes 确保缓冲区中有至少 n 个未消费字节。
func (d *Decoder) ensureBytes(n int) error {
	for len(d.buf)-d.bufPos < n {
		// 确保缓冲区有空间读取更多数据
		if cap(d.buf)-len(d.buf) < readBufferSize {
			newCap := cap(d.buf) * 2
			if newCap < readBufferSize {
				newCap = readBufferSize
			}
			newBuf := make([]byte, len(d.buf), newCap)
			copy(newBuf, d.buf)
			d.buf = newBuf
		}
		// 扩展到容量边界以允许 Read 写入
		oldLen := len(d.buf)
		d.buf = d.buf[:cap(d.buf)]
		nr, err := d.reader.Read(d.buf[oldLen:])
		d.buf = d.buf[:oldLen+nr]
		if err != nil {
			if err == io.EOF {
				if len(d.buf)-d.bufPos < n {
					return io.EOF
				}
				return nil
			}
			return fmt.Errorf("protoq decoder read: %w", err)
		}
	}
	return nil
}

// compact 清理已消费的字节，释放缓冲区空间。
func (d *Decoder) compact() {
	if d.bufPos > 0 && d.bufPos < len(d.buf) {
		copy(d.buf, d.buf[d.bufPos:])
	}
	d.buf = d.buf[:len(d.buf)-d.bufPos]
	d.curFrameStart -= d.bufPos
	if d.curFrameStart < 0 {
		d.curFrameStart = 0
	}
	d.bufPos = 0
}

// buildFrame 从缓冲区构建 Frame 对象。
func (d *Decoder) buildFrame() (*Frame, error) {
	pos := d.curFrameStart

	// Magic (已跳过)
	pos++

	// Flags (已读取)
	pos++

	// Length (如果有)
	if d.curHasLen {
		pos += 2
	}

	// Opcode
	opcode := readUintN(d.buf[pos:], d.curOpcodeLen)
	pos += d.curOpcodeLen

	// Seq
	seq := readUintN(d.buf[pos:], d.curSeqLen)
	pos += d.curSeqLen

	// Body
	var body []byte
	if d.curBodyLen > 0 {
		body = make([]byte, d.curBodyLen)
		copy(body, d.buf[pos:pos+d.curBodyLen])
		pos += d.curBodyLen
	}

	// CRC 验证
	if d.curCRCLen > 0 {
		crcStart := d.curFrameStart + 1 // 从 Flags 开始
		crcEnd := pos
		expectedCRC := d.buf[pos : pos+d.curCRCLen]
		crcData := d.buf[crcStart:crcEnd]
		if !VerifyCRC(crcData, expectedCRC, d.curCRCLen) {
			return nil, ErrCRCMismatch
		}
		pos += d.curCRCLen
	}

	// 验证标志位
	if err := d.curFlags.Validate(len(body) > 0); err != nil {
		return nil, err
	}

	return &Frame{
		Flags:  d.curFlags,
		Opcode: opcode,
		Seq:    seq,
		Body:   body,
	}, nil
}

// resetFrame 重置帧解析状态，准备解析下一帧。
func (d *Decoder) resetFrame() {
	d.state = stateMagic
	d.curFlags = 0
	d.curOpcodeLen = 0
	d.curSeqLen = 0
	d.curCRCLen = 0
	d.curHasLen = false
	d.curLengthVal = 0
	d.curBodyLen = 0
	d.curNeedBytes = 0
	// curFrameStart 在下次找到 Magic 时设置
}

// FramesDecoded 返回已解码的帧数。
func (d *Decoder) FramesDecoded() uint64 {
	return d.framesDecoded
}

// BytesConsumed 返回已消费的字节数。
func (d *Decoder) BytesConsumed() uint64 {
	return d.bytesConsumed
}
