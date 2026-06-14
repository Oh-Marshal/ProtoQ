// Package protoq — 协议错误定义
//
// 按协议层分类的错误哨兵值（sentinel errors）：
//   - 帧格式错误：Magic、长度、标志位组合等协议层面的格式校验失败
//   - 传输错误：CRC 校验、超时、重传耗尽
//   - 连接错误：连接关闭等运行时状态错误
package protoq

import "errors"

// ─── 帧格式错误 ──────────────────────────────────────────────────────────

var (
	// ErrInvalidMagic 帧起始 Magic 字节不正确（非 0x51）
	ErrInvalidMagic = errors.New("protoq: invalid magic byte")

	// ErrRequiresAckNoSeq ACK_REQ=1 但 SEQ_LEN=0（需要序列号来匹配响应）
	ErrRequiresAckNoSeq = errors.New("protoq: ACK_REQ requires SEQ_LEN > 0")

	// ErrResponseRequiresAck 响应帧不能设置 ACK_REQ（响应不能要求再次应答）
	ErrResponseRequiresAck = errors.New("protoq: response frame cannot set ACK_REQ")

	// ErrBodyWithoutLength BODY_LEN=0 时不能有 Body（变体 B 必须无载荷）
	ErrBodyWithoutLength = errors.New("protoq: body present but BODY_LEN is 0")

	// ErrInvalidLength Length 字段值小于 Opcode+Seq+CRC 最小长度
	ErrInvalidLength = errors.New("protoq: invalid length field")

	// ErrInvalidFrame 帧格式无效（通用）
	ErrInvalidFrame = errors.New("protoq: invalid frame")

	// ErrInvalidFlagCombination 无效的标志位组合
	ErrInvalidFlagCombination = errors.New("protoq: invalid flag combination")
)

// ─── 字段长度错误 ────────────────────────────────────────────────────────

var (
	// ErrInvalidOpcodeLen 无效的 Opcode 长度（非 0/2/4）
	ErrInvalidOpcodeLen = errors.New("protoq: invalid opcode length")

	// ErrInvalidSeqLen 无效的 Seq 长度（非 0/2/4）
	ErrInvalidSeqLen = errors.New("protoq: invalid seq length")

	// ErrInvalidCRCLen 无效的 CRC 长度（当前仅支持 0 或 2）
	ErrInvalidCRCLen = errors.New("protoq: invalid crc length, only 0 or 2 supported")
)

// ─── 传输与校验错误 ──────────────────────────────────────────────────────

var (
	// ErrCRCMismatch CRC 校验失败（帧尾 CRC 与计算值不一致）
	ErrCRCMismatch = errors.New("protoq: CRC mismatch")

	// ErrTimeout 请求超时（请求级 context 到期）
	ErrTimeout = errors.New("protoq: request timeout")

	// ErrMaxRetries 超过最大重传次数
	ErrMaxRetries = errors.New("protoq: max retries exceeded")
)

// ─── 连接错误 ────────────────────────────────────────────────────────────

var (
	// ErrConnClosed 连接已关闭（无法继续读写）
	ErrConnClosed = errors.New("protoq: connection closed")
)

// ─── 错误包装 ────────────────────────────────────────────────────────────

// ProtoQError 带操作上下文的协议错误，用于在调用链中附加操作名称。
type ProtoQError struct {
	Op  string // 操作名称（如 "send request", "decode"）
	Err error  // 底层错误
}

func (e *ProtoQError) Error() string {
	return "protoq: " + e.Op + ": " + e.Err.Error()
}

func (e *ProtoQError) Unwrap() error {
	return e.Err
}

// WrapError 包装错误并附加操作上下文。
// 若 err 为 nil 则返回 nil。
func WrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &ProtoQError{Op: op, Err: err}
}
