package protoq

import "errors"

// ProtoQ 协议错误定义
var (
	// ErrInvalidMagic Magic 字节不正确
	ErrInvalidMagic = errors.New("protoq: invalid magic byte")

	// ErrACKReqNeedsSeq ACK_REQ=1 时缺少序列号
	ErrACKReqNeedsSeq = errors.New("protoq: ACK_REQ requires SEQ_LEN > 0")

	// ErrResponseCantAckReq 响应帧不能设置 ACK_REQ
	ErrResponseCantAckReq = errors.New("protoq: response frame cannot set ACK_REQ")

	// ErrBodyWithoutLength HAS_LEN=0 时不能有 Body
	ErrBodyWithoutLength = errors.New("protoq: body present but HAS_LEN is 0")

	// ErrInvalidLength 长度字段无效（Length < Opcode+Seq+CRC 最小长度）
	ErrInvalidLength = errors.New("protoq: invalid length field")

	// ErrInvalidFrame 帧格式无效
	ErrInvalidFrame = errors.New("protoq: invalid frame")

	// ErrCRC Mismatch CRC 校验失败
	ErrCRCMismatch = errors.New("protoq: CRC mismatch")

	// ErrTimeout 请求超时
	ErrTimeout = errors.New("protoq: request timeout")

	// ErrMaxRetries 超过最大重传次数
	ErrMaxRetries = errors.New("protoq: max retries exceeded")

	// ErrConnClosed 连接已关闭
	ErrConnClosed = errors.New("protoq: connection closed")

	// ErrInvalidOpcodeLen 无效的 Opcode 长度
	ErrInvalidOpcodeLen = errors.New("protoq: invalid opcode length")

	// ErrInvalidSeqLen 无效的 Seq 长度
	ErrInvalidSeqLen = errors.New("protoq: invalid seq length")

	// ErrInvalidCRCLen 无效的 CRC 长度
	ErrInvalidCRCLen = errors.New("protoq: invalid crc length, only 0 or 2 supported")

	// ErrInvalidFlagCombination 无效的标志位组合
	ErrInvalidFlagCombination = errors.New("protoq: invalid flag combination")
)

// ProtoQError 带上下文的协议错误
type ProtoQError struct {
	Op  string // 操作名称
	Err error  // 底层错误
}

func (e *ProtoQError) Error() string {
	return "protoq: " + e.Op + ": " + e.Err.Error()
}

func (e *ProtoQError) Unwrap() error {
	return e.Err
}

// WrapError 包装错误并附加操作上下文。
func WrapError(op string, err error) error {
	if err == nil {
		return nil
	}
	return &ProtoQError{Op: op, Err: err}
}
