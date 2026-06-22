package conn

import (
	"context"
	"errors"
	"net"
)

// QUICTransport QUIC 传输层（桩实现，不可用于生产环境）。
//
// QUIC 协议需要第三方库（如 github.com/quic-go/quic-go），
// 标准库不包含 QUIC 支持。此处提供接口占位，Dial/Listen 始终返回错误。
// 正式使用前请按以下步骤完成集成后才可启用。
//
// 集成步骤：
//  1. 导入 quic-go: go get github.com/quic-go/quic-go
//  2. 实现 Dial/Listen 方法，将 quic.Stream 包装为 net.Conn
//  3. 由于 QUIC 是多流复用，Conn 对应 quic.Stream 而非 quic.Connection
type QUICTransport struct {
	// TLSConfig 用于 QUIC 的 TLS 配置（QUIC 强制加密）
	// TLSConfig *tls.Config
}

// NewQUICTransport 创建一个 QUIC 传输桩实例。
func NewQUICTransport() *QUICTransport {
	return &QUICTransport{}
}

// Dial 使用 QUIC 连接到指定地址。
// 当前未实现，始终返回错误。
func (t *QUICTransport) Dial(ctx context.Context, addr string) (net.Conn, error) {
	return nil, errors.New("protoq: QUIC transport requires github.com/quic-go/quic-go (not implemented in stdlib)")
}

// Listen 使用 QUIC 在指定地址上监听。
// 当前未实现，始终返回错误。
func (t *QUICTransport) Listen(ctx context.Context, addr string) (net.Listener, error) {
	return nil, errors.New("protoq: QUIC transport requires github.com/quic-go/quic-go (not implemented in stdlib)")
}

// Protocol 返回传输协议名称。
func (t *QUICTransport) Protocol() string {
	return "quic"
}
