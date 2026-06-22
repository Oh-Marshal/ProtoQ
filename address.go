// Package protoq — 网络地址与配置
//
// 对标 Java uni-protocol 的 NetworkAddress 和 NetworkConfig。
package protoq

import "fmt"

// AddressType 地址类型。
type AddressType int

const (
	AddrIPv4       AddressType = iota // IPv4
	AddrIPv6                          // IPv6
	AddrUnixSocket                    // Unix Domain Socket
	AddrUnknown                       // 未知类型
)

// NetworkAddress 网络地址。对标 uni-protocol org.facelang.unified.proto.model.NetworkAddress。
type NetworkAddress struct {
	Host string      // 主机地址（IP 或域名）
	Port int         // 端口号
	Type AddressType // 地址类型
}

// NewNetworkAddress 创建一个 IPv4 网络地址。
func NewNetworkAddress(host string, port int) *NetworkAddress {
	return &NetworkAddress{Host: host, Port: port, Type: AddrIPv4}
}

// String 返回标准地址字符串。
func (a *NetworkAddress) String() string {
	if a.Type == AddrUnixSocket {
		return a.Host
	}
	return fmt.Sprintf("%s:%d", a.Host, a.Port)
}

// IsLocalhost 判断是否为本地地址。
func (a *NetworkAddress) IsLocalhost() bool {
	return a.Host == "localhost" || a.Host == "127.0.0.1" || a.Host == "::1"
}

// ─── NetworkConfig ────────────────────────────────────────────────────────────

// NetworkConfig 网络配置。对标 uni-protocol org.facelang.unified.proto.model.NetworkConfig。
type NetworkConfig struct {
	// AckQueueCapacity ACK 等待队列容量（默认 8）。
	AckQueueCapacity int
}

// DefaultNetworkConfig 返回默认网络配置。
func DefaultNetworkConfig() *NetworkConfig {
	return &NetworkConfig{AckQueueCapacity: 8}
}
