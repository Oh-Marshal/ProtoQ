// Package protoq — 消息服务器接口
//
// 对标 Java uni-protocol 的 MessageServer 接口。
// 定义服务端生命周期、连接管理和 Bean 注册能力。
package protoq

// MessageServer 消息服务器接口。对标 uni-protocol org.facelang.unified.proto.api.MessageServer。
type MessageServer interface {
	// GetConnection 按连接 ID 获取连接。
	GetConnection(connectionID string) Connection

	// GetConnectionCount 获取当前活跃连接数。
	GetConnectionCount() int

	// GetServerType 获取服务端类型标识。
	GetServerType() string

	// RegisterBean 注册组件（Codec/Filter/Handler 等）。
	// 对标 uni-protocol MessageServer.registerBean(bean)。
	RegisterBean(bean interface{})

	// IsRunning 检查服务端是否正在运行。
	IsRunning() bool

	// Start 启动服务端监听指定端口。
	Start(port int) error

	// Stop 停止服务端。
	Stop() error
}
