// Package protoq — 编解码与序列化接口
//
// 对标 Java uni-protocol 的 Codec / Converter 分层设计：
//   - Codec 负责二进制加解密（与协议帧解耦，操作对象为 []byte）
//   - Converter 负责业务对象 ↔ []byte 序列化/反序列化
//
// Codec.Register 通过 CodecRegister 按 encryptType 匹配选取。
// 协商消息（messageId=0x01/0x02）跳过加解密。
package api

// Codec 编解码器接口。对标 uni-protocol org.facelang.unified.proto.api.Codec。
//
// 负责二进制数据的加解密，操作对象为 []byte。
// 与 Converter 配合：Codec 处理加密层，Converter 处理序列化层。
type Codec interface {
	// Converter 返回绑定的序列化/反序列化器。
	Converter() Converter

	// Decrypt 解密：密文 []byte → 明文 []byte。
	// 协商消息不经过此方法（在 ConnectionBridge 层跳过）。
	Decrypt(data []byte) ([]byte, error)

	// Encrypt 加密：明文 []byte → 密文 []byte。
	// 协商消息不经过此方法。
	Encrypt(data []byte) ([]byte, error)

	// Match 判断本编解码器是否处理给定加密类型标识。
	// encryptType 来自协商阶段约定的加密方案编号。
	Match(encryptType uint16) bool
}

// Converter 序列化/反序列化器接口。对标 uni-protocol org.facelang.unified.proto.api.Converter。
//
// 纯粹的业务对象 ↔ []byte 转换，不涉及加密。
type Converter interface {
	// Read 将字节反序列化为指定类型的对象（Go 中通常返回 interface{}）。
	Read(data []byte, target interface{}) error

	// Write 将对象序列化为字节数组。
	Write(value interface{}) ([]byte, error)
}
