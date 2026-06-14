// Package protoq — 默认编解码器实现（明文透传）
//
// 对标 Java uni-protocol 的 DefaultCodec：在未协商加密或无需加密的场景下，
// 提供明文透传的加解密行为。
package basic

import "encoding/json"

// DefaultCodec 默认编解码器：明文透传，使用 JSON 序列化。
// 对标 uni-protocol basic.codec.DefaultCodec。
// 在协商完成前（messageId=0x01/0x02）或未配置加密时使用。
type DefaultCodec struct{}

// Converter 返回 JSON 序列化器。
func (d *DefaultCodec) Converter() Converter {
	return &JSONConverter{}
}

// Decrypt 明文透传（不解密）。
func (d *DefaultCodec) Decrypt(data []byte) ([]byte, error) {
	return data, nil
}

// Encrypt 明文透传（不加密）。
func (d *DefaultCodec) Encrypt(data []byte) ([]byte, error) {
	return data, nil
}

// Match 匹配所有加密类型（作为默认兜底）。
func (d *DefaultCodec) Match(encryptType uint16) bool {
	return true
}

// JSONConverter JSON 序列化/反序列化器。
// 对标 uni-protocol basic.codec.convert.JacksonConverter。
type JSONConverter struct{}

// Read 将 JSON 字节反序列化到 target（target 必须是指针）。
func (j *JSONConverter) Read(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}

// Write 将对象序列化为 JSON 字节。
func (j *JSONConverter) Write(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}
