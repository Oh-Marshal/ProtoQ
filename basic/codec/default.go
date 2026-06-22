// Package codec — 默认编解码器实现
//
// 对标 Java uni-protocol basic.codec.DefaultCodec。
package codec

import (
	"encoding/json"

	api "github.com/oh-marshal/protoq"
)

// Default 默认编解码器：明文透传，使用 JSON 序列化。
// 对标 uni-protocol DefaultCodec。
type Default struct{}

// Converter 返回 JacksonConverter。
func (d *Default) Converter() api.Converter {
	return JacksonConverter{}
}

// Decrypt 明文透传。
func (d *Default) Decrypt(data []byte) ([]byte, error) {
	return data, nil
}

// Encrypt 明文透传。
func (d *Default) Encrypt(data []byte) ([]byte, error) {
	return data, nil
}

// Match 匹配 encryptType=0。
func (d *Default) Match(encryptType uint16) bool {
	return encryptType == 0
}

// JacksonConverter JSON 序列化器。
type JacksonConverter struct{}

// Read 将 JSON 字节反序列化。
func (j JacksonConverter) Read(data []byte, target interface{}) error {
	return json.Unmarshal(data, target)
}

// Write 将对象序列化为 JSON。
func (j JacksonConverter) Write(value interface{}) ([]byte, error) {
	return json.Marshal(value)
}
