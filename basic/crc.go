package basic

import "hash/crc32"

// CRC-16-IBM 预计算查找表
// 参数：多项式 0x8005, 初始值 0x0000, 反射输入/输出, 无最终异或
var crc16IBMTable [256]uint16

func init() {
	for i := 0; i < 256; i++ {
		crc := uint16(i)
		for j := 0; j < 8; j++ {
			if crc&1 != 0 {
				crc = (crc >> 1) ^ 0xA001 // 反射多项式 0x8005 → 0xA001
			} else {
				crc >>= 1
			}
		}
		crc16IBMTable[i] = crc
	}
}

// CRC16IBM 计算 CRC-16-IBM 校验值。
// 等同于 CRC-16-ARC / CRC-16-LHA。
func CRC16IBM(data []byte) uint16 {
	var crc uint16
	for _, b := range data {
		crc = (crc >> 8) ^ crc16IBMTable[byte(crc)^b]
	}
	return crc
}

// CRC32IEEE 计算 CRC-32-IEEE 校验值。
// 使用 Go 标准库 hash/crc32.IEEE 表。
func CRC32IEEE(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// ComputeCRC 根据 CRC 长度计算校验值。
// crcLen: 0=无, 2=CRC-16-IBM, 4=CRC-32-IEEE。
func ComputeCRC(data []byte, crcLen int) []byte {
	switch crcLen {
	case 2:
		v := CRC16IBM(data)
		return []byte{byte(v >> 8), byte(v)}
	case 4:
		v := CRC32IEEE(data)
		return []byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
	default:
		return nil
	}
}

// VerifyCRC 验证 CRC 校验值。
func VerifyCRC(data []byte, expected []byte, crcLen int) bool {
	computed := ComputeCRC(data, crcLen)
	if len(computed) != len(expected) {
		return false
	}
	for i := range computed {
		if computed[i] != expected[i] {
			return false
		}
	}
	return true
}
