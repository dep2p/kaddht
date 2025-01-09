package keyspace

import (
	"bytes"
	"math/big"
	"math/bits"

	u "github.com/ipfs/boxo/util"
	sha256 "github.com/minio/sha256-simd"
)

// XORKeySpace 是一个 KeySpace,它:
// - 使用加密哈希(sha256)规范化标识符
// - 通过对键进行异或运算来度量距离
var XORKeySpace = &xorKeySpace{}
var _ KeySpace = XORKeySpace // 确保它符合接口

type xorKeySpace struct{}

// Key 将标识符转换为此空间中的 Key
// 参数:
//   - id: []byte 标识符
//
// 返回值:
//   - Key 转换后的key
func (s *xorKeySpace) Key(id []byte) Key {
	hash := sha256.Sum256(id)
	key := hash[:]
	return Key{
		Space:    s,
		Original: id,
		Bytes:    key,
	}
}

// Equal 判断在此 key space 中两个 key 是否相等
// 参数:
//   - k1: Key 第一个key
//   - k2: Key 第二个key
//
// 返回值:
//   - bool 是否相等
func (s *xorKeySpace) Equal(k1, k2 Key) bool {
	return bytes.Equal(k1.Bytes, k2.Bytes)
}

// Distance 返回此 key space 中的距离度量
// 参数:
//   - k1: Key 第一个key
//   - k2: Key 第二个key
//
// 返回值:
//   - *big.Int 距离值
func (s *xorKeySpace) Distance(k1, k2 Key) *big.Int {
	// 对键进行异或运算
	k3 := u.XOR(k1.Bytes, k2.Bytes)

	// 将其解释为整数
	dist := big.NewInt(0).SetBytes(k3)
	return dist
}

// Less 判断第一个 key 是否小于第二个
// 参数:
//   - k1: Key 第一个key
//   - k2: Key 第二个key
//
// 返回值:
//   - bool 是否小于
func (s *xorKeySpace) Less(k1, k2 Key) bool {
	return bytes.Compare(k1.Bytes, k2.Bytes) < 0
}

// ZeroPrefixLen 返回字节切片中连续零的数量
// 参数:
//   - id: []byte 字节切片
//
// 返回值:
//   - int 连续零的数量
func ZeroPrefixLen(id []byte) int {
	for i, b := range id {
		if b != 0 {
			return i*8 + bits.LeadingZeros8(uint8(b))
		}
	}
	return len(id) * 8
}
