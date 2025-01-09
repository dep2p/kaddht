package key

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	kbucket "github.com/dep2p/kaddht/kbucket"
)

// KbucketIDToKey 将kbucket.ID转换为Key
// 参数:
//   - id: kbucket.ID kbucket的ID
//
// 返回值:
//   - Key 转换后的Key
func KbucketIDToKey(id kbucket.ID) Key {
	return Key(id)
}

// ByteKey 将单个字节转换为Key
// 参数:
//   - b: byte 输入字节
//
// 返回值:
//   - Key 转换后的Key
func ByteKey(b byte) Key {
	return Key{b}
}

// BytesKey 将字节切片转换为Key
// 参数:
//   - b: []byte 输入字节切片
//
// 返回值:
//   - Key 转换后的Key
func BytesKey(b []byte) Key {
	return Key(b)
}

// Key 是由Go字节切片支持的位向量
// 第一个字节最重要
// 每个字节中的第一位最不重要
type Key []byte

// BitAt 获取指定偏移位置的位值
// 参数:
//   - offset: int 位偏移量
//
// 返回值:
//   - byte 位值(0或1)
func (k Key) BitAt(offset int) byte {
	if k[offset/8]&(byte(1)<<(7-offset%8)) == 0 {
		return 0
	} else {
		return 1
	}
}

// NormInt 将Key转换为大整数
// 返回值:
//   - *big.Int 转换后的大整数
func (k Key) NormInt() *big.Int {
	return big.NewInt(0).SetBytes(k)
}

// BitLen 获取Key的位长度
// 返回值:
//   - int 位长度
func (k Key) BitLen() int {
	return 8 * len(k)
}

// String 将Key转换为字符串表示
// 返回值:
//   - string Key的字符串表示
func (k Key) String() string {
	b, _ := json.Marshal(k)
	return string(b)
}

// BitString 返回Key的位表示，按重要性降序排列
// 返回值:
//   - string Key的位字符串表示
func (k Key) BitString() string {
	s := make([]string, len(k))
	for i, b := range k {
		s[i] = fmt.Sprintf("%08b", b)
	}
	return strings.Join(s, "")
}

// Equal 比较两个Key是否相等
// 参数:
//   - x: Key 第一个Key
//   - y: Key 第二个Key
//
// 返回值:
//   - bool 是否相等
func Equal(x, y Key) bool {
	return bytes.Equal(x, y)
}

// Xor 计算两个Key的异或值
// 参数:
//   - x: Key 第一个Key
//   - y: Key 第二个Key
//
// 返回值:
//   - Key 异或结果
func Xor(x, y Key) Key {
	z := make(Key, len(x))
	for i := range x {
		z[i] = x[i] ^ y[i]
	}
	return z
}

// DistInt 计算两个Key之间的距离
// 参数:
//   - x: Key 第一个Key
//   - y: Key 第二个Key
//
// 返回值:
//   - *big.Int 距离值
func DistInt(x, y Key) *big.Int {
	return Xor(x, y).NormInt()
}
