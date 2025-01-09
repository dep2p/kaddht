package internal

import (
	"fmt"
	"strings"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
)

// multibaseB32Encode 使用Base32编码字节数组
// 参数:
//   - k: []byte 要编码的字节数组
//
// 返回值:
//   - string 编码后的字符串
func multibaseB32Encode(k []byte) string {
	res, err := multibase.Encode(multibase.Base32, k)
	if err != nil {
		// 不应该到达这里
		panic(err)
	}
	return res
}

// tryFormatLoggableRecordKey 尝试格式化记录键为可记录的格式
// 参数:
//   - k: string 记录键
//
// 返回值:
//   - string 格式化后的记录键
//   - error 错误信息
func tryFormatLoggableRecordKey(k string) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("记录键为空")
	}
	var proto, cstr string
	if k[0] == '/' {
		// 可能是路径
		protoEnd := strings.IndexByte(k[1:], '/')
		if protoEnd < 0 {
			return "", fmt.Errorf("记录键以'/'开头但不是路径: %s", multibaseB32Encode([]byte(k)))
		}
		proto = k[1 : protoEnd+1]
		cstr = k[protoEnd+2:]

		encStr := multibaseB32Encode([]byte(cstr))
		return fmt.Sprintf("/%s/%s", proto, encStr), nil
	}

	return "", fmt.Errorf("记录键不是路径: %s", multibaseB32Encode([]byte(cstr)))
}

// LoggableRecordKeyString 可记录的字符串类型记录键
type LoggableRecordKeyString string

// String 实现Stringer接口,返回格式化的记录键字符串
// 返回值:
//   - string 格式化后的记录键字符串
func (lk LoggableRecordKeyString) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableRecordKey(k)
	if err == nil {
		return newKey
	}
	return err.Error()
}

// LoggableRecordKeyBytes 可记录的字节数组类型记录键
type LoggableRecordKeyBytes []byte

// String 实现Stringer接口,返回格式化的记录键字符串
// 返回值:
//   - string 格式化后的记录键字符串
func (lk LoggableRecordKeyBytes) String() string {
	k := string(lk)
	newKey, err := tryFormatLoggableRecordKey(k)
	if err == nil {
		return newKey
	}
	return err.Error()
}

// LoggableProviderRecordBytes 可记录的提供者记录字节数组
type LoggableProviderRecordBytes []byte

// String 实现Stringer接口,返回格式化的提供者记录字符串
// 返回值:
//   - string 格式化后的提供者记录字符串
func (lk LoggableProviderRecordBytes) String() string {
	newKey, err := tryFormatLoggableProviderKey(lk)
	if err == nil {
		return newKey
	}
	return err.Error()
}

// tryFormatLoggableProviderKey 尝试格式化提供者键为可记录的格式
// 参数:
//   - k: []byte 提供者键
//
// 返回值:
//   - string 格式化后的提供者键
//   - error 错误信息
func tryFormatLoggableProviderKey(k []byte) (string, error) {
	if len(k) == 0 {
		return "", fmt.Errorf("提供者键为空")
	}

	encodedKey := multibaseB32Encode(k)

	// DHT 过去提供 CID,但现在提供 multihash
	// TODO: 当网络中足够多的节点升级后删除这段代码
	if _, err := cid.Cast(k); err == nil {
		return encodedKey, nil
	}

	if _, err := multihash.Cast(k); err == nil {
		return encodedKey, nil
	}

	return "", fmt.Errorf("提供者键不是 Multihash 或 CID: %s", encodedKey)
}
