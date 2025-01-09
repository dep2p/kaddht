package record

import (
	"strings"
)

// SplitKey 将形如 `/$namespace/$path` 的键拆分为 `$namespace` 和 `$path`
// 参数:
//   - key: string 要拆分的键
//
// 返回值:
//   - string 命名空间
//   - string 路径
//   - error 错误信息
func SplitKey(key string) (string, string, error) {
	if len(key) == 0 || key[0] != '/' {
		return "", "", ErrInvalidRecordType
	}

	key = key[1:]

	i := strings.IndexByte(key, '/')
	if i <= 0 {
		return "", "", ErrInvalidRecordType
	}

	return key[:i], key[i+1:], nil
}
