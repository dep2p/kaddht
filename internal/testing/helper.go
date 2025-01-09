package testing

import (
	"bytes"
	"errors"
)

// TestValidator 测试验证器结构体
type TestValidator struct{}

// Select 从多个字节数组中选择一个有效的记录
// 参数:
//   - _: string 未使用的参数
//   - bs: [][]byte 字节数组切片
//
// 返回值:
//   - int 选中的记录索引
//   - error 错误信息
func (TestValidator) Select(_ string, bs [][]byte) (int, error) {
	index := -1
	for i, b := range bs {
		if bytes.Equal(b, []byte("newer")) {
			index = i
		} else if bytes.Equal(b, []byte("valid")) {
			if index == -1 {
				index = i
			}
		}
	}
	if index == -1 {
		return -1, errors.New("未找到记录")
	}
	return index, nil
}

// Validate 验证单个记录是否有效
// 参数:
//   - _: string 未使用的参数
//   - b: []byte 要验证的字节数组
//
// 返回值:
//   - error 错误信息
func (TestValidator) Validate(_ string, b []byte) error {
	if bytes.Equal(b, []byte("expired")) {
		return errors.New("记录已过期")
	}
	return nil
}
