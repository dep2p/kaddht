package internal

import "errors"

// ErrIncorrectRecord 接收到错误记录时返回的错误
// 返回值:
//   - error 错误信息
var ErrIncorrectRecord = errors.New("接收到错误的记录")
