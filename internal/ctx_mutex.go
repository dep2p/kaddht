package internal

import (
	"context"
)

// CtxMutex 带上下文的互斥锁
type CtxMutex chan struct{}

// NewCtxMutex 创建一个新的带上下文的互斥锁
// 返回值:
//   - CtxMutex 互斥锁实例
func NewCtxMutex() CtxMutex {
	return make(CtxMutex, 1)
}

// Lock 获取互斥锁
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (m CtxMutex) Lock(ctx context.Context) error {
	select {
	case m <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Unlock 释放互斥锁
func (m CtxMutex) Unlock() {
	select {
	case <-m:
	default:
		panic("未加锁")
	}
}
