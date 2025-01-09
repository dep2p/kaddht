package routinghelpers

import (
	"context"
)

// TODO: 考虑将此移动到routing包?

// Bootstrap 需要被引导的路由器应该实现的接口
type Bootstrap interface {
	// Bootstrap 引导路由器
	// 参数:
	//   - ctx: context.Context 上下文
	//
	// 返回值:
	//   - error 错误信息
	Bootstrap(ctx context.Context) error
}
