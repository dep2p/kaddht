package routinghelpers

import (
	"context"
	"time"

	"github.com/dep2p/kaddht/helpers/tracing"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/multiformats/go-multihash"
)

// 用于路由助手的追踪器
const tracer = tracing.Tracer("dep2p-routing-helpers")

// ParallelRouter 并行路由器结构体
type ParallelRouter struct {
	// 超时时间
	Timeout time.Duration
	// 路由器实例
	Router routing.Routing
	// 执行延迟时间
	ExecuteAfter time.Duration
	// DoNotWaitForSearchValue 是实验性的,等待更好的解决方案
	DoNotWaitForSearchValue bool
	// 是否忽略错误
	IgnoreError bool
}

// SequentialRouter 顺序路由器结构体
type SequentialRouter struct {
	// 超时时间
	Timeout time.Duration
	// 是否忽略错误
	IgnoreError bool
	// 路由器实例
	Router routing.Routing
}

// ProvideManyRouter 批量提供者路由器接口
type ProvideManyRouter interface {
	// ProvideMany 批量提供多个键
	// 参数:
	//   - ctx: context.Context 上下文
	//   - keys: []multihash.Multihash 多个键的哈希值
	//
	// 返回值:
	//   - error 错误信息
	ProvideMany(ctx context.Context, keys []multihash.Multihash) error
}

// ReadyAbleRouter 可就绪路由器接口
type ReadyAbleRouter interface {
	// Ready 检查路由器是否就绪
	// 返回值:
	//   - bool 是否就绪
	Ready() bool
}

// ComposableRouter 可组合路由器接口
type ComposableRouter interface {
	// Routers 获取路由器列表
	// 返回值:
	//   - []routing.Routing 路由器列表
	Routers() []routing.Routing
}
