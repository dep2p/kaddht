package routinghelpers

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

var _ routing.Routing = (*composableSequential)(nil)
var _ ProvideManyRouter = (*composableSequential)(nil)
var _ ReadyAbleRouter = (*composableSequential)(nil)
var _ ComposableRouter = (*composableSequential)(nil)

const sequentialName = "ComposableSequential"

type composableSequential struct {
	routers []*SequentialRouter
}

// NewComposableSequential 创建一个新的顺序组合路由器
// 参数:
//   - routers: []*SequentialRouter 路由器列表
//
// 返回值:
//   - *composableSequential 顺序组合路由器实例
func NewComposableSequential(routers []*SequentialRouter) *composableSequential {
	return &composableSequential{
		routers: routers,
	}
}

// Routers 获取所有路由器列表
// 返回值:
//   - []routing.Routing 路由器列表
func (r *composableSequential) Routers() []routing.Routing {
	var routers []routing.Routing
	for _, sr := range r.routers {
		routers = append(routers, sr.Router)
	}

	return routers
}

// Provide 按顺序调用每个路由器的Provide方法
// 如果某个路由器失败且IgnoreError标志为true,将继续执行下一个路由器
// 如果设置了标志,上下文超时错误也会被忽略
// 参数:
//   - ctx: context.Context 上下文
//   - cid: cid.Cid 内容标识符
//   - provide: bool 是否提供
//
// 返回值:
//   - error 错误信息
func (r *composableSequential) Provide(ctx context.Context, cid cid.Cid, provide bool) (err error) {
	ctx, end := tracer.Provide(sequentialName, ctx, cid, provide)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Provide(ctx, cid, provide)
		})
}

// ProvideMany 按顺序调用所有支持的路由器,对于不支持ProvideManyRouter的路由器将回退到迭代单个Provide调用
// 参数:
//   - ctx: context.Context 上下文
//   - keys: []multihash.Multihash 多重哈希列表
//
// 返回值:
//   - error 错误信息
func (r *composableSequential) ProvideMany(ctx context.Context, keys []multihash.Multihash) (err error) {
	ctx, end := tracer.ProvideMany(sequentialName, ctx, keys)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			if pm, ok := r.(ProvideManyRouter); ok {
				return pm.ProvideMany(ctx, keys)
			}

			for _, k := range keys {
				if err := r.Provide(ctx, cid.NewCidV1(cid.Raw, k), true); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

// Ready 按顺序调用所有支持ReadyAbleRouter的路由器
// 如果其中任何一个未就绪,此方法将返回false
// 返回值:
//   - bool 是否就绪
func (r *composableSequential) Ready() bool {
	for _, ro := range r.routers {
		pm, ok := ro.Router.(ReadyAbleRouter)
		if !ok {
			continue
		}

		if !pm.Ready() {
			return false
		}
	}

	return true
}

// FindProvidersAsync 按顺序调用每个路由器的FindProvidersAsync方法
// 如果某个路由器失败且IgnoreError标志为true,将继续执行下一个路由器
// 如果设置了标志,上下文超时错误也会被忽略
// 如果设置了count,通道将最多返回count个结果,然后停止路由器迭代
// 参数:
//   - ctx: context.Context 上下文
//   - cid: cid.Cid 内容标识符
//   - count: int 最大结果数
//
// 返回值:
//   - <-chan peer.AddrInfo 节点地址信息通道
func (r *composableSequential) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	ctx, wrapper := tracer.FindProvidersAsync(sequentialName, ctx, cid, count)

	var totalCount int64
	return wrapper(getChannelOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan peer.AddrInfo, error) {
			return r.FindProvidersAsync(ctx, cid, count), nil
		},
		func() bool {
			return atomic.AddInt64(&totalCount, 1) > int64(count) && count != 0
		},
	), nil)
}

// FindPeer 按顺序调用每个路由器的FindPeer方法
// 如果某个路由器失败且IgnoreError标志为true,将继续执行下一个路由器
// 如果设置了标志,上下文超时错误也会被忽略
// 参数:
//   - ctx: context.Context 上下文
//   - pid: peer.ID 节点ID
//
// 返回值:
//   - peer.AddrInfo 节点地址信息
//   - error 错误信息
func (r *composableSequential) FindPeer(ctx context.Context, pid peer.ID) (p peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(sequentialName, ctx, pid)
	defer func() { end(p, err) }()

	return getValueOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (peer.AddrInfo, bool, error) {
			addr, err := r.FindPeer(ctx, pid)
			return addr, addr.ID == "", err
		},
	)
}

// PutValue 按顺序调用每个路由器的PutValue方法
// 如果某个路由器失败且IgnoreError标志为true,将继续执行下一个路由器
// 如果设置了标志,上下文超时错误也会被忽略
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - error 错误信息
func (r *composableSequential) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(sequentialName, ctx, key, val, opts...)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.PutValue(ctx, key, val, opts...)
		})
}

// GetValue 按顺序调用每个路由器的GetValue方法
// 如果某个路由器失败且IgnoreError标志为true,将继续执行下一个路由器
// 如果设置了标志,上下文超时错误也会被忽略
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - []byte 值
//   - error 错误信息
func (r *composableSequential) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	return getValueOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) ([]byte, bool, error) {
			val, err := r.GetValue(ctx, key, opts...)
			return val, len(val) == 0, err
		},
	)
}

// SearchValue 按顺序调用每个路由器的SearchValue方法
// 如果某个路由器失败且IgnoreError标志为true,将继续执行下一个路由器
// 如果设置了标志,上下文超时错误也会被忽略
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - <-chan []byte 值通道
//   - error 错误信息
func (r *composableSequential) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	ctx, wrapper := tracer.SearchValue(sequentialName, ctx, key, opts...)

	return wrapper(getChannelOrErrorSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan []byte, error) {
			return r.SearchValue(ctx, key, opts...)
		},
		func() bool { return false },
	), nil)

}

// Bootstrap 按顺序调用每个路由器的Bootstrap方法
// 如果某个路由器失败且IgnoreError标志为true,将继续执行下一个路由器
// 如果设置了标志,上下文超时错误也会被忽略
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (r *composableSequential) Bootstrap(ctx context.Context) (err error) {
	ctx, end := tracer.Bootstrap(sequentialName, ctx)
	defer func() { end(err) }()

	return executeSequential(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Bootstrap(ctx)
		},
	)
}

// getValueOrErrorSequential 按顺序从路由器获取值或错误
// 参数:
//   - ctx: context.Context 上下文
//   - routers: []*SequentialRouter 路由器列表
//   - f: func(context.Context, routing.Routing) (T, bool, error) 处理函数
//
// 返回值:
//   - T 泛型值
//   - error 错误信息
func getValueOrErrorSequential[T any](
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing) (T, bool, error),
) (value T, err error) {
	for _, router := range routers {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return value, ctxErr
		}

		ctx, cancel := withCancelAndOptionalTimeout(ctx, router.Timeout)
		defer cancel()

		value, empty, err := f(ctx, router.Router)
		if err != nil &&
			!errors.Is(err, routing.ErrNotFound) &&
			!router.IgnoreError {
			return value, err
		}

		if empty {
			continue
		}

		return value, nil
	}

	return value, routing.ErrNotFound
}

// executeSequential 按顺序执行路由器操作
// 参数:
//   - ctx: context.Context 上下文
//   - routers: []*SequentialRouter 路由器列表
//   - f: func(context.Context, routing.Routing) error 执行函数
//
// 返回值:
//   - error 错误信息
func executeSequential(
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing,
	) error) error {
	for _, router := range routers {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		ctx, cancel := withCancelAndOptionalTimeout(ctx, router.Timeout)
		defer cancel()

		if err := f(ctx, router.Router); err != nil &&
			!errors.Is(err, routing.ErrNotFound) &&
			!router.IgnoreError {
			return err
		}
	}

	return nil
}

// getChannelOrErrorSequential 按顺序从路由器获取通道或错误
// 参数:
//   - ctx: context.Context 上下文
//   - routers: []*SequentialRouter 路由器列表
//   - f: func(context.Context, routing.Routing) (<-chan T, error) 处理函数
//   - shouldStop: func() bool 停止条件函数
//
// 返回值:
//   - chan T 泛型通道
func getChannelOrErrorSequential[T any](
	ctx context.Context,
	routers []*SequentialRouter,
	f func(context.Context, routing.Routing) (<-chan T, error),
	shouldStop func() bool,
) chan T {
	chanOut := make(chan T)

	go func() {
		for _, router := range routers {
			if ctxErr := ctx.Err(); ctxErr != nil {
				close(chanOut)
				return
			}
			ctx, cancel := withCancelAndOptionalTimeout(ctx, router.Timeout)
			defer cancel()
			rch, err := f(ctx, router.Router)
			if err != nil &&
				!errors.Is(err, routing.ErrNotFound) &&
				!router.IgnoreError {
				break
			}

		f:
			for {
				select {
				case <-ctx.Done():
					break f
				case v, ok := <-rch:
					if !ok {
						break f
					}
					select {
					case <-ctx.Done():
						break f
					case chanOut <- v:
					}

				}
			}
		}

		close(chanOut)
	}()

	return chanOut
}
