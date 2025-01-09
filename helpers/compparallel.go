package routinghelpers

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jorropo/jsync"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/ipfs/go-cid"
	logging "github.com/ipfs/go-log"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.uber.org/multierr"
)

// 路由日志记录器
var log = logging.Logger("routing/composable")

// 确保 composableParallel 实现了所需接口
var _ routing.Routing = (*composableParallel)(nil)
var _ ProvideManyRouter = (*composableParallel)(nil)
var _ ReadyAbleRouter = (*composableParallel)(nil)
var _ ComposableRouter = (*composableParallel)(nil)

// 并行组合路由器名称
const nameParallel = "ComposableParallel"

// composableParallel 并行组合路由器结构体
type composableParallel struct {
	routers []*ParallelRouter
}

// NewComposableParallel 创建一个并行执行多个路由器方法的路由器
// 对于所有方法,如果设置了 IgnoreError 标志,该路由器不会停止整个执行
// 对于所有方法,如果设置了 ExecuteAfter,该路由器将在计时器之后执行
// 路由器特定的超时将在 ExecuteAfter 计时器之后开始计数
// 参数:
//   - routers: []*ParallelRouter 路由器列表
//
// 返回值:
//   - *composableParallel 并行组合路由器实例
func NewComposableParallel(routers []*ParallelRouter) *composableParallel {
	return &composableParallel{
		routers: routers,
	}
}

// Routers 获取路由器列表
// 返回值:
//   - []routing.Routing 路由器列表
func (r *composableParallel) Routers() []routing.Routing {
	var routers []routing.Routing
	for _, pr := range r.routers {
		routers = append(routers, pr.Router)
	}

	return routers
}

// Provide 并行调用所有路由器
// 参数:
//   - ctx: context.Context 上下文
//   - cid: cid.Cid 内容标识符
//   - provide: bool 是否提供
//
// 返回值:
//   - error 错误信息
func (r *composableParallel) Provide(ctx context.Context, cid cid.Cid, provide bool) (err error) {
	ctx, end := tracer.Provide(nameParallel, ctx, cid, provide)
	defer func() { end(err) }()

	return executeParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Provide(ctx, cid, provide)
		},
	)
}

// ProvideMany 并行调用所有路由器,对于不支持 ProvideManyRouter 的路由器回退到迭代单个 Provide 调用
// 参数:
//   - ctx: context.Context 上下文
//   - keys: []multihash.Multihash 多个键的哈希值
//
// 返回值:
//   - error 错误信息
func (r *composableParallel) ProvideMany(ctx context.Context, keys []multihash.Multihash) (err error) {
	ctx, end := tracer.ProvideMany(nameParallel, ctx, keys)
	defer func() { end(err) }()

	return executeParallel(ctx, r.routers,
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

// Ready 顺序调用所有支持 ReadyAbleRouter 的路由器
// 如果其中有任何一个未就绪,此方法将返回 false
// 返回值:
//   - bool 是否就绪
func (r *composableParallel) Ready() bool {
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

// FindProvidersAsync 并行执行所有路由器,以未指定顺序迭代它们的结果
// 如果设置了 count,将只返回指定数量的元素,不指定从哪个路由器获取
// 要首先从一组路由器收集提供者,可以使用 ExecuteAfter 计时器延迟某些路由器的执行
// 参数:
//   - ctx: context.Context 上下文
//   - cid: cid.Cid 内容标识符
//   - count: int 返回的最大结果数
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者地址信息通道
func (r *composableParallel) FindProvidersAsync(ctx context.Context, cid cid.Cid, count int) <-chan peer.AddrInfo {
	ctx, chWrapper := tracer.FindProvidersAsync(nameParallel, ctx, cid, count)

	var totalCount int64
	ch, err := getChannelOrErrorParallel(
		ctx,
		r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan peer.AddrInfo, error) {
			return r.FindProvidersAsync(ctx, cid, count), nil
		},
		func() bool {
			if count <= 0 {
				return false
			}
			return atomic.AddInt64(&totalCount, 1) >= int64(count)
		}, false,
	)

	if err != nil {
		ch = make(chan peer.AddrInfo)
		close(ch)
	}

	return chWrapper(ch, err)
}

// FindPeer 并行执行所有路由器,获取找到的第一个 AddrInfo 并取消所有其他路由器调用
// 参数:
//   - ctx: context.Context 上下文
//   - id: peer.ID 对等点ID
//
// 返回值:
//   - peer.AddrInfo 对等点地址信息
//   - error 错误信息
func (r *composableParallel) FindPeer(ctx context.Context, id peer.ID) (p peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(nameParallel, ctx, id)
	defer func() { end(p, err) }()

	return getValueOrErrorParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) (peer.AddrInfo, bool, error) {
			addr, err := r.FindPeer(ctx, id)
			return addr, addr.ID == "", err
		},
	)
}

// PutValue 并行执行所有路由器。如果路由器失败且未设置 IgnoreError 标志,整个执行将失败
// 即使我们返回错误,失败之前的一些 Put 可能已成功
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - error 错误信息
func (r *composableParallel) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(nameParallel, ctx, key, val, opts...)
	defer func() { end(err) }()

	return executeParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.PutValue(ctx, key, val, opts...)
		},
	)
}

// GetValue 并行执行所有路由器。返回找到的第一个值,取消所有其他执行
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - []byte 值
//   - error 错误信息
func (r *composableParallel) GetValue(ctx context.Context, key string, opts ...routing.Option) (val []byte, err error) {
	ctx, end := tracer.GetValue(nameParallel, ctx, key, opts...)
	defer func() { end(val, err) }()

	return getValueOrErrorParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) ([]byte, bool, error) {
			val, err := r.GetValue(ctx, key, opts...)
			return val, len(val) == 0, err
		})
}

// SearchValue 搜索值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - <-chan []byte 值通道
//   - error 错误信息
func (r *composableParallel) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	ctx, wrapper := tracer.SearchValue(nameParallel, ctx, key, opts...)

	return wrapper(getChannelOrErrorParallel(
		ctx,
		r.routers,
		func(ctx context.Context, r routing.Routing) (<-chan []byte, error) {
			return r.SearchValue(ctx, key, opts...)
		},
		func() bool { return false }, true,
	))
}

// Bootstrap 引导路由器
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (r *composableParallel) Bootstrap(ctx context.Context) (err error) {
	ctx, end := tracer.Bootstrap(nameParallel, ctx)
	defer end(err)

	return executeParallel(ctx, r.routers,
		func(ctx context.Context, r routing.Routing) error {
			return r.Bootstrap(ctx)
		})
}

// withCancelAndOptionalTimeout 创建带有可选超时的可取消上下文
// 参数:
//   - ctx: context.Context 上下文
//   - timeout: time.Duration 超时时间
//
// 返回值:
//   - context.Context 新上下文
//   - context.CancelFunc 取消函数
func withCancelAndOptionalTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout != 0 {
		return context.WithTimeout(ctx, timeout)
	}
	return context.WithCancel(ctx)
}

// getValueOrErrorParallel 并行获取值或错误
// 参数:
//   - ctx: context.Context 上下文
//   - routers: []*ParallelRouter 路由器列表
//   - f: func(context.Context, routing.Routing) (T, bool, error) 执行函数
//
// 返回值:
//   - T 值
//   - error 错误信息
func getValueOrErrorParallel[T any](
	ctx context.Context,
	routers []*ParallelRouter,
	f func(context.Context, routing.Routing) (T, bool, error),
) (value T, err error) {
	outCh := make(chan T)
	errCh := make(chan error)

	// 全局取消上下文以提前停止其他路由器的执行
	ctx, cancelAll := context.WithCancel(ctx)
	defer cancelAll()
	fwg := jsync.NewFWaitGroup(func() {
		close(outCh)
		close(errCh)
		log.Debug("getValueOrErrorParallel: 完成执行所有路由器 ", len(routers))
	}, uint64(len(routers)))
	for _, r := range routers {
		go func(r *ParallelRouter) {
			defer fwg.Done()
			log.Debug("getValueOrErrorParallel: 开始执行路由器 ", r.Router,
				" 超时时间 ", r.Timeout,
				" 忽略错误 ", r.IgnoreError,
			)
			tim := time.NewTimer(r.ExecuteAfter)
			defer tim.Stop()
			select {
			case <-ctx.Done():
			case <-tim.C:
				ctx, cancel := withCancelAndOptionalTimeout(ctx, r.Timeout)
				defer cancel()
				value, empty, err := f(ctx, r.Router)
				if err != nil {
					if r.IgnoreError || errors.Is(err, routing.ErrNotFound) {
						log.Debug("getValueOrErrorParallel: 未找到或可忽略错误,路由器 ", r.Router,
							" 超时时间 ", r.Timeout,
							" 忽略错误 ", r.IgnoreError,
						)
						return
					}
					log.Debug("getValueOrErrorParallel: 调用路由器函数出错,路由器 ", r.Router,
						" 超时时间 ", r.Timeout,
						" 忽略错误 ", r.IgnoreError,
						" 错误 ", err,
					)
					select {
					case <-ctx.Done():
					case errCh <- err:
					}
					return
				}
				if empty {
					log.Debug("getValueOrErrorParallel: 空标志,路由器 ", r.Router,
						" 超时时间 ", r.Timeout,
						" 忽略错误 ", r.IgnoreError,
					)
					return
				}
				select {
				case <-ctx.Done():
					return
				case outCh <- value:
				}
			}
		}(r)
	}

	select {
	case out, ok := <-outCh:
		if !ok {
			return value, routing.ErrNotFound
		}

		log.Debug("getValueOrErrorParallel: 通道返回值")

		return out, nil
	case err, ok := <-errCh:
		if !ok {
			return value, routing.ErrNotFound
		}

		log.Debug("getValueOrErrorParallel: 通道返回错误:", err)

		return value, err
	case <-ctx.Done():
		err := ctx.Err()
		log.Debug("getValueOrErrorParallel: 上下文完成时出错:", err)
		return value, err
	}
}

// executeParallel 并行执行
// 参数:
//   - ctx: context.Context 上下文
//   - routers: []*ParallelRouter 路由器列表
//   - f: func(context.Context, routing.Routing) error 执行函数
//
// 返回值:
//   - error 错误信息
func executeParallel(
	ctx context.Context,
	routers []*ParallelRouter,
	f func(context.Context, routing.Routing) error,
) error {
	var errsLk sync.Mutex
	var errs []error
	var wg sync.WaitGroup
	wg.Add(len(routers))

	for _, r := range routers {
		go func(r *ParallelRouter, ctx context.Context) {
			defer wg.Done()

			if err := func() error {
				log.Debug("executeParallel: 开始执行路由器 ", r.Router,
					" 超时时间 ", r.Timeout,
					" 忽略错误 ", r.IgnoreError,
				)
				tim := time.NewTimer(r.ExecuteAfter)
				defer tim.Stop()
				select {
				case <-ctx.Done():
					if !r.IgnoreError {
						log.Debug("executeParallel: 上下文完成时停止路由器执行 ", r.Router,
							" 超时时间 ", r.Timeout,
							" 忽略错误 ", r.IgnoreError,
						)
						return ctx.Err()
					}
				case <-tim.C:
					ctx, cancel := withCancelAndOptionalTimeout(ctx, r.Timeout)
					defer cancel()

					log.Debug("executeParallel: 调用路由器函数,路由器 ", r.Router,
						" 超时时间 ", r.Timeout,
						" 忽略错误 ", r.IgnoreError,
					)
					if err := f(ctx, r.Router); err != nil && !r.IgnoreError {
						log.Debug("executeParallel: 调用路由器函数出错,路由器 ", r.Router,
							" 超时时间 ", r.Timeout,
							" 忽略错误 ", r.IgnoreError,
							" 错误 ", err,
						)
						return err
					}
				}

				return nil
			}(); err != nil {
				errsLk.Lock()
				errs = append(errs, err)
				errsLk.Unlock()
			}
		}(r, ctx)
	}

	wg.Wait()
	errOut := multierr.Combine(errs...)

	if errOut != nil {
		log.Debug("executeParallel: 完成执行所有路由器,出错: ", errOut)
	}

	return errOut
}

// getChannelOrErrorParallel 并行获取通道或错误
// 参数:
//   - ctx: context.Context 上下文
//   - routers: []*ParallelRouter 路由器列表
//   - f: func(context.Context, routing.Routing) (<-chan T, error) 执行函数
//   - shouldStop: func() bool 是否应该停止的函数
//   - isSearchValue: bool 是否为搜索值
//
// 返回值:
//   - chan T 结果通道
//   - error 错误信息
func getChannelOrErrorParallel[T any](
	ctx context.Context,
	routers []*ParallelRouter,
	f func(context.Context, routing.Routing) (<-chan T, error),
	shouldStop func() bool, isSearchValue bool,
) (chan T, error) {
	// ready 是一个从锁定状态开始的互斥锁,它将保持锁定状态直到至少一个通道获得一个项目,这使我们只在准备好时返回
	var ready sync.Mutex
	ready.Lock()
	var resultsLk sync.Mutex
	var outCh chan T
	// nil 错误表示成功
	errors := []error{}

	ctx, cancelAll := context.WithCancel(ctx)
	fwg := jsync.NewFWaitGroup(func() {
		if outCh != nil {
			close(outCh)
		} else {
			ready.Unlock()
		}

		cancelAll()
	}, uint64(len(routers)))

	var blocking atomic.Uint64
	blocking.Add(1) // 从 1 开始,这样我们在分发时不会取消
	var sent atomic.Bool

	for i, r := range routers {
		ctx, span := tracer.StartSpan(ctx, composeName+".worker")
		isBlocking := !isSearchValue || !r.DoNotWaitForSearchValue
		if isBlocking {
			blocking.Add(1)
		}
		isRecording := span.IsRecording()
		if isRecording {
			span.SetAttributes(
				attribute.Bool("blocking", isBlocking),
				attribute.Stringer("type", reflect.TypeOf(r.Router)),
				attribute.Int("routingNumber", i),
			)
		}

		go func(r *ParallelRouter) {
			defer span.End()
			defer fwg.Done()
			defer func() {
				var remainingBlockers uint64
				if isSearchValue && r.DoNotWaitForSearchValue {
					remainingBlockers = blocking.Load()
				} else {
					var minusOne uint64
					minusOne--
					remainingBlockers = blocking.Add(minusOne)
				}

				if remainingBlockers == 0 && sent.Load() {
					cancelAll()
				}
			}()

			if r.ExecuteAfter != 0 {
				tim := time.NewTimer(r.ExecuteAfter)
				defer tim.Stop()
				select {
				case <-ctx.Done():
					return
				case <-tim.C:
					// 准备就绪
				}
			}

			ctx, cancel := withCancelAndOptionalTimeout(ctx, r.Timeout)
			defer cancel()

			valueChan, err := f(ctx, r.Router)
			if err != nil {
				if isRecording {
					span.SetStatus(codes.Error, err.Error())
				}

				if r.IgnoreError {
					return
				}

				resultsLk.Lock()
				defer resultsLk.Unlock()
				if errors == nil {
					return
				}
				errors = append(errors, err)

				return
			}
			if isRecording {
				span.AddEvent("开始流式传输")
			}

			for first := true; true; first = false {
				select {
				case <-ctx.Done():
					return
				case val, ok := <-valueChan:
					if !ok {
						return
					}
					if isRecording {
						span.AddEvent("获得结果")
					}

					if first {
						resultsLk.Lock()
						if outCh == nil {
							outCh = make(chan T)
							errors = nil
							ready.Unlock()
						}
						resultsLk.Unlock()
					}

					select {
					case <-ctx.Done():
						return
					case outCh <- val:
						sent.Store(true)
					}

					if shouldStop() {
						cancelAll()
						return
					}
				}
			}
		}(r)
	}

	// 移除分发计数并检查是否应该取消
	var minusOne uint64
	minusOne--
	if blocking.Add(minusOne) == 0 && sent.Load() {
		cancelAll()
	}

	ready.Lock()
	if outCh != nil {
		return outCh, nil
	} else if len(errors) == 0 {
		// 未找到任何内容
		ch := make(chan T)
		close(ch)
		return ch, nil
	} else {
		return nil, multierr.Combine(errors...)
	}
}
