package routinghelpers

import (
	"bytes"
	"context"
	"io"
	"reflect"
	"sync"

	"github.com/Jorropo/jsync"
	record "github.com/dep2p/kaddht/record"
	ci "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
)

// Parallel 并行操作路由器切片
type Parallel struct {
	Routers   []routing.Routing // 路由器切片
	Validator record.Validator  // 验证器
}

// supportsKey 检查路由器是否支持指定的键
// 参数:
//   - vs: routing.ValueStore 值存储接口
//   - key: string 键名
//
// 返回值:
//   - bool 是否支持该键
func supportsKey(vs routing.ValueStore, key string) bool {
	switch vs := vs.(type) {
	case Null:
		return false
	case *Compose:
		return vs.ValueStore != nil && supportsKey(vs.ValueStore, key)
	case Parallel:
		for _, ri := range vs.Routers {
			if supportsKey(ri, key) {
				return true
			}
		}
		return false
	case Tiered:
		for _, ri := range vs.Routers {
			if supportsKey(ri, key) {
				return true
			}
		}
		return false
	case *LimitedValueStore:
		return vs.KeySupported(key) && supportsKey(vs.ValueStore, key)
	default:
		return true
	}
}

// supportsPeer 检查路由器是否支持对等节点路由
// 参数:
//   - vs: routing.PeerRouting 对等节点路由接口
//
// 返回值:
//   - bool 是否支持对等节点路由
func supportsPeer(vs routing.PeerRouting) bool {
	switch vs := vs.(type) {
	case Null:
		return false
	case *Compose:
		return vs.PeerRouting != nil && supportsPeer(vs.PeerRouting)
	case Parallel:
		for _, ri := range vs.Routers {
			if supportsPeer(ri) {
				return true
			}
		}
		return false
	case Tiered:
		for _, ri := range vs.Routers {
			if supportsPeer(ri) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// supportsContent 检查路由器是否支持内容路由
// 参数:
//   - vs: routing.ContentRouting 内容路由接口
//
// 返回值:
//   - bool 是否支持内容路由
func supportsContent(vs routing.ContentRouting) bool {
	switch vs := vs.(type) {
	case Null:
		return false
	case *Compose:
		return vs.ContentRouting != nil && supportsContent(vs.ContentRouting)
	case Parallel:
		for _, ri := range vs.Routers {
			if supportsContent(ri) {
				return true
			}
		}
		return false
	case Tiered:
		for _, ri := range vs.Routers {
			if supportsContent(ri) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

// filter 根据过滤函数筛选路由器
// 参数:
//   - filter: func(routing.Routing) bool 过滤函数
//
// 返回值:
//   - Parallel 过滤后的并行路由器
func (r Parallel) filter(filter func(routing.Routing) bool) Parallel {
	cpy := make([]routing.Routing, 0, len(r.Routers))
	for _, ri := range r.Routers {
		if filter(ri) {
			cpy = append(cpy, ri)
		}
	}
	return Parallel{Routers: cpy, Validator: r.Validator}
}

// put 并行执行路由器操作
// 参数:
//   - do: func(routing.Routing) error 执行函数
//
// 返回值:
//   - error 错误信息
func (r Parallel) put(do func(routing.Routing) error) error {
	switch len(r.Routers) {
	case 0:
		return routing.ErrNotSupported
	case 1:
		return do(r.Routers[0])
	}

	var wg sync.WaitGroup
	results := make([]error, len(r.Routers))
	wg.Add(len(r.Routers))
	for i, ri := range r.Routers {
		go func(ri routing.Routing, i int) {
			results[i] = do(ri)
			wg.Done()
		}(ri, i)
	}
	wg.Wait()

	var (
		errs    []error
		success bool
	)
	for _, err := range results {
		switch err {
		case nil:
			// 至少有一个路由器支持此操作
			success = true
		case routing.ErrNotSupported:
		default:
			errs = append(errs, err)
		}
	}

	switch len(errs) {
	case 0:
		if success {
			// 没有错误且至少有一个路由器成功
			return nil
		}
		// 没有路由器支持此操作
		return routing.ErrNotSupported
	case 1:
		return errs[0]
	default:
		return &multierror.Error{Errors: errs}
	}
}

// search 并行搜索路由器
// 参数:
//   - ctx: context.Context 上下文
//   - do: func(routing.Routing) (<-chan []byte, error) 执行函数
//
// 返回值:
//   - <-chan []byte 结果通道
//   - error 错误信息
func (r Parallel) search(ctx context.Context, do func(routing.Routing) (<-chan []byte, error)) (<-chan []byte, error) {
	switch len(r.Routers) {
	case 0:
		return nil, routing.ErrNotFound
	case 1:
		return do(r.Routers[0])
	}

	ctx, cancel := context.WithCancel(ctx)

	out := make(chan []byte)

	fwg := jsync.NewFWaitGroup(func() {
		close(out)
		cancel()
	}, 1)
	for _, ri := range r.Routers {
		vchan, err := do(ri)
		if err != nil {
			continue
		}

		fwg.Add()
		go func() {
			var sent int
			defer fwg.Done()

			for {
				select {
				case v, ok := <-vchan:
					if !ok {
						if sent > 0 {
							cancel()
						}
						return
					}

					select {
					case out <- v:
						sent++
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	fwg.Done()

	return out, nil
}

// get 并行获取路由器结果
// 参数:
//   - ctx: context.Context 上下文
//   - do: func(routing.Routing) (interface{}, error) 执行函数
//
// 返回值:
//   - interface{} 结果
//   - error 错误信息
func (r Parallel) get(ctx context.Context, do func(routing.Routing) (interface{}, error)) (interface{}, error) {
	switch len(r.Routers) {
	case 0:
		return nil, routing.ErrNotFound
	case 1:
		return do(r.Routers[0])
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan struct {
		val interface{}
		err error
	})
	for _, ri := range r.Routers {
		go func(ri routing.Routing) {
			value, err := do(ri)
			select {
			case results <- struct {
				val interface{}
				err error
			}{
				val: value,
				err: err,
			}:
			case <-ctx.Done():
			}
		}(ri)
	}

	var errs []error
	for range r.Routers {
		select {
		case res := <-results:
			switch res.err {
			case nil:
				return res.val, nil
			case routing.ErrNotFound, routing.ErrNotSupported:
				continue
			}
			// 如果上下文已过期，仅返回该错误并忽略其他错误
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			errs = append(errs, res.err)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	switch len(errs) {
	case 0:
		return nil, routing.ErrNotFound
	case 1:
		return nil, errs[0]
	default:
		return nil, &multierror.Error{Errors: errs}
	}
}

// forKey 获取支持指定键的路由器
// 参数:
//   - key: string 键名
//
// 返回值:
//   - Parallel 支持该键的并行路由器
func (r Parallel) forKey(key string) Parallel {
	return r.filter(func(ri routing.Routing) bool {
		return supportsKey(ri, key)
	})
}

// mergeQueryEvents 限制仅在所有并行路由器失败时在上下文中发送 routing.QueryError 事件
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - context.Context 新的上下文
//   - context.CancelFunc 取消函数
func (r Parallel) mergeQueryEvents(ctx context.Context) (context.Context, context.CancelFunc) {
	subCtx, cancel := context.WithCancel(ctx)
	if !routing.SubscribesToQueryEvents(ctx) {
		return subCtx, cancel
	}

	subCtx, evCh := routing.RegisterForQueryEvents(subCtx)
	go func() {
		var errEvt *routing.QueryEvent
		successfulEvent := false
		for {
			select {
			// 注意：这是外部上下文
			// 在这种情况下可能会丢失错误事件，但由于超时关闭本质上在这方面是有竞争的
			case <-ctx.Done():
				return
			// evCh 将在 subCtx 取消时关闭
			case ev, ok := <-evCh:
				if !ok {
					if errEvt != nil && !successfulEvent {
						routing.PublishQueryEvent(ctx, errEvt)
					}
					return
				}
				if ev == nil {
					continue
				}
				if ev.Type == routing.QueryError {
					errEvt = ev
					continue
				}
				successfulEvent = true
				routing.PublishQueryEvent(ctx, ev)
			}
		}
	}()
	return subCtx, cancel
}

// PutValue 并行将给定的键值对存储到所有子路由器中
// 只要至少有一个子路由器成功，就视为成功，但会等待所有存储操作完成
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键名
//   - value: []byte 值
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - error 错误信息
func (r Parallel) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	reqCtx, cancel := r.mergeQueryEvents(ctx)
	defer cancel()
	err := r.forKey(key).put(func(ri routing.Routing) error {
		return ri.PutValue(reqCtx, key, value, opts...)
	})
	return err
}

// GetValue 在所有子路由器中搜索给定的键，返回第一个完成查询的子路由器的结果
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键名
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - []byte 值
//   - error 错误信息
func (r Parallel) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	reqCtx, cancel := r.mergeQueryEvents(ctx)
	defer cancel()
	vInt, err := r.forKey(key).get(reqCtx, func(ri routing.Routing) (interface{}, error) {
		return ri.GetValue(reqCtx, key, opts...)
	})
	val, _ := vInt.([]byte)
	return val, err
}

// SearchValue 并行在所有子路由器中搜索给定的键，返回所有子路由器按"新鲜度"单调递增的结果
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键名
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - <-chan []byte 结果通道
//   - error 错误信息
func (r Parallel) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	reqCtx, cancel := r.mergeQueryEvents(ctx)
	resCh, err := r.forKey(key).search(reqCtx, func(ri routing.Routing) (<-chan []byte, error) {
		return ri.SearchValue(reqCtx, key, opts...)
	})
	if err != nil {
		cancel()
		return nil, err
	}

	valid := make(chan []byte)
	var best []byte
	go func() {
		defer close(valid)
		defer cancel()

		for v := range resCh {
			if best != nil {
				n, err := r.Validator.Select(key, [][]byte{best, v})
				if err != nil {
					continue
				}
				if n != 1 {
					continue
				}
			}
			if bytes.Equal(best, v) && len(v) != 0 {
				continue
			}

			best = v
			select {
			case valid <- v:
			case <-ctx.Done():
				return
			}
		}
	}()

	return valid, err
}

// GetPublicKey 并行从所有子路由器中检索公钥，返回第一个结果
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (r Parallel) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	vInt, err := r.
		forKey(routing.KeyForPublicKey(p)).
		get(ctx, func(ri routing.Routing) (interface{}, error) {
			return routing.GetPublicKey(ri, ctx, p)
		})
	val, _ := vInt.(ci.PubKey)
	return val, err
}

// FindPeer 并行在所有子路由器中查找给定的对等节点，返回第一个结果
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点地址信息
//   - error 错误信息
func (r Parallel) FindPeer(ctx context.Context, p peer.ID) (peer.AddrInfo, error) {
	reqCtx, cancel := r.mergeQueryEvents(ctx)
	defer cancel()
	vInt, err := r.filter(func(ri routing.Routing) bool {
		return supportsPeer(ri)
	}).get(ctx, func(ri routing.Routing) (interface{}, error) {
		return ri.FindPeer(reqCtx, p)
	})
	pi, _ := vInt.(peer.AddrInfo)
	return pi, err
}

// Provide 并行向所有子路由器宣告此对等节点提供指定的内容
// 只要有一个子路由器成功，就视为成功，但仍会等待所有子路由器完成后再返回
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - local: bool 是否为本地提供
//
// 返回值:
//   - error 错误信息
func (r Parallel) Provide(ctx context.Context, c cid.Cid, local bool) error {
	return r.filter(func(ri routing.Routing) bool {
		return supportsContent(ri)
	}).put(func(ri routing.Routing) error {
		return ri.Provide(ctx, c, local)
	})
}

// FindProvidersAsync 并行在所有子路由器中搜索能够提供给定键的对等节点
// 如果 count > 0，最多返回 count 个提供者。如果 count == 0，返回无限数量的提供者
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - count: int 最大提供者数量
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者通道
func (r Parallel) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	routers := r.filter(func(ri routing.Routing) bool {
		return supportsContent(ri)
	})

	switch len(routers.Routers) {
	case 0:
		ch := make(chan peer.AddrInfo)
		close(ch)
		return ch
	case 1:
		return routers.Routers[0].FindProvidersAsync(ctx, c, count)
	}

	out := make(chan peer.AddrInfo)

	reqCtx, cancel := r.mergeQueryEvents(ctx)

	providers := make([]<-chan peer.AddrInfo, len(routers.Routers))
	for i, ri := range routers.Routers {
		providers[i] = ri.FindProvidersAsync(reqCtx, c, count)
	}

	go func() {
		defer cancel()
		defer close(out)
		if len(providers) > 8 {
			manyProviders(reqCtx, out, providers, count)
		} else {
			fewProviders(reqCtx, out, providers, count)
		}
	}()
	return out
}

// manyProviders 处理多个提供者的情况（未优化）
// 使用反射实现虽然较慢但更简单。如果并行运行的对等节点路由器超过8个，可以重新考虑此实现
// 参数:
//   - ctx: context.Context 上下文
//   - out: chan<- peer.AddrInfo 输出通道
//   - in: []<-chan peer.AddrInfo 输入通道切片
//   - count: int 最大提供者数量
func manyProviders(ctx context.Context, out chan<- peer.AddrInfo, in []<-chan peer.AddrInfo, count int) {
	found := make(map[peer.ID]struct{}, count)

	selectCases := make([]reflect.SelectCase, len(in))
	for i, ch := range in {
		selectCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ch),
		}
	}

	// 如果请求0个提供者，表示获取所有提供者
	if count == 0 {
		count = -1
	}

	for count != 0 && len(selectCases) > 0 {
		chosen, val, ok := reflect.Select(selectCases)
		if !ok {
			// 移除通道
			selectCases[chosen] = selectCases[len(selectCases)-1]
			selectCases = selectCases[:len(selectCases)-1]
			continue
		}

		pi := val.Interface().(peer.AddrInfo)
		if _, ok := found[pi.ID]; ok {
			continue
		}

		select {
		case out <- pi:
			found[pi.ID] = struct{}{}
			count--
		case <-ctx.Done():
			return
		}
	}
}

// fewProviders 处理少量提供者的优化情况（<=8）
// 参数:
//   - ctx: context.Context 上下文
//   - out: chan<- peer.AddrInfo 输出通道
//   - in: []<-chan peer.AddrInfo 输入通道切片
//   - count: int 最大提供者数量
func fewProviders(ctx context.Context, out chan<- peer.AddrInfo, in []<-chan peer.AddrInfo, count int) {
	if len(in) > 8 {
		panic("仅适用于合并少于8个通道的情况")
	}

	found := make(map[peer.ID]struct{}, count)

	cases := make([]<-chan peer.AddrInfo, 8)
	copy(cases, in)

	// 如果请求0个提供者，表示获取所有提供者
	if count == 0 {
		count = -1
	}

	// Go语言，没有你我们该怎么办！
	nch := len(in)
	var pi peer.AddrInfo
	for nch > 0 && count != 0 {
		var ok bool
		var selected int
		select {
		case pi, ok = <-cases[0]:
			selected = 0
		case pi, ok = <-cases[1]:
			selected = 1
		case pi, ok = <-cases[2]:
			selected = 2
		case pi, ok = <-cases[3]:
			selected = 3
		case pi, ok = <-cases[4]:
			selected = 4
		case pi, ok = <-cases[5]:
			selected = 5
		case pi, ok = <-cases[6]:
			selected = 6
		case pi, ok = <-cases[7]:
			selected = 7
		}
		if !ok {
			cases[selected] = nil
			nch--
			continue
		}
		if _, ok = found[pi.ID]; ok {
			continue
		}

		select {
		case out <- pi:
			found[pi.ID] = struct{}{}
			count--
		case <-ctx.Done():
			return
		}
	}
}

// Bootstrap 通知所有子路由器进行引导
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (r Parallel) Bootstrap(ctx context.Context) error {
	var me multierror.Error
	for _, b := range r.Routers {
		if err := b.Bootstrap(ctx); err != nil {
			me.Errors = append(me.Errors, err)
		}
	}
	return me.ErrorOrNil()
}

// Close 关闭所有实现了 io.Closer 接口的子路由器
// 返回值:
//   - error 错误信息
func (r Parallel) Close() error {
	var me multierror.Error
	for _, router := range r.Routers {
		if closer, ok := router.(io.Closer); ok {
			if err := closer.Close(); err != nil {
				me.Errors = append(me.Errors, err)
			}
		}
	}
	return me.ErrorOrNil()
}

var _ routing.Routing = Parallel{}
