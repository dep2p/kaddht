package routinghelpers

import (
	"context"
	"io"

	record "github.com/dep2p/kaddht/record"
	ci "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
)

// Tiered 类似于 Parallel，但 GetValue 和 FindPeer 是按顺序调用的。
type Tiered struct {
	Routers   []routing.Routing
	Validator record.Validator
}

// PutValue 并行地将给定的键值对写入所有子路由器。
// 只要至少有一个子路由器成功写入就算成功，但会等待所有写入操作完成后才返回。
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - value: []byte 值
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - error 错误信息
func (r Tiered) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	return Parallel{Routers: r.Routers}.PutValue(ctx, key, value, opts...)
}

// get 从子路由器中获取值的内部方法
// 参数:
//   - ctx: context.Context 上下文
//   - do: func(routing.Routing) (interface{}, error) 执行函数
//
// 返回值:
//   - interface{} 获取的值
//   - error 错误信息
func (r Tiered) get(ctx context.Context, do func(routing.Routing) (interface{}, error)) (interface{}, error) {
	var errs []error
	for _, ri := range r.Routers {
		val, err := do(ri)
		switch err {
		case nil:
			return val, nil
		case routing.ErrNotFound, routing.ErrNotSupported:
			continue
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		errs = append(errs, err)
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

// GetValue 按顺序在每个子路由器中搜索给定的键，返回第一个完成查询的子路由器的值。
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - []byte 获取的值
//   - error 错误信息
func (r Tiered) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	valInt, err := r.get(ctx, func(ri routing.Routing) (interface{}, error) {
		return ri.GetValue(ctx, key, opts...)
	})
	val, _ := valInt.([]byte)
	return val, err
}

// SearchValue 并行地在所有子路由器中搜索给定的键，从所有子路由器返回按"新鲜度"单调递增的结果。
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - <-chan []byte 值通道
//   - error 错误信息
func (r Tiered) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	return Parallel(r).SearchValue(ctx, key, opts...)
}

// GetPublicKey 按顺序在每个子路由器中搜索公钥，返回第一个结果。
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (r Tiered) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	vInt, err := r.get(ctx, func(ri routing.Routing) (interface{}, error) {
		return routing.GetPublicKey(ri, ctx, p)
	})
	val, _ := vInt.(ci.PubKey)
	return val, err
}

// Provide 向所有子路由器并行地宣告此对等节点提供指定的内容。
// 只要有一个子路由器成功就返回成功，但会等待所有子路由器完成后才返回。
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - local: bool 是否本地提供
//
// 返回值:
//   - error 错误信息
func (r Tiered) Provide(ctx context.Context, c cid.Cid, local bool) error {
	return Parallel{Routers: r.Routers}.Provide(ctx, c, local)
}

// FindProvidersAsync 并行地在所有子路由器中搜索能够提供给定键的对等节点。
//
// 如果 count > 0，最多返回 count 个提供者。如果 count == 0，返回无限数量的提供者。
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - count: int 提供者数量限制
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者地址信息通道
func (r Tiered) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	return Parallel{Routers: r.Routers}.FindProvidersAsync(ctx, c, count)
}

// FindPeer 按顺序使用每个子路由器搜索给定的对等节点，返回第一个结果。
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点地址信息
//   - error 错误信息
func (r Tiered) FindPeer(ctx context.Context, p peer.ID) (peer.AddrInfo, error) {
	valInt, err := r.get(ctx, func(ri routing.Routing) (interface{}, error) {
		return ri.FindPeer(ctx, p)
	})
	val, _ := valInt.(peer.AddrInfo)
	return val, err
}

// Bootstrap 通知所有子路由器进行引导。
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (r Tiered) Bootstrap(ctx context.Context) error {
	return Parallel{Routers: r.Routers}.Bootstrap(ctx)
}

// Close 关闭所有实现了 io.Closer 接口的子路由器。
// 返回值:
//   - error 错误信息
func (r Tiered) Close() error {
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

var _ routing.Routing = Tiered{}
