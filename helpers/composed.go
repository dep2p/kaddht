package routinghelpers

import (
	"context"

	ci "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
)

// Compose 将组件组合成单个路由器。未指定组件(保留为nil)等同于指定Null路由器。
//
// 它还实现了Bootstrap接口。所有实现Bootstrap的不同组件将并行引导。
// 相同的组件不会被引导两次。
type Compose struct {
	ValueStore     routing.ValueStore
	PeerRouting    routing.PeerRouting
	ContentRouting routing.ContentRouting
}

const composeName = "Compose"

// 注意:我们显式实现这些方法以避免在不想实现某些功能的地方手动指定Null路由器。

// PutValue 添加与给定键对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - value: []byte 值
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - error 错误信息
func (cr *Compose) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(composeName, ctx, key, value, opts...)
	defer func() { end(err) }()

	if cr.ValueStore == nil {
		return routing.ErrNotSupported
	}
	return cr.ValueStore.PutValue(ctx, key, value, opts...)
}

// GetValue 搜索与给定键对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - []byte 值
//   - error 错误信息
func (cr *Compose) GetValue(ctx context.Context, key string, opts ...routing.Option) (value []byte, err error) {
	ctx, end := tracer.GetValue(composeName, ctx, key, opts...)
	defer func() { end(value, err) }()

	if cr.ValueStore == nil {
		return nil, routing.ErrNotFound
	}
	return cr.ValueStore.GetValue(ctx, key, opts...)
}

// SearchValue 搜索与给定键对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - <-chan []byte 值通道
//   - error 错误信息
func (cr *Compose) SearchValue(ctx context.Context, key string, opts ...routing.Option) (ch <-chan []byte, err error) {
	ctx, wrapper := tracer.SearchValue(composeName, ctx, key, opts...)
	defer func() { ch, err = wrapper(ch, err) }()

	if cr.ValueStore == nil {
		out := make(chan []byte)
		close(out)
		return out, nil
	}
	return cr.ValueStore.SearchValue(ctx, key, opts...)
}

// Provide 将给定的CID添加到内容路由系统中
// 如果传入'true',它还会公告它,否则仅保留在本地记录中
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - local: bool 是否本地提供
//
// 返回值:
//   - error 错误信息
func (cr *Compose) Provide(ctx context.Context, c cid.Cid, local bool) (err error) {
	ctx, end := tracer.Provide(composeName, ctx, c, local)
	defer func() { end(err) }()

	if cr.ContentRouting == nil {
		return routing.ErrNotSupported
	}
	return cr.ContentRouting.Provide(ctx, c, local)
}

// FindProvidersAsync 搜索能够提供给定键的对等节点
//
// 如果count > 0,它最多返回count个提供者。如果count == 0,它返回无限数量的提供者。
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - count: int 最大提供者数量
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者地址信息通道
func (cr *Compose) FindProvidersAsync(ctx context.Context, c cid.Cid, count int) <-chan peer.AddrInfo {
	ctx, wrapper := tracer.FindProvidersAsync(composeName, ctx, c, count)

	if cr.ContentRouting == nil {
		ch := make(chan peer.AddrInfo)
		close(ch)
		return wrapper(ch, routing.ErrNotFound)
	}
	return wrapper(cr.ContentRouting.FindProvidersAsync(ctx, c, count), nil)
}

// FindPeer 搜索具有给定ID的对等节点,返回包含相关地址的peer.AddrInfo
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点地址信息
//   - error 错误信息
func (cr *Compose) FindPeer(ctx context.Context, p peer.ID) (info peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(composeName, ctx, p)
	defer func() { end(info, err) }()

	if cr.PeerRouting == nil {
		return peer.AddrInfo{}, routing.ErrNotFound
	}
	return cr.PeerRouting.FindPeer(ctx, p)
}

// GetPublicKey 返回给定对等节点的公钥
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (cr *Compose) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	if cr.ValueStore == nil {
		return nil, routing.ErrNotFound
	}
	return routing.GetPublicKey(cr.ValueStore, ctx, p)
}

// Bootstrap 引导路由器
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (cr *Compose) Bootstrap(ctx context.Context) (err error) {
	ctx, end := tracer.Bootstrap(composeName, ctx)
	defer func() { end(err) }()

	// 去重。从技术上讲,多次调用bootstrap不应该有问题,但在Compose的多个字段中使用相同的路由器是很常见的。
	routers := make(map[Bootstrap]struct{}, 3)
	for _, value := range [...]interface{}{
		cr.ValueStore,
		cr.ContentRouting,
		cr.PeerRouting,
	} {
		switch b := value.(type) {
		case nil:
		case Null:
		case Bootstrap:
			routers[b] = struct{}{}
		}
	}

	var me multierror.Error
	for b := range routers {
		if err := b.Bootstrap(ctx); err != nil {
			me.Errors = append(me.Errors, err)
		}
	}
	return me.ErrorOrNil()
}

var _ routing.Routing = (*Compose)(nil)
var _ routing.PubKeyFetcher = (*Compose)(nil)
