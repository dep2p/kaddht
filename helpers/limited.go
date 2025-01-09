package routinghelpers

import (
	"context"
	"io"
	"strings"

	ci "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
)

// LimitedValueStore 限制内部值存储到指定的命名空间
type LimitedValueStore struct {
	routing.ValueStore
	Namespaces []string
}

// GetPublicKey 获取指定对等节点的公钥
// 仅当路由器支持/pk命名空间时才返回公钥,否则返回routing.ErrNotFound
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (lvs *LimitedValueStore) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	for _, ns := range lvs.Namespaces {
		if ns == "pk" {
			return routing.GetPublicKey(lvs.ValueStore, ctx, p)
		}
	}
	return nil, routing.ErrNotFound
}

// PutValue 将给定的键值对存储到底层值存储中
// 仅当命名空间受支持时才存储,否则返回routing.ErrNotSupported
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - value: []byte 值
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - error 错误信息
func (lvs *LimitedValueStore) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) error {
	if !lvs.KeySupported(key) {
		return routing.ErrNotSupported
	}
	return lvs.ValueStore.PutValue(ctx, key, value, opts...)
}

// KeySupported 检查给定的键是否被此值存储支持
// 参数:
//   - key: string 键
//
// 返回值:
//   - bool 是否支持
func (lvs *LimitedValueStore) KeySupported(key string) bool {
	if len(key) < 3 {
		return false
	}
	if key[0] != '/' {
		return false
	}
	key = key[1:]
	for _, ns := range lvs.Namespaces {
		if len(ns) < len(key) && strings.HasPrefix(key, ns) && key[len(ns)] == '/' {
			return true
		}
	}
	return false
}

// GetValue 从底层值存储中获取给定键的值
// 仅当命名空间受支持时才获取,否则返回routing.ErrNotFound
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - []byte 值
//   - error 错误信息
func (lvs *LimitedValueStore) GetValue(ctx context.Context, key string, opts ...routing.Option) ([]byte, error) {
	if !lvs.KeySupported(key) {
		return nil, routing.ErrNotFound
	}
	return lvs.ValueStore.GetValue(ctx, key, opts...)
}

// SearchValue 在底层值存储中搜索给定的键
// 仅当命名空间受支持时才搜索,否则返回一个空的已关闭通道表示未找到值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - <-chan []byte 结果通道
//   - error 错误信息
func (lvs *LimitedValueStore) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	if !lvs.KeySupported(key) {
		out := make(chan []byte)
		close(out)
		return out, nil
	}
	return lvs.ValueStore.SearchValue(ctx, key, opts...)
}

// Bootstrap 使底层值存储进入"已引导"状态
// 仅当它实现了Bootstrap接口时才执行
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (lvs *LimitedValueStore) Bootstrap(ctx context.Context) error {
	if bs, ok := lvs.ValueStore.(Bootstrap); ok {
		return bs.Bootstrap(ctx)
	}
	return nil
}

// Close 关闭底层值存储
// 仅当它实现了io.Closer接口时才执行
// 返回值:
//   - error 错误信息
func (lvs *LimitedValueStore) Close() error {
	if closer, ok := lvs.ValueStore.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

var _ routing.PubKeyFetcher = (*LimitedValueStore)(nil)
var _ routing.ValueStore = (*LimitedValueStore)(nil)
var _ Bootstrap = (*LimitedValueStore)(nil)
