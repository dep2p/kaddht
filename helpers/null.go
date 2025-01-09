package routinghelpers

import (
	"context"

	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/ipfs/go-cid"
)

// Null 是一个不执行任何操作的路由器
type Null struct{}

// PutValue 总是返回 ErrNotSupported
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - value: []byte 值
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - error 错误信息
func (nr Null) PutValue(context.Context, string, []byte, ...routing.Option) error {
	return routing.ErrNotSupported
}

// GetValue 总是返回 ErrNotFound
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - []byte 值
//   - error 错误信息
func (nr Null) GetValue(context.Context, string, ...routing.Option) ([]byte, error) {
	return nil, routing.ErrNotFound
}

// SearchValue 总是返回 ErrNotFound
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - <-chan []byte 结果通道
//   - error 错误信息
func (nr Null) SearchValue(ctx context.Context, key string, opts ...routing.Option) (<-chan []byte, error) {
	return nil, routing.ErrNotFound
}

// Provide 总是返回 ErrNotSupported
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - provide: bool 是否提供
//
// 返回值:
//   - error 错误信息
func (nr Null) Provide(context.Context, cid.Cid, bool) error {
	return routing.ErrNotSupported
}

// FindProvidersAsync 总是返回一个已关闭的通道
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 内容标识符
//   - count: int 最大提供者数量
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者信息通道
func (nr Null) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	close(ch)
	return ch
}

// FindPeer 总是返回 ErrNotFound
// 参数:
//   - ctx: context.Context 上下文
//   - id: peer.ID 对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点信息
//   - error 错误信息
func (nr Null) FindPeer(context.Context, peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, routing.ErrNotFound
}

// Bootstrap 总是立即成功返回
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (nr Null) Bootstrap(context.Context) error {
	return nil
}

// Close 总是立即成功返回
// 返回值:
//   - error 错误信息
func (nr Null) Close() error {
	return nil
}

var _ routing.Routing = Null{}
