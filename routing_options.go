package dht

import (
	internalConfig "github.com/dep2p/kaddht/internal/config"
	"github.com/dep2p/libp2p/core/routing"
)

// Quorum 设置DHT查询需要从多少个对等节点获取值后才返回最佳值
// 当值为0时,表示DHT查询应该完整执行而不是提前返回
// 默认值: 0
//
// 参数:
//   - n: int 需要查询的对等节点数量
//
// 返回值:
//   - routing.Option 路由选项
func Quorum(n int) routing.Option {
	return func(opts *routing.Options) error {
		if opts.Other == nil {
			opts.Other = make(map[interface{}]interface{}, 1)
		}
		opts.Other[internalConfig.QuorumOptionKey{}] = n
		return nil
	}
}
