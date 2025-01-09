package config

import "github.com/dep2p/libp2p/core/routing"

type QuorumOptionKey struct{}

const defaultQuorum = 0

// GetQuorum 获取所需响应数,如果未找到选项则默认为0
// 参数:
//   - opts: *routing.Options 路由选项
//
// 返回值:
//   - int 所需响应数
func GetQuorum(opts *routing.Options) int {
	responsesNeeded, ok := opts.Other[QuorumOptionKey{}].(int)
	if !ok {
		responsesNeeded = defaultQuorum
	}
	return responsesNeeded
}
