package providers

import (
	"time"

	"github.com/dep2p/libp2p/core/peer"
)

// providerSet 包含提供者列表及其添加时间
// 它作为数据存储和 GetProviders 调用消费者之间的中间数据结构
type providerSet struct {
	providers []peer.ID
	set       map[peer.ID]time.Time
}

// newProviderSet 创建新的提供者集合
// 返回值:
//   - *providerSet 新的提供者集合
func newProviderSet() *providerSet {
	return &providerSet{
		set: make(map[peer.ID]time.Time),
	}
}

// Add 添加提供者
// 参数:
//   - p: peer.ID 提供者ID
func (ps *providerSet) Add(p peer.ID) {
	ps.setVal(p, time.Now())
}

// setVal 设置提供者及其时间戳
// 参数:
//   - p: peer.ID 提供者ID
//   - t: time.Time 时间戳
func (ps *providerSet) setVal(p peer.ID, t time.Time) {
	_, found := ps.set[p]
	if !found {
		ps.providers = append(ps.providers, p)
	}

	ps.set[p] = t
}
