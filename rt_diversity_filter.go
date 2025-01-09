package dht

import (
	"sync"

	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/peer"

	"github.com/dep2p/kaddht/kbucket/peerdiversity"

	ma "github.com/multiformats/go-multiaddr"
)

// 确保 rtPeerIPGroupFilter 实现了 PeerIPGroupFilter 接口
var _ peerdiversity.PeerIPGroupFilter = (*rtPeerIPGroupFilter)(nil)

// rtPeerIPGroupFilter 实现了对等节点IP组过滤器
type rtPeerIPGroupFilter struct {
	mu sync.RWMutex // 互斥锁
	h  host.Host    // libp2p主机实例

	maxPerCpl   int // 每个CPL(公共前缀长度)允许的最大IP组数
	maxForTable int // 路由表允许的最大IP组数

	cplIpGroupCount   map[int]map[peerdiversity.PeerIPGroupKey]int // 每个CPL的IP组计数
	tableIpGroupCount map[peerdiversity.PeerIPGroupKey]int         // 整个路由表的IP组计数
}

// NewRTPeerDiversityFilter 构造一个用于路由表的对等节点IP组过滤器
// 参数:
//   - h: host.Host libp2p主机实例
//   - maxPerCpl: int 每个CPL允许的最大IP组数
//   - maxForTable: int 路由表允许的最大IP组数
//
// 返回值:
//   - *rtPeerIPGroupFilter 新创建的过滤器实例
func NewRTPeerDiversityFilter(h host.Host, maxPerCpl, maxForTable int) *rtPeerIPGroupFilter {
	return &rtPeerIPGroupFilter{
		h: h,

		maxPerCpl:   maxPerCpl,
		maxForTable: maxForTable,

		cplIpGroupCount:   make(map[int]map[peerdiversity.PeerIPGroupKey]int),
		tableIpGroupCount: make(map[peerdiversity.PeerIPGroupKey]int),
	}
}

// Allow 检查是否允许添加新的对等节点组
// 参数:
//   - g: peerdiversity.PeerGroupInfo 对等节点组信息
//
// 返回值:
//   - bool 如果允许添加则返回true,否则返回false
func (r *rtPeerIPGroupFilter) Allow(g peerdiversity.PeerGroupInfo) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	// 检查整个路由表的IP组数量限制
	if r.tableIpGroupCount[key] >= r.maxForTable {
		return false
	}

	// 检查特定CPL的IP组数量限制
	c, ok := r.cplIpGroupCount[cpl]
	allow := !ok || c[key] < r.maxPerCpl
	return allow
}

// Increment 增加对等节点组的计数
// 参数:
//   - g: peerdiversity.PeerGroupInfo 对等节点组信息
//
// 返回值:
//   - 无
func (r *rtPeerIPGroupFilter) Increment(g peerdiversity.PeerGroupInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	// 增加路由表中的IP组计数
	r.tableIpGroupCount[key] = r.tableIpGroupCount[key] + 1
	if _, ok := r.cplIpGroupCount[cpl]; !ok {
		r.cplIpGroupCount[cpl] = make(map[peerdiversity.PeerIPGroupKey]int)
	}

	// 增加特定CPL的IP组计数
	r.cplIpGroupCount[cpl][key] = r.cplIpGroupCount[cpl][key] + 1
}

// Decrement 减少对等节点组的计数
// 参数:
//   - g: peerdiversity.PeerGroupInfo 对等节点组信息
//
// 返回值:
//   - 无
func (r *rtPeerIPGroupFilter) Decrement(g peerdiversity.PeerGroupInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := g.IPGroupKey
	cpl := g.Cpl

	// 减少路由表中的IP组计数
	r.tableIpGroupCount[key] = r.tableIpGroupCount[key] - 1
	if r.tableIpGroupCount[key] == 0 {
		delete(r.tableIpGroupCount, key)
	}

	// 减少特定CPL的IP组计数
	r.cplIpGroupCount[cpl][key] = r.cplIpGroupCount[cpl][key] - 1
	if r.cplIpGroupCount[cpl][key] == 0 {
		delete(r.cplIpGroupCount[cpl], key)
	}
	if len(r.cplIpGroupCount[cpl]) == 0 {
		delete(r.cplIpGroupCount, cpl)
	}
}

// PeerAddresses 获取指定对等节点的所有多地址
// 参数:
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - []ma.Multiaddr 对等节点的多地址列表
func (r *rtPeerIPGroupFilter) PeerAddresses(p peer.ID) []ma.Multiaddr {
	// 获取与对等节点的所有连接
	cs := r.h.Network().ConnsToPeer(p)
	addr := make([]ma.Multiaddr, 0, len(cs))
	// 从每个连接中获取远程多地址
	for _, c := range cs {
		addr = append(addr, c.RemoteMultiaddr())
	}
	return addr
}
