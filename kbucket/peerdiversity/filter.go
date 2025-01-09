package peerdiversity

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"

	"github.com/dep2p/libp2p/core/peer"

	"github.com/libp2p/go-cidranger"
	asnutil "github.com/libp2p/go-libp2p-asn-util"

	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

var dfLog = logging.Logger("diversityFilter")

// PeerIPGroupKey 是代表对等节点所属的一个IP组的唯一键。
// 一个对等节点每个地址都有一个 PeerIPGroupKey。因此,如果一个对等节点有多个地址,它可以属于多个组。
// 目前,给定一个对等节点地址,我们的分组机制如下:
//  1. 对于IPv6地址,我们按IP地址的ASN进行分组。
//  2. 对于IPv4地址,所有属于相同传统(Class A)/8分配或共享相同/16前缀的地址都在同一组中。
type PeerIPGroupKey string

// https://en.wikipedia.org/wiki/List_of_assigned_/8_IPv4_address_blocks
var legacyClassA = []string{"12.0.0.0/8", "17.0.0.0/8", "19.0.0.0/8", "38.0.0.0/8", "48.0.0.0/8", "56.0.0.0/8", "73.0.0.0/8", "53.0.0.0/8"}

// PeerGroupInfo 表示对等节点的分组信息
type PeerGroupInfo struct {
	Id         peer.ID
	Cpl        int
	IPGroupKey PeerIPGroupKey
}

// PeerIPGroupFilter 是想要实例化 peerdiversity.Filter 的调用者必须实现的接口。
// 该接口提供了被 peerdiversity.Filter 使用/调用的函数钩子。
type PeerIPGroupFilter interface {
	// Allow 被 Filter 调用以测试具有给定分组信息的对等节点是否应该被 Filter 允许/拒绝。
	// 这将仅在对等节点成功通过所有 Filter 的内部检查后调用。
	// 注意:如果对等节点在 Filter 上被列入白名单,则不会调用此函数就允许该对等节点。
	Allow(PeerGroupInfo) (allow bool)

	// Increment 在具有给定分组信息的对等节点被添加到 Filter 状态时被调用。
	// 这将在对等节点通过所有 Filter 的内部检查和上面定义的所有组的 Allow 函数后发生。
	Increment(PeerGroupInfo)

	// Decrement 在具有给定分组信息的对等节点从 Filter 中移除时被调用。
	// 当 Filter 的调用者/用户不再希望对等节点及其所属的 IP 组计入 Filter 状态时,这将发生。
	Decrement(PeerGroupInfo)

	// PeerAddresses 被 Filter 调用以确定它应该使用给定对等节点的哪些地址来确定它所属的 IP 组。
	PeerAddresses(peer.ID) []ma.Multiaddr
}

// Filter 是一个对等节点多样性过滤器,它根据配置的白名单规则和由传递给它的 PeerIPGroupFilter 接口实现定义的多样性策略来接受或拒绝对等节点。
type Filter struct {
	mu sync.Mutex
	// PeerIPGroupFilter 接口的实现
	pgm        PeerIPGroupFilter
	peerGroups map[peer.ID][]PeerGroupInfo

	// 白名单对等节点
	wlpeers map[peer.ID]struct{}

	// 传统 IPv4 Class A 网络
	legacyCidrs cidranger.Ranger

	logKey string

	cplFnc func(peer.ID) int

	cplPeerGroups map[int]map[peer.ID][]PeerIPGroupKey
}

// NewFilter 创建一个对等节点多样性过滤器
// 参数:
//   - pgm: PeerIPGroupFilter 对等节点组过滤器实现
//   - logKey: string 日志键
//   - cplFnc: func(peer.ID) int CPL计算函数
//
// 返回值:
//   - *Filter 过滤器实例
//   - error 错误信息
func NewFilter(pgm PeerIPGroupFilter, logKey string, cplFnc func(peer.ID) int) (*Filter, error) {
	if pgm == nil {
		return nil, errors.New("对等节点组实现不能为空")
	}

	// 为传统 Class N 网络创建 Trie
	legacyCidrs := cidranger.NewPCTrieRanger()
	for _, cidr := range legacyClassA {
		_, nn, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, err
		}
		if err := legacyCidrs.Insert(cidranger.NewBasicRangerEntry(*nn)); err != nil {
			return nil, err
		}
	}

	return &Filter{
		pgm:           pgm,
		peerGroups:    make(map[peer.ID][]PeerGroupInfo),
		wlpeers:       make(map[peer.ID]struct{}),
		legacyCidrs:   legacyCidrs,
		logKey:        logKey,
		cplFnc:        cplFnc,
		cplPeerGroups: make(map[int]map[peer.ID][]PeerIPGroupKey),
	}, nil
}

// Remove 从过滤器中移除对等节点
// 参数:
//   - p: peer.ID 要移除的对等节点ID
func (f *Filter) Remove(p peer.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	cpl := f.cplFnc(p)

	for _, info := range f.peerGroups[p] {
		f.pgm.Decrement(info)
	}
	f.peerGroups[p] = nil
	delete(f.peerGroups, p)
	delete(f.cplPeerGroups[cpl], p)

	if len(f.cplPeerGroups[cpl]) == 0 {
		delete(f.cplPeerGroups, cpl)
	}
}

// TryAdd 尝试将对等节点添加到过滤器状态,如果成功则返回true,否则返回false
// 参数:
//   - p: peer.ID 要添加的对等节点ID
//
// 返回值:
//   - bool 是否添加成功
func (f *Filter) TryAdd(p peer.ID) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.wlpeers[p]; ok {
		return true
	}

	cpl := f.cplFnc(p)

	// 不允许我们无法确定地址的对等节点
	addrs := f.pgm.PeerAddresses(p)
	if len(addrs) == 0 {
		dfLog.Debugw("未找到对等节点的地址", "appKey", f.logKey, "peer", p)
		return false
	}

	peerGroups := make([]PeerGroupInfo, 0, len(addrs))
	for _, a := range addrs {
		ip, err := manet.ToIP(a)
		if err != nil {
			dfLog.Errorw("从多地址解析IP失败", "appKey", f.logKey,
				"multiaddr", a.String(), "err", err)
			return false
		}

		// 如果我们无法确定其中一个地址的分组,则拒绝该对等节点
		key := f.ipGroupKey(ip)
		if len(key) == 0 {
			dfLog.Errorw("组键为空", "appKey", f.logKey, "ip", ip.String(), "peer", p)
			return false
		}
		group := PeerGroupInfo{Id: p, Cpl: cpl, IPGroupKey: key}

		if !f.pgm.Allow(group) {
			return false
		}

		peerGroups = append(peerGroups, group)
	}

	if _, ok := f.cplPeerGroups[cpl]; !ok {
		f.cplPeerGroups[cpl] = make(map[peer.ID][]PeerIPGroupKey)
	}

	for _, g := range peerGroups {
		f.pgm.Increment(g)

		f.peerGroups[p] = append(f.peerGroups[p], g)
		f.cplPeerGroups[cpl][p] = append(f.cplPeerGroups[cpl][p], g.IPGroupKey)
	}

	return true
}

// WhitelistPeers 将始终允许给定的对等节点
// 参数:
//   - peers: ...peer.ID 要加入白名单的对等节点ID列表
func (f *Filter) WhitelistPeers(peers ...peer.ID) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, p := range peers {
		f.wlpeers[p] = struct{}{}
	}
}

// ipGroupKey 返回给定IP所属的PeerIPGroupKey
// 参数:
//   - ip: net.IP IP地址
//
// 返回值:
//   - PeerIPGroupKey IP组键
func (f *Filter) ipGroupKey(ip net.IP) PeerIPGroupKey {
	switch bz := ip.To4(); bz {
	case nil:
		// ipv6地址 -> 获取ASN
		s := asnutil.AsnForIPv6(ip)

		// 如果未找到ASN,则回退到使用/32前缀
		if s == 0 {
			dfLog.Debugw("ASN未知", "appKey", f.logKey, "ip", ip)
			return PeerIPGroupKey(fmt.Sprintf("unknown ASN: %s", net.CIDRMask(32, 128).String()))
		}

		return PeerIPGroupKey(strconv.FormatUint(uint64(s), 10))
	default:
		// 如果属于传统Class 8,我们返回/8前缀作为键
		rs, _ := f.legacyCidrs.ContainingNetworks(ip)
		if len(rs) != 0 {
			key := ip.Mask(net.IPv4Mask(255, 0, 0, 0)).String()
			return PeerIPGroupKey(key)
		}

		// 否则 -> /16前缀
		key := ip.Mask(net.IPv4Mask(255, 255, 0, 0)).String()
		return PeerIPGroupKey(key)
	}
}

// CplDiversityStats 包含CPL的对等节点多样性统计信息
type CplDiversityStats struct {
	Cpl   int
	Peers map[peer.ID][]PeerIPGroupKey
}

// GetDiversityStats 返回每个CPL的多样性统计信息,并按CPL排序
// 返回值:
//   - []CplDiversityStats CPL多样性统计信息列表
func (f *Filter) GetDiversityStats() []CplDiversityStats {
	f.mu.Lock()
	defer f.mu.Unlock()

	stats := make([]CplDiversityStats, 0, len(f.cplPeerGroups))

	var sortedCpls []int
	for cpl := range f.cplPeerGroups {
		sortedCpls = append(sortedCpls, cpl)
	}
	sort.Ints(sortedCpls)

	for _, cpl := range sortedCpls {
		ps := make(map[peer.ID][]PeerIPGroupKey, len(f.cplPeerGroups[cpl]))
		cd := CplDiversityStats{cpl, ps}

		for p, groups := range f.cplPeerGroups[cpl] {
			ps[p] = groups
		}
		stats = append(stats, cd)
	}

	return stats
}
