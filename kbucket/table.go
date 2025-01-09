// kbucket 包实现了 kademlia 'k-bucket' 路由表
package kbucket

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/peerstore"

	"github.com/dep2p/kaddht/kbucket/peerdiversity"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("table")

var ErrPeerRejectedHighLatency = errors.New("节点被拒绝: 延迟太高")
var ErrPeerRejectedNoCapacity = errors.New("节点被拒绝: 容量不足")

// RoutingTable 定义路由表
type RoutingTable struct {
	// 路由表上下文
	ctx context.Context
	// 取消路由表上下文的函数
	ctxCancel context.CancelFunc

	// 本地节点ID
	local ID

	// 全局锁,后续优化性能时可细化
	tabLock sync.RWMutex

	// 延迟度量
	metrics peerstore.Metrics

	// 此集群中节点的最大可接受延迟
	maxLatency time.Duration

	// kBuckets 定义了所有指向其他节点的指针
	buckets    []*bucket
	bucketsize int

	cplRefreshLk   sync.RWMutex
	cplRefreshedAt map[uint]time.Time

	// 通知函数
	PeerRemoved func(peer.ID)
	PeerAdded   func(peer.ID)

	// usefulnessGracePeriod 是我们给桶中节点的最大宽限期,在此期间节点必须对我们有用,否则如果桶已满,我们会将其驱逐以为新节点腾出空间
	usefulnessGracePeriod time.Duration

	df *peerdiversity.Filter
}

// NewRoutingTable 创建一个新的路由表
// 参数:
//   - bucketsize: int 桶大小
//   - localID: ID 本地节点ID
//   - latency: time.Duration 延迟容忍度
//   - m: peerstore.Metrics 度量对象
//   - usefulnessGracePeriod: time.Duration 有用性宽限期
//   - df: *peerdiversity.Filter 多样性过滤器
//
// 返回值:
//   - *RoutingTable 路由表对象
//   - error 错误信息
func NewRoutingTable(bucketsize int, localID ID, latency time.Duration, m peerstore.Metrics, usefulnessGracePeriod time.Duration,
	df *peerdiversity.Filter) (*RoutingTable, error) {
	rt := &RoutingTable{
		buckets:    []*bucket{newBucket()},
		bucketsize: bucketsize,
		local:      localID,

		maxLatency: latency,
		metrics:    m,

		cplRefreshedAt: make(map[uint]time.Time),

		PeerRemoved: func(p peer.ID) {
			log.Debugw("节点已移除", "peer", p)
		},
		PeerAdded: func(p peer.ID) {
			log.Debugw("节点已添加", "peer", p)
		},

		usefulnessGracePeriod: usefulnessGracePeriod,

		df: df,
	}

	rt.ctx, rt.ctxCancel = context.WithCancel(context.Background())

	return rt, nil
}

// Close 关闭路由表和所有相关进程
// 可以安全地多次调用
// 返回值:
//   - error 错误信息
func (rt *RoutingTable) Close() error {
	rt.ctxCancel()
	return nil
}

// NPeersForCpl 返回给定 Cpl 的节点数量
// 参数:
//   - cpl: uint 公共前缀长度
//
// 返回值:
//   - int 节点数量
func (rt *RoutingTable) NPeersForCpl(cpl uint) int {
	rt.tabLock.RLock()
	defer rt.tabLock.RUnlock()

	// 在最后一个桶中
	if int(cpl) >= len(rt.buckets)-1 {
		count := 0
		b := rt.buckets[len(rt.buckets)-1]
		for _, p := range b.peers() {
			if CommonPrefixLen(rt.local, p.dhtId) == int(cpl) {
				count++
			}
		}
		return count
	} else {
		return rt.buckets[cpl].len()
	}
}

// UsefulNewPeer 验证给定的 peer.ID 是否适合路由表
// 如果节点不在路由表中,或者对应的桶未满,或者包含可替换的节点,或者是最后一个桶且添加节点会导致展开,则返回 true
// 参数:
//   - p: peer.ID 待验证的节点ID
//
// 返回值:
//   - bool 是否有用
func (rt *RoutingTable) UsefulNewPeer(p peer.ID) bool {
	rt.tabLock.RLock()
	defer rt.tabLock.RUnlock()

	// 对应节点的桶
	bucketID := rt.bucketIdForPeer(p)
	bucket := rt.buckets[bucketID]

	if bucket.getPeer(p) != nil {
		// 节点已存在于路由表中,因此无用
		return false
	}

	// 桶未满
	if bucket.len() < rt.bucketsize {
		return true
	}

	// 桶已满,检查是否包含可替换的节点
	for e := bucket.list.Front(); e != nil; e = e.Next() {
		peer := e.Value.(*PeerInfo)
		if peer.replaceable {
			// 至少有1个节点可替换
			return true
		}
	}

	// 最后一个桶可能包含不同 CPL 的节点ID,如果需要可以分成2个桶
	if bucketID == len(rt.buckets)-1 {
		peers := bucket.peers()
		cpl := CommonPrefixLen(rt.local, ConvertPeerID(p))
		for _, peer := range peers {
			// 如果至少有2个节点有不同的 CPL,新节点有用且会触发桶分裂
			if CommonPrefixLen(rt.local, peer.dhtId) != cpl {
				return true
			}
		}
	}

	// 对应的桶已满且都是不可替换的节点
	return false
}

// TryAddPeer 尝试将节点添加到路由表中
// 如果节点已存在于路由表中且之前已查询过,此调用无效
// 如果节点已存在于路由表中但之前未查询过,我们将其 LastUsefulAt 值设置为当前时间
// 这是必要的,因为我们首次连接到节点时不会将其标记为"有用"(通过设置 LastUsefulAt 值)
//
// 如果是查询节点(即我们查询它或它查询我们),我们将 LastSuccessfulOutboundQuery 设置为当前时间
// 如果只是我们连接到/它连接到我们而没有任何 DHT 查询的节点,我们认为它没有 LastSuccessfulOutboundQuery
//
// 如果节点所属的逻辑桶已满且不是最后一个桶,我们尝试用新节点替换该桶中 LastSuccessfulOutboundQuery 超过最大允许阈值的现有节点
// 如果该桶中不存在这样的节点,我们不会将节点添加到路由表中并返回错误 "ErrPeerRejectedNoCapacity"
//
// 参数:
//   - p: peer.ID 待添加的节点ID
//   - queryPeer: bool 是否为查询节点
//   - isReplaceable: bool 是否可替换
//
// 返回值:
//   - bool 如果节点是新添加到路由表中则为 true,否则为 false
//   - error 添加节点时发生的错误。如果错误不为 nil,布尔值将始终为 false,即如果节点不在路由表中,它不会被添加
//
// 返回值 false 且 error=nil 表示节点已存在于路由表中
func (rt *RoutingTable) TryAddPeer(p peer.ID, queryPeer bool, isReplaceable bool) (bool, error) {
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()

	return rt.addPeer(p, queryPeer, isReplaceable)
}

// locking 由调用者负责
func (rt *RoutingTable) addPeer(p peer.ID, queryPeer bool, isReplaceable bool) (bool, error) {
	bucketID := rt.bucketIdForPeer(p)
	bucket := rt.buckets[bucketID]

	now := time.Now()
	var lastUsefulAt time.Time
	if queryPeer {
		lastUsefulAt = now
	}

	// 节点已存在于路由表中
	if peerInfo := bucket.getPeer(p); peerInfo != nil {
		// 如果我们在添加节点后首次查询它,让我们给它一个有用性提升。这只会发生一次
		if peerInfo.LastUsefulAt.IsZero() && queryPeer {
			peerInfo.LastUsefulAt = lastUsefulAt
		}
		return false, nil
	}

	// 节点的延迟阈值不可接受
	if rt.metrics.LatencyEWMA(p) > rt.maxLatency {
		// 连接不满足要求,跳过!
		return false, ErrPeerRejectedHighLatency
	}

	// 暂时将其添加到多样性过滤器中
	// 如果我们无法在表中为节点找到位置,稍后我们将简单地从过滤器中删除它
	if rt.df != nil {
		if !rt.df.TryAdd(p) {
			return false, errors.New("节点被多样性过滤器拒绝")
		}
	}

	// 桶中有足够的空间(无论是新生成的还是分组的)
	if bucket.len() < rt.bucketsize {
		bucket.pushFront(&PeerInfo{
			Id:                            p,
			LastUsefulAt:                  lastUsefulAt,
			LastSuccessfulOutboundQueryAt: now,
			AddedAt:                       now,
			dhtId:                         ConvertPeerID(p),
			replaceable:                   isReplaceable,
		})
		rt.PeerAdded(p)
		return true, nil
	}

	if bucketID == len(rt.buckets)-1 {
		// 如果桶太大且这是最后一个桶(即通配符),展开它
		rt.nextBucket()
		// 表的结构已更改,让我们重新检查节点现在是否有专用桶
		bucketID = rt.bucketIdForPeer(p)
		bucket = rt.buckets[bucketID]

		// 仅当分裂后桶未溢出时才推入节点
		if bucket.len() < rt.bucketsize {
			bucket.pushFront(&PeerInfo{
				Id:                            p,
				LastUsefulAt:                  lastUsefulAt,
				LastSuccessfulOutboundQueryAt: now,
				AddedAt:                       now,
				dhtId:                         ConvertPeerID(p),
				replaceable:                   isReplaceable,
			})
			rt.PeerAdded(p)
			return true, nil
		}
	}

	// 节点所属的桶已满。让我们尝试在该桶中找到一个可替换的节点
	// 我们不需要稳定排序,因为只要是可替换的节点,驱逐哪个节点并不重要
	replaceablePeer := bucket.min(func(p1 *PeerInfo, p2 *PeerInfo) bool {
		return p1.replaceable
	})

	if replaceablePeer != nil && replaceablePeer.replaceable {
		// 我们找到了一个可替换的节点,让我们用新节点替换它

		// 将新节点添加到桶中。需要在我们移除可替换节点之前发生,因为如果桶大小为1,我们将最终移除唯一的节点,并删除桶
		bucket.pushFront(&PeerInfo{
			Id:                            p,
			LastUsefulAt:                  lastUsefulAt,
			LastSuccessfulOutboundQueryAt: now,
			AddedAt:                       now,
			dhtId:                         ConvertPeerID(p),
			replaceable:                   isReplaceable,
		})
		rt.PeerAdded(p)

		// 移除可替换节点
		rt.removePeer(replaceablePeer.Id)
		return true, nil
	}

	// 我们无法为节点找到位置,从过滤器状态中移除它
	if rt.df != nil {
		rt.df.Remove(p)
	}
	return false, ErrPeerRejectedNoCapacity
}

// MarkAllPeersIrreplaceable 将路由表中的所有节点标记为不可替换
// 这意味着我们永远不会替换表中的现有节点来为新节点腾出空间
// 但是,它们仍然可以通过调用 `RemovePeer` API 来移除
func (rt *RoutingTable) MarkAllPeersIrreplaceable() {
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()

	for i := range rt.buckets {
		b := rt.buckets[i]
		b.updateAllWith(func(p *PeerInfo) {
			p.replaceable = false
		})
	}
}

// GetPeerInfos 返回我们在桶中存储的节点信息
// 返回值:
//   - []PeerInfo 节点信息列表
func (rt *RoutingTable) GetPeerInfos() []PeerInfo {
	rt.tabLock.RLock()
	defer rt.tabLock.RUnlock()

	var pis []PeerInfo
	for _, b := range rt.buckets {
		pis = append(pis, b.peers()...)
	}
	return pis
}

// UpdateLastSuccessfulOutboundQueryAt 更新节点的 LastSuccessfulOutboundQueryAt 时间
// 参数:
//   - p: peer.ID 节点ID
//   - t: time.Time 时间
//
// 返回值:
//   - bool 如果更新成功则为 true,否则为 false
func (rt *RoutingTable) UpdateLastSuccessfulOutboundQueryAt(p peer.ID, t time.Time) bool {
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()

	bucketID := rt.bucketIdForPeer(p)
	bucket := rt.buckets[bucketID]

	if pc := bucket.getPeer(p); pc != nil {
		pc.LastSuccessfulOutboundQueryAt = t
		return true
	}
	return false
}

// UpdateLastUsefulAt 更新节点的 LastUsefulAt 时间
// 参数:
//   - p: peer.ID 节点ID
//   - t: time.Time 时间
//
// 返回值:
//   - bool 如果更新成功则为 true,否则为 false
func (rt *RoutingTable) UpdateLastUsefulAt(p peer.ID, t time.Time) bool {
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()

	bucketID := rt.bucketIdForPeer(p)
	bucket := rt.buckets[bucketID]

	if pc := bucket.getPeer(p); pc != nil {
		pc.LastUsefulAt = t
		return true
	}
	return false
}

// RemovePeer 当调用者确定节点对查询无用时应调用此函数
// 例如:节点可能已停止支持 DHT 协议
// 它会从路由表中驱逐节点
// 参数:
//   - p: peer.ID 要移除的节点ID
func (rt *RoutingTable) RemovePeer(p peer.ID) {
	rt.tabLock.Lock()
	defer rt.tabLock.Unlock()
	rt.removePeer(p)
}

// locking 由调用者负责
func (rt *RoutingTable) removePeer(p peer.ID) bool {
	bucketID := rt.bucketIdForPeer(p)
	bucket := rt.buckets[bucketID]
	if bucket.remove(p) {
		if rt.df != nil {
			rt.df.Remove(p)
		}
		for {
			lastBucketIndex := len(rt.buckets) - 1

			// 如果最后一个桶为空且不是我们唯一的桶,则移除它
			if len(rt.buckets) > 1 && rt.buckets[lastBucketIndex].len() == 0 {
				rt.buckets[lastBucketIndex] = nil
				rt.buckets = rt.buckets[:lastBucketIndex]
			} else if len(rt.buckets) >= 2 && rt.buckets[lastBucketIndex-1].len() == 0 {
				// 如果倒数第二个桶刚刚变空,移除它并用最后一个桶替换它
				rt.buckets[lastBucketIndex-1] = rt.buckets[lastBucketIndex]
				rt.buckets[lastBucketIndex] = nil
				rt.buckets = rt.buckets[:lastBucketIndex]
			} else {
				break
			}
		}

		// 节点移除回调
		rt.PeerRemoved(p)
		return true
	}
	return false
}

func (rt *RoutingTable) nextBucket() {
	// 这是最后一个桶,据称是一个混合桶,包含不属于专用(展开的)桶的节点
	// 这里使用"据称"是为了表示最后一个桶中的*所有*节点可能实际上属于另一个桶
	// 例如,如果我们展开了4个桶,而折叠桶5中的所有节点实际上属于桶8,就可能发生这种情况
	bucket := rt.buckets[len(rt.buckets)-1]
	newBucket := bucket.split(len(rt.buckets)-1, rt.local)
	rt.buckets = append(rt.buckets, newBucket)

	// 新形成的桶仍然包含太多节点。我们可能刚刚展开了一个空桶
	if newBucket.len() >= rt.bucketsize {
		// 继续展开表,直到最后一个桶不再溢出
		rt.nextBucket()
	}
}

// Find 通过ID查找特定节点或返回nil
// 参数:
//   - id: peer.ID 要查找的节点ID
//
// 返回值:
//   - peer.ID 找到的节点ID,如果未找到则为空
func (rt *RoutingTable) Find(id peer.ID) peer.ID {
	srch := rt.NearestPeers(ConvertPeerID(id), 1)
	if len(srch) == 0 || srch[0] != id {
		return ""
	}
	return srch[0]
}

// NearestPeer 返回最接近给定ID的单个节点
// 参数:
//   - id: ID 目标ID
//
// 返回值:
//   - peer.ID 最近节点的ID,如果未找到则为空
func (rt *RoutingTable) NearestPeer(id ID) peer.ID {
	peers := rt.NearestPeers(id, 1)
	if len(peers) > 0 {
		return peers[0]
	}

	log.Debugf("NearestPeer: 返回 nil, 表大小 = %d", rt.Size())
	return ""
}

// NearestPeers 返回距离给定ID最近的'count'个节点列表
// 参数:
//   - id: ID 目标ID
//   - count: int 要返回的节点数量
//
// 返回值:
//   - []peer.ID 最近节点的ID列表
func (rt *RoutingTable) NearestPeers(id ID, count int) []peer.ID {
	// 这是_我们_与键共享的位数。此桶中的所有节点与我们共享cpl位,因此将至少与给定键共享cpl+1位。+1是因为目标和此桶中的所有节点在cpl位上与我们不同
	cpl := CommonPrefixLen(id, rt.local)

	// 假定这也保护了桶
	rt.tabLock.RLock()

	// 获取桶索引或最后一个桶
	if cpl >= len(rt.buckets) {
		cpl = len(rt.buckets) - 1
	}

	pds := peerDistanceSorter{
		peers:  make([]peerDistance, 0, count+rt.bucketsize),
		target: id,
	}

	// 从目标桶添加节点(共享cpl+1位)
	pds.appendPeersFromList(rt.buckets[cpl].list)

	// 如果数量不足,从右侧的所有桶添加节点
	// 右侧的所有桶恰好共享cpl位(与cpl桶中的节点共享的cpl+1位相对)
	//
	// 这不幸地比我们希望的效率低。我们最终会切换到trie实现,这将允许我们找到最接近任何目标键的N个节点

	if pds.Len() < count {
		for i := cpl + 1; i < len(rt.buckets); i++ {
			pds.appendPeersFromList(rt.buckets[i].list)
		}
	}

	// 如果仍然不足,添加共享_更少_位的桶
	// 我们可以一个桶一个桶地做,因为每个桶比上一个桶少共享1位
	//
	// * 桶cpl-1: 共享cpl-1位
	// * 桶cpl-2: 共享cpl-2位
	// ...
	for i := cpl - 1; i >= 0 && pds.Len() < count; i-- {
		pds.appendPeersFromList(rt.buckets[i].list)
	}
	rt.tabLock.RUnlock()

	// 按与本地节点的距离排序
	pds.sort()

	if count < pds.Len() {
		pds.peers = pds.peers[:count]
	}

	out := make([]peer.ID, 0, pds.Len())
	for _, p := range pds.peers {
		out = append(out, p.p)
	}

	return out
}

// Size 返回路由表中的节点总数
// 返回值:
//   - int 节点总数
func (rt *RoutingTable) Size() int {
	var tot int
	rt.tabLock.RLock()
	for _, buck := range rt.buckets {
		tot += buck.len()
	}
	rt.tabLock.RUnlock()
	return tot
}

// ListPeers 获取路由表并返回表中所有桶的所有节点列表
// 返回值:
//   - []peer.ID 所有节点的ID列表
func (rt *RoutingTable) ListPeers() []peer.ID {
	rt.tabLock.RLock()
	defer rt.tabLock.RUnlock()

	var peers []peer.ID
	for _, buck := range rt.buckets {
		peers = append(peers, buck.peerIds()...)
	}
	return peers
}

// Print 打印关于提供的路由表的描述性语句
func (rt *RoutingTable) Print() {
	fmt.Printf("路由表, bs = %d, 最大延迟 = %d\n", rt.bucketsize, rt.maxLatency)
	rt.tabLock.RLock()

	for i, b := range rt.buckets {
		fmt.Printf("\t桶: %d\n", i)

		for e := b.list.Front(); e != nil; e = e.Next() {
			p := e.Value.(*PeerInfo).Id
			fmt.Printf("\t\t- %s %s\n", p.String(), rt.metrics.LatencyEWMA(p).String())
		}
	}
	rt.tabLock.RUnlock()
}

// GetDiversityStats 如果配置了多样性过滤器,则返回路由表的多样性统计信息
// 返回值:
//   - []peerdiversity.CplDiversityStats 多样性统计信息
func (rt *RoutingTable) GetDiversityStats() []peerdiversity.CplDiversityStats {
	if rt.df != nil {
		return rt.df.GetDiversityStats()
	}
	return nil
}

// 调用者负责锁定
func (rt *RoutingTable) bucketIdForPeer(p peer.ID) int {
	peerID := ConvertPeerID(p)
	cpl := CommonPrefixLen(peerID, rt.local)
	bucketID := cpl
	if bucketID >= len(rt.buckets) {
		bucketID = len(rt.buckets) - 1
	}
	return bucketID
}

// maxCommonPrefix 返回表中任何节点与当前节点之间的最大公共前缀长度
// 返回值:
//   - uint 最大公共前缀长度
func (rt *RoutingTable) maxCommonPrefix() uint {
	rt.tabLock.RLock()
	defer rt.tabLock.RUnlock()

	for i := len(rt.buckets) - 1; i >= 0; i-- {
		if rt.buckets[i].len() > 0 {
			return rt.buckets[i].maxCommonPrefix(rt.local)
		}
	}
	return 0
}
