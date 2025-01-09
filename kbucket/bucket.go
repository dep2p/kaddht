//go:generate go run ./generate

package kbucket

import (
	"container/list"
	"time"

	"github.com/dep2p/libp2p/core/peer"
)

// PeerInfo 保存了 K-Bucket 中一个对等节点的所有相关信息
type PeerInfo struct {
	// Id 对等节点的标识符
	Id peer.ID

	// LastUsefulAt 是对等节点最后一次对我们"有用"的时间点
	// 请参阅 DHT 文档中关于有用性的定义
	LastUsefulAt time.Time

	// LastSuccessfulOutboundQueryAt 是我们最后一次从对等节点获得成功查询响应的时间点
	LastSuccessfulOutboundQueryAt time.Time

	// AddedAt 是该对等节点被添加到路由表的时间
	AddedAt time.Time

	// dhtId 是对等节点在 DHT XOR 密钥空间中的标识符
	dhtId ID

	// replaceable 表示当桶已满时,此对等节点可以被替换以为新对等节点腾出空间
	replaceable bool
}

// bucket 保存了一个对等节点列表
// 我们通过路由表锁来同步对桶的所有访问,因此桶本身不需要任何锁
// 如果将来我们想/需要避免在访问桶时锁定表,那么将由调用者负责同步对桶的所有访问
type bucket struct {
	list *list.List
}

// newBucket 创建一个新的桶
// 返回值:
//   - *bucket 新创建的桶
func newBucket() *bucket {
	b := new(bucket)
	b.list = list.New()
	return b
}

// peers 返回桶中的所有对等节点
// 返回值:
//   - []PeerInfo 对等节点信息列表
//
// 注意: 返回的对象对调用者来说是安全的,因为这是一个防御性副本
func (b *bucket) peers() []PeerInfo {
	ps := make([]PeerInfo, 0, b.len())
	for e := b.list.Front(); e != nil; e = e.Next() {
		p := e.Value.(*PeerInfo)
		ps = append(ps, *p)
	}
	return ps
}

// min 根据传入的 lessThan 比较器返回桶中的"最小"对等节点
// 参数:
//   - lessThan: func(p1 *PeerInfo, p2 *PeerInfo) bool 比较函数
//
// 返回值:
//   - *PeerInfo 最小的对等节点信息
//
// 注意: 比较器不能修改给定的 PeerInfo,因为我们传入的是指针
// 注意: 不能修改返回的值
func (b *bucket) min(lessThan func(p1 *PeerInfo, p2 *PeerInfo) bool) *PeerInfo {
	if b.list.Len() == 0 {
		return nil
	}

	minVal := b.list.Front().Value.(*PeerInfo)

	for e := b.list.Front().Next(); e != nil; e = e.Next() {
		val := e.Value.(*PeerInfo)

		if lessThan(val, minVal) {
			minVal = val
		}
	}

	return minVal
}

// updateAllWith 通过应用给定的更新函数来更新桶中的所有对等节点
// 参数:
//   - updateFnc: func(p *PeerInfo) 更新函数
func (b *bucket) updateAllWith(updateFnc func(p *PeerInfo)) {
	for e := b.list.Front(); e != nil; e = e.Next() {
		val := e.Value.(*PeerInfo)
		updateFnc(val)
	}
}

// peerIds 返回桶中所有对等节点的标识符
// 返回值:
//   - []peer.ID 对等节点标识符列表
func (b *bucket) peerIds() []peer.ID {
	ps := make([]peer.ID, 0, b.list.Len())
	for e := b.list.Front(); e != nil; e = e.Next() {
		p := e.Value.(*PeerInfo)
		ps = append(ps, p.Id)
	}
	return ps
}

// getPeer 返回具有给定标识符的对等节点(如果存在)
// 参数:
//   - p: peer.ID 对等节点标识符
//
// 返回值:
//   - *PeerInfo 对等节点信息,如果不存在则返回 nil
func (b *bucket) getPeer(p peer.ID) *PeerInfo {
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*PeerInfo).Id == p {
			return e.Value.(*PeerInfo)
		}
	}
	return nil
}

// remove 从桶中删除具有给定标识符的对等节点
// 参数:
//   - id: peer.ID 要删除的对等节点标识符
//
// 返回值:
//   - bool 如果删除成功返回 true,否则返回 false
func (b *bucket) remove(id peer.ID) bool {
	for e := b.list.Front(); e != nil; e = e.Next() {
		if e.Value.(*PeerInfo).Id == id {
			b.list.Remove(e)
			return true
		}
	}
	return false
}

// pushFront 将对等节点信息添加到桶的前面
// 参数:
//   - p: *PeerInfo 要添加的对等节点信息
func (b *bucket) pushFront(p *PeerInfo) {
	b.list.PushFront(p)
}

// len 返回桶中对等节点的数量
// 返回值:
//   - int 对等节点数量
func (b *bucket) len() int {
	return b.list.Len()
}

// split 将桶中的对等节点分成两个桶
// 参数:
//   - cpl: int 公共前缀长度
//   - target: ID 目标标识符
//
// 返回值:
//   - *bucket 新桶,包含 CPL 大于给定值的对等节点(更接近的对等节点)
//
// 注意: 接收者将保留 CPL 等于给定值的对等节点
func (b *bucket) split(cpl int, target ID) *bucket {
	out := list.New()
	newbuck := newBucket()
	newbuck.list = out
	e := b.list.Front()
	for e != nil {
		pDhtId := e.Value.(*PeerInfo).dhtId
		peerCPL := CommonPrefixLen(pDhtId, target)
		if peerCPL > cpl {
			cur := e
			out.PushBack(e.Value)
			e = e.Next()
			b.list.Remove(cur)
			continue
		}
		e = e.Next()
	}
	return newbuck
}

// maxCommonPrefix 返回桶中任何对等节点与目标标识符之间的最大公共前缀长度
// 参数:
//   - target: ID 目标标识符
//
// 返回值:
//   - uint 最大公共前缀长度
func (b *bucket) maxCommonPrefix(target ID) uint {
	maxCpl := uint(0)
	for e := b.list.Front(); e != nil; e = e.Next() {
		cpl := uint(CommonPrefixLen(e.Value.(*PeerInfo).dhtId, target))
		if cpl > maxCpl {
			maxCpl = cpl
		}
	}
	return maxCpl
}
