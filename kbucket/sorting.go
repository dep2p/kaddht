package kbucket

import (
	"container/list"
	"sort"

	"github.com/dep2p/libp2p/core/peer"
)

// peerDistance 是一个辅助结构体,用于按照对等节点到本地节点的距离进行排序
type peerDistance struct {
	p        peer.ID
	distance ID
}

// peerDistanceSorter 实现了 sort.Interface 接口,用于按照异或距离对对等节点进行排序
type peerDistanceSorter struct {
	peers  []peerDistance
	target ID
}

// Len 返回对等节点的数量
// 返回值:
//   - int 对等节点数量
func (pds *peerDistanceSorter) Len() int { return len(pds.peers) }

// Swap 交换两个对等节点的位置
// 参数:
//   - a: int 第一个对等节点的索引
//   - b: int 第二个对等节点的索引
func (pds *peerDistanceSorter) Swap(a, b int) {
	pds.peers[a], pds.peers[b] = pds.peers[b], pds.peers[a]
}

// Less 比较两个对等节点到目标的距离
// 参数:
//   - a: int 第一个对等节点的索引
//   - b: int 第二个对等节点的索引
//
// 返回值:
//   - bool 如果第一个对等节点距离更近返回 true,否则返回 false
func (pds *peerDistanceSorter) Less(a, b int) bool {
	return pds.peers[a].distance.less(pds.peers[b].distance)
}

// appendPeer 将对等节点ID添加到排序器的切片中。添加后可能不再有序
// 参数:
//   - p: peer.ID 对等节点ID
//   - pDhtId: ID DHT标识符
func (pds *peerDistanceSorter) appendPeer(p peer.ID, pDhtId ID) {
	pds.peers = append(pds.peers, peerDistance{
		p:        p,
		distance: xor(pds.target, pDhtId),
	})
}

// appendPeersFromList 将列表中的对等节点ID添加到排序器的切片中。添加后可能不再有序
// 参数:
//   - l: *list.List 对等节点列表
func (pds *peerDistanceSorter) appendPeersFromList(l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		pds.appendPeer(e.Value.(*PeerInfo).Id, e.Value.(*PeerInfo).dhtId)
	}
}

// sort 对排序器中的对等节点进行排序
func (pds *peerDistanceSorter) sort() {
	sort.Sort(pds)
}

// SortClosestPeers 按照与目标的距离升序对给定的对等节点进行排序
// 参数:
//   - peers: []peer.ID 要排序的对等节点ID列表
//   - target: ID 目标标识符
//
// 返回值:
//   - []peer.ID 排序后的新对等节点ID切片
func SortClosestPeers(peers []peer.ID, target ID) []peer.ID {
	sorter := peerDistanceSorter{
		peers:  make([]peerDistance, 0, len(peers)),
		target: target,
	}
	for _, p := range peers {
		sorter.appendPeer(p, ConvertPeerID(p))
	}
	sorter.sort()
	out := make([]peer.ID, 0, sorter.Len())
	for _, p := range sorter.peers {
		out = append(out, p.p)
	}
	return out
}
