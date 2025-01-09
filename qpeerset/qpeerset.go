package qpeerset

import (
	"math/big"
	"sort"

	"github.com/dep2p/libp2p/core/peer"
	ks "github.com/whyrusleeping/go-keyspace"
)

// PeerState 描述在单个查找生命周期中对等节点ID的状态
type PeerState int

const (
	// PeerHeard 表示尚未查询的对等节点
	PeerHeard PeerState = iota
	// PeerWaiting 表示当前正在查询的对等节点
	PeerWaiting
	// PeerQueried 表示已查询且成功获得响应的对等节点
	PeerQueried
	// PeerUnreachable 表示已查询但未成功获得响应的对等节点
	PeerUnreachable
)

// QueryPeerset 维护Kademlia异步查找的状态
// 查找状态是一组对等节点,每个节点都标有对等节点状态
type QueryPeerset struct {
	// 正在搜索的键
	key ks.Key

	// 所有已知的对等节点
	all []queryPeerState

	// sorted 表示all是否当前已排序
	sorted bool
}

// queryPeerState 查询对等节点状态
type queryPeerState struct {
	id         peer.ID   // 对等节点ID
	distance   *big.Int  // 与目标键的距离
	state      PeerState // 节点状态
	referredBy peer.ID   // 推荐该节点的对等节点ID
}

// sortedQueryPeerset 已排序的查询对等节点集合
type sortedQueryPeerset QueryPeerset

// Len 实现sort.Interface接口
// 返回值:
//   - int 集合长度
func (sqp *sortedQueryPeerset) Len() int {
	return len(sqp.all)
}

// Swap 实现sort.Interface接口
// 参数:
//   - i: int 第一个元素索引
//   - j: int 第二个元素索引
func (sqp *sortedQueryPeerset) Swap(i, j int) {
	sqp.all[i], sqp.all[j] = sqp.all[j], sqp.all[i]
}

// Less 实现sort.Interface接口
// 参数:
//   - i: int 第一个元素索引
//   - j: int 第二个元素索引
//
// 返回值:
//   - bool 如果第一个元素小于第二个元素则返回true
func (sqp *sortedQueryPeerset) Less(i, j int) bool {
	di, dj := sqp.all[i].distance, sqp.all[j].distance
	return di.Cmp(dj) == -1
}

// NewQueryPeerset 创建一个新的空对等节点集合
// 参数:
//   - key: string 查找的目标键
//
// 返回值:
//   - *QueryPeerset 查询对等节点集合
func NewQueryPeerset(key string) *QueryPeerset {
	return &QueryPeerset{
		key:    ks.XORKeySpace.Key([]byte(key)),
		all:    []queryPeerState{},
		sorted: false,
	}
}

// find 在集合中查找对等节点
// 参数:
//   - p: peer.ID 要查找的对等节点ID
//
// 返回值:
//   - int 找到的索引,未找到返回-1
func (qp *QueryPeerset) find(p peer.ID) int {
	for i := range qp.all {
		if qp.all[i].id == p {
			return i
		}
	}
	return -1
}

// distanceToKey 计算对等节点到目标键的距离
// 参数:
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - *big.Int XOR距离
func (qp *QueryPeerset) distanceToKey(p peer.ID) *big.Int {
	return ks.XORKeySpace.Key([]byte(p)).Distance(qp.key)
}

// TryAdd 将对等节点p添加到对等节点集合中
// 如果对等节点已存在,则不执行任何操作
// 否则,将对等节点添加并将状态设置为PeerHeard
// 参数:
//   - p: peer.ID 要添加的对等节点ID
//   - referredBy: peer.ID 推荐该节点的对等节点ID
//
// 返回值:
//   - bool 如果对等节点不存在则返回true
func (qp *QueryPeerset) TryAdd(p, referredBy peer.ID) bool {
	if qp.find(p) >= 0 {
		return false
	} else {
		qp.all = append(qp.all,
			queryPeerState{id: p, distance: qp.distanceToKey(p), state: PeerHeard, referredBy: referredBy})
		qp.sorted = false
		return true
	}
}

func (qp *QueryPeerset) sort() {
	if qp.sorted {
		return
	}
	sort.Sort((*sortedQueryPeerset)(qp))
	qp.sorted = true
}

// SetState 设置对等节点p的状态为s
// 如果p不在对等节点集合中,SetState会panic
// 参数:
//   - p: peer.ID 对等节点ID
//   - s: PeerState 要设置的状态
func (qp *QueryPeerset) SetState(p peer.ID, s PeerState) {
	qp.all[qp.find(p)].state = s
}

// GetState 返回对等节点p的状态
// 如果p不在对等节点集合中,GetState会panic
// 参数:
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - PeerState 对等节点状态
func (qp *QueryPeerset) GetState(p peer.ID) PeerState {
	return qp.all[qp.find(p)].state
}

// GetReferrer 返回推荐对等节点p的节点
// 如果p不在对等节点集合中,GetReferrer会panic
// 参数:
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - peer.ID 推荐节点ID
func (qp *QueryPeerset) GetReferrer(p peer.ID) peer.ID {
	return qp.all[qp.find(p)].referredBy
}

// GetClosestNInStates 返回距离键最近的、处于给定状态之一的n个对等节点
// 如果满足条件的对等节点少于n个,则返回所有满足条件的节点
// 返回的对等节点按照到键的距离升序排序
// 参数:
//   - n: int 要返回的节点数量
//   - states: ...PeerState 状态列表
//
// 返回值:
//   - []peer.ID 对等节点ID列表
func (qp *QueryPeerset) GetClosestNInStates(n int, states ...PeerState) (result []peer.ID) {
	qp.sort()
	m := make(map[PeerState]struct{}, len(states))
	for i := range states {
		m[states[i]] = struct{}{}
	}

	for _, p := range qp.all {
		if _, ok := m[p.state]; ok {
			result = append(result, p.id)
		}
	}
	if len(result) >= n {
		return result[:n]
	}
	return result
}

// GetClosestInStates 返回处于给定状态之一的所有对等节点
// 返回的对等节点按照到键的距离升序排序
// 参数:
//   - states: ...PeerState 状态列表
//
// 返回值:
//   - []peer.ID 对等节点ID列表
func (qp *QueryPeerset) GetClosestInStates(states ...PeerState) (result []peer.ID) {
	return qp.GetClosestNInStates(len(qp.all), states...)
}

// NumHeard 返回处于PeerHeard状态的对等节点数量
// 返回值:
//   - int 对等节点数量
func (qp *QueryPeerset) NumHeard() int {
	return len(qp.GetClosestInStates(PeerHeard))
}

// NumWaiting 返回处于PeerWaiting状态的对等节点数量
// 返回值:
//   - int 对等节点数量
func (qp *QueryPeerset) NumWaiting() int {
	return len(qp.GetClosestInStates(PeerWaiting))
}
