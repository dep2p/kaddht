package netsize

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kbucket "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/libp2p/core/peer"
	logging "github.com/ipfs/go-log/v2"
	ks "github.com/whyrusleeping/go-keyspace"
)

// invalidEstimate 表示当前没有有效的缓存估计值
const invalidEstimate int32 = -1

var (
	ErrNotEnoughData   = fmt.Errorf("数据不足")
	ErrWrongNumOfPeers = fmt.Errorf("预期的对等节点数量错误")
)

var (
	logger                   = logging.Logger("dht/netsize")
	MaxMeasurementAge        = 2 * time.Hour // 测量数据的最大有效期
	MinMeasurementsThreshold = 5             // 最小测量数据阈值
	MaxMeasurementsThreshold = 150           // 最大测量数据阈值
	keyspaceMaxInt, _        = new(big.Int).SetString(strings.Repeat("1", 256), 2)
	keyspaceMaxFloat         = new(big.Float).SetInt(keyspaceMaxInt)
)

// Estimator 网络规模估计器
type Estimator struct {
	localID    kbucket.ID            // 本地节点ID
	rt         *kbucket.RoutingTable // 路由表
	bucketSize int                   // 桶大小

	measurementsLk sync.RWMutex          // 测量数据锁
	measurements   map[int][]measurement // 测量数据映射表

	netSizeCache int32 // 网络规模缓存
}

// NewEstimator 创建新的网络规模估计器
// 参数:
//   - localID: peer.ID 本地节点ID
//   - rt: *kbucket.RoutingTable 路由表
//   - bucketSize: int 桶大小
//
// 返回值:
//   - *Estimator 网络规模估计器实例
func NewEstimator(localID peer.ID, rt *kbucket.RoutingTable, bucketSize int) *Estimator {
	// 初始化测量数据映射表
	measurements := map[int][]measurement{}
	for i := 0; i < bucketSize; i++ {
		measurements[i] = []measurement{}
	}

	return &Estimator{
		localID:      kbucket.ConvertPeerID(localID),
		rt:           rt,
		bucketSize:   bucketSize,
		measurements: measurements,
		netSizeCache: invalidEstimate,
	}
}

// NormedDistance 计算给定键的归一化XOR距离(范围0到1)
// 参数:
//   - p: peer.ID 节点ID
//   - k: ks.Key 目标键
//
// 返回值:
//   - float64 归一化距离
func NormedDistance(p peer.ID, k ks.Key) float64 {
	pKey := ks.XORKeySpace.Key([]byte(p))
	ksDistance := new(big.Float).SetInt(pKey.Distance(k))
	normedDist, _ := new(big.Float).Quo(ksDistance, keyspaceMaxFloat).Float64()
	return normedDist
}

// measurement 测量数据结构
type measurement struct {
	distance  float64   // 距离
	weight    float64   // 权重
	timestamp time.Time // 时间戳
}

// Track 跟踪给定键的对等节点列表以纳入下一次网络规模估计
// 参数:
//   - key: string 目标键(预期不在kademlia键空间中)
//   - peers: []peer.ID 对给定键最近的对等节点列表(按距离排序)
//
// 返回值:
//   - error 错误信息
func (e *Estimator) Track(key string, peers []peer.ID) error {
	e.measurementsLk.Lock()
	defer e.measurementsLk.Unlock()

	// 合法性检查
	if len(peers) != e.bucketSize {
		return ErrWrongNumOfPeers
	}

	logger.Debugw("跟踪键的对等节点", "key", key)

	now := time.Now()

	// 使缓存失效
	atomic.StoreInt32(&e.netSizeCache, invalidEstimate)

	// 计算对等节点距离的权重
	weight := e.calcWeight(key, peers)

	// 将给定键映射到Kademlia键空间(哈希)
	ksKey := ks.XORKeySpace.Key([]byte(key))

	// 测量数据点的最大有效期时间戳
	maxAgeTs := now.Add(-MaxMeasurementAge)

	for i, p := range peers {
		// 构造测量数据结构
		m := measurement{
			distance:  NormedDistance(p, ksKey),
			weight:    weight,
			timestamp: now,
		}

		measurements := append(e.measurements[i], m)

		// 找到仍在允许时间窗口内的测量数据的最小索引
		// 所有较低索引的测量数据都应被丢弃,因为它们太旧了
		n := len(measurements)
		idx := sort.Search(n, func(j int) bool {
			return measurements[j].timestamp.After(maxAgeTs)
		})

		// 如果测量数据超出允许的时间窗口则移除它们
		// idx == n - 没有在允许时间窗口内的测量数据 -> 重置切片
		// idx == 0 - 正常情况,只有有效条目
		// idx != 0 - 有效和过期条目的混合
		if idx != 0 {
			x := make([]measurement, n-idx)
			copy(x, measurements[idx:])
			measurements = x
		}

		// 如果数据点数量超过最大阈值,删除最旧的测量数据点
		if len(measurements) > MaxMeasurementsThreshold {
			measurements = measurements[len(measurements)-MaxMeasurementsThreshold:]
		}

		e.measurements[i] = measurements
	}

	return nil
}

// NetworkSize 计算当前网络规模估计
// 返回值:
//   - int32 网络规模估计值
//   - error 错误信息
func (e *Estimator) NetworkSize() (int32, error) {

	// 无锁返回缓存的计算结果(快速路径)
	if estimate := atomic.LoadInt32(&e.netSizeCache); estimate != invalidEstimate {
		logger.Debugw("缓存的网络规模估计", "estimate", estimate)
		return estimate, nil
	}

	e.measurementsLk.Lock()
	defer e.measurementsLk.Unlock()

	// 再次检查。这是必要的,因为我们可能必须等待另一个goroutine完成计算。
	// 然后计算刚好被另一个goroutine完成,我们不需要重做。
	if estimate := e.netSizeCache; estimate != invalidEstimate {
		logger.Debugw("缓存的网络规模估计", "estimate", estimate)
		return estimate, nil
	}

	// 移除过期数据点
	e.garbageCollect()

	// 初始化线性拟合的切片
	xs := make([]float64, e.bucketSize)
	ys := make([]float64, e.bucketSize)
	yerrs := make([]float64, e.bucketSize)

	for i := 0; i < e.bucketSize; i++ {
		observationCount := len(e.measurements[i])

		// 如果没有足够的数据来合理计算网络规模,提前返回
		if observationCount < MinMeasurementsThreshold {
			return 0, ErrNotEnoughData
		}

		// 计算平均距离
		sumDistances := 0.0
		sumWeights := 0.0
		for _, m := range e.measurements[i] {
			sumDistances += m.weight * m.distance
			sumWeights += m.weight
		}
		distanceAvg := sumDistances / sumWeights

		// 计算标准差
		sumWeightedDiffs := 0.0
		for _, m := range e.measurements[i] {
			diff := m.distance - distanceAvg
			sumWeightedDiffs += m.weight * diff * diff
		}
		variance := sumWeightedDiffs / (float64(observationCount-1) / float64(observationCount) * sumWeights)
		distanceStd := math.Sqrt(variance)

		// 跟踪计算结果
		xs[i] = float64(i + 1)
		ys[i] = distanceAvg
		yerrs[i] = distanceStd
	}

	// 计算线性回归(假设线通过原点)
	var x2Sum, xySum float64
	for i, xi := range xs {
		yi := ys[i]
		xySum += yerrs[i] * xi * yi
		x2Sum += yerrs[i] * xi * xi
	}
	slope := xySum / x2Sum

	// 计算最终网络规模
	netSize := int32(1/slope - 1)

	// 缓存网络规模估计
	atomic.StoreInt32(&e.netSizeCache, netSize)

	logger.Debugw("新的网络规模估计", "estimate", netSize)
	return netSize, nil
}

// calcWeight 计算数据点的权重
// 如果数据点落入非满桶,则权重呈指数衰减
// 基于CPL和桶级别对距离估计进行加权
// 桶级别: 20 -> 1/2^0 -> 权重: 1
// 桶级别: 17 -> 1/2^3 -> 权重: 1/8
// 桶级别: 10 -> 1/2^10 -> 权重: 1/1024
//
// 可能出现路由表没有满桶,但我们在这里跟踪理论上适合该桶的对等节点列表的情况。
// 假设桶3中只有13个对等节点,虽然有20个空间。现在,Track函数获得一个对等节点列表(长度20),
// 其中所有对等节点都落入桶3。这组对等节点的权重应该是1而不是1/2^7。
// 我原本认为这种情况不会发生,因为在调用Track函数之前对等节点应该已经被添加到路由表中。
// 但它们似乎有时没有被添加。
// 参数:
//   - key: string 目标键
//   - peers: []peer.ID 对等节点列表
//
// 返回值:
//   - float64 计算得到的权重
func (e *Estimator) calcWeight(key string, peers []peer.ID) float64 {

	cpl := kbucket.CommonPrefixLen(kbucket.ConvertKey(key), e.localID)
	bucketLevel := e.rt.NPeersForCpl(uint(cpl))

	if bucketLevel < e.bucketSize {
		// 路由表没有满桶。检查有多少对等节点适合该桶
		peerLevel := 0
		for _, p := range peers {
			if cpl == kbucket.CommonPrefixLen(kbucket.ConvertPeerID(p), e.localID) {
				peerLevel += 1
			}
		}

		if peerLevel > bucketLevel {
			return math.Pow(2, float64(peerLevel-e.bucketSize))
		}
	}

	return math.Pow(2, float64(bucketLevel-e.bucketSize))
}

// garbageCollect 移除所有超出测量时间窗口的测量数据
func (e *Estimator) garbageCollect() {
	logger.Debug("运行垃圾回收")

	// 测量数据点的最大有效期时间戳
	maxAgeTs := time.Now().Add(-MaxMeasurementAge)

	for i := 0; i < e.bucketSize; i++ {

		// 找到仍在允许时间窗口内的测量数据的最小索引
		// 所有较低索引的测量数据都应被丢弃,因为它们太旧了
		n := len(e.measurements[i])
		idx := sort.Search(n, func(j int) bool {
			return e.measurements[i][j].timestamp.After(maxAgeTs)
		})

		// 如果测量数据超出允许的时间窗口则移除它们
		// idx == n - 没有在允许时间窗口内的测量数据 -> 重置切片
		// idx == 0 - 正常情况,只有有效条目
		// idx != 0 - 有效和过期条目的混合
		if idx == n {
			e.measurements[i] = []measurement{}
		} else if idx != 0 {
			e.measurements[i] = e.measurements[i][idx:]
		}
	}
}
