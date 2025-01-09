package dht

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	kb "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/kaddht/metrics"
	"github.com/dep2p/kaddht/netsize"
	"github.com/dep2p/kaddht/qpeerset"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/multiformats/go-multihash"
	ks "github.com/whyrusleeping/go-keyspace"
	"gonum.org/v1/gonum/mathext"
)

const (
	// optProvIndividualThresholdCertainty 描述我们希望在遍历DHT时找到的单个节点属于基于当前网络规模估计的k个最近节点的确定性
	optProvIndividualThresholdCertainty = 0.9

	// optProvSetThresholdStrictness 描述最近节点集实际上比计算的集合阈值更远的概率。
	// 换句话说,我们过于严格而不提前终止进程的概率是多少,因为我们找不到更近的节点。
	optProvSetThresholdStrictness = 0.1

	// optProvReturnRatio 对应在返回给用户之前必须完成的ADD_PROVIDER RPC数量(无论成功与否)。
	// 0.75的比率等于15个RPC,因为它基于Kademlia桶大小。
	optProvReturnRatio = 0.75
)

// addProviderRPCState 表示ADD_PROVIDER RPC的状态
type addProviderRPCState int

const (
	scheduled addProviderRPCState = iota + 1 // 已调度
	success                                  // 成功
	failure                                  // 失败
)

// optimisticState 表示乐观提供状态
type optimisticState struct {
	// putCtx 用于所有ADD_PROVIDER RPC的上下文
	putCtx context.Context

	// dht DHT实例的引用
	dht *IpfsDHT

	// networkSize 最近的网络规模估计
	networkSize int32

	// doneChan 指示ADD_PROVIDER RPC完成的通道(无论成功与否)
	doneChan chan struct{}

	// peerStatesLk 用于保护peerStates的互斥锁
	peerStatesLk sync.RWMutex
	// peerStates 跟踪我们已存储提供者记录的节点
	peerStates map[peer.ID]addProviderRPCState

	// key 要提供的键
	key string

	// ksKey 转换为Kademlia键空间的键
	ksKey ks.Key

	// individualThreshold 单个节点的距离阈值。如果节点比这个数字更近,我们立即存储提供者记录。
	individualThreshold float64

	// setThreshold bucketSize个最近节点集的距离阈值。
	// 如果bucketSize个最近节点的平均距离低于此数字,我们停止DHT遍历并存储剩余的提供者记录。
	// "剩余"是因为我们可能已经在低于individualThreshold的节点上存储了一些记录。
	setThreshold float64

	// returnThreshold 在返回控制权给用户之前需要完成的ADD_PROVIDER RPC数量(无论成功与否)。
	returnThreshold int

	// putProvDone 计数已完成的ADD_PROVIDER RPC(成功和失败)
	putProvDone atomic.Int32
}

// newOptimisticState 创建新的乐观状态
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键值
//
// 返回值:
//   - *optimisticState 乐观状态实例
//   - error 错误信息
func (dht *IpfsDHT) newOptimisticState(ctx context.Context, key string) (*optimisticState, error) {
	// 获取网络规模,如果没有合理的估计则报错
	networkSize, err := dht.nsEstimator.NetworkSize()
	if err != nil {
		return nil, err
	}

	individualThreshold := mathext.GammaIncRegInv(float64(dht.bucketSize), 1-optProvIndividualThresholdCertainty) / float64(networkSize)
	setThreshold := mathext.GammaIncRegInv(float64(dht.bucketSize)/2.0+1, 1-optProvSetThresholdStrictness) / float64(networkSize)
	returnThreshold := int(math.Ceil(float64(dht.bucketSize) * optProvReturnRatio))

	return &optimisticState{
		putCtx:              ctx,
		dht:                 dht,
		key:                 key,
		doneChan:            make(chan struct{}, returnThreshold), // 缓冲通道以不错过事件
		ksKey:               ks.XORKeySpace.Key([]byte(key)),
		networkSize:         networkSize,
		peerStates:          map[peer.ID]addProviderRPCState{},
		individualThreshold: individualThreshold,
		setThreshold:        setThreshold,
		returnThreshold:     returnThreshold,
		putProvDone:         atomic.Int32{},
	}, nil
}

// optimisticProvide 执行乐观提供操作
// 参数:
//   - outerCtx: context.Context 外部上下文
//   - keyMH: multihash.Multihash 多哈希键值
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) optimisticProvide(outerCtx context.Context, keyMH multihash.Multihash) error {
	key := string(keyMH)

	if key == "" {
		return fmt.Errorf("不能查找空键值")
	}

	// 为所有putProvider操作初始化新的上下文。
	// 我们不想将外部上下文提供给put操作,因为我们在所有put操作完成之前就提前返回,以避免延迟分布的长尾。
	// 如果我们提供外部上下文,put操作可能会根据用户端上下文的情况而被取消。
	putCtx, putCtxCancel := context.WithTimeout(context.Background(), time.Minute)

	es, err := dht.newOptimisticState(putCtx, key)
	if err != nil {
		putCtxCancel()
		return err
	}

	// 初始化在此函数返回时完成的上下文
	innerCtx, innerCtxCancel := context.WithCancel(outerCtx)
	defer innerCtxCancel()

	go func() {
		select {
		case <-outerCtx.Done():
			// 如果外部上下文在我们仍在此函数中时被取消。我们停止所有待处理的put操作。
			putCtxCancel()
		case <-innerCtx.Done():
			// 我们已从此函数返回。
			// 忽略外部上下文的取消并继续剩余的put操作。
		}
	}()

	lookupRes, err := dht.runLookupWithFollowup(outerCtx, key, dht.pmGetClosestPeers(key), es.stopFn)
	if err != nil {
		return err
	}

	// 与所有我们尚未联系/调度交互的最近节点存储提供者记录。
	es.peerStatesLk.Lock()
	for _, p := range lookupRes.peers {
		if _, found := es.peerStates[p]; found {
			continue
		}

		go es.putProviderRecord(p)
		es.peerStates[p] = scheduled
	}
	es.peerStatesLk.Unlock()

	// 等待阈值数量的RPC完成
	es.waitForRPCs()

	if err := outerCtx.Err(); err != nil || !lookupRes.completed { // "completed"字段可能为false,但这不是必然的
		return err
	}

	// 由于"completed"为true,跟踪查找结果用于网络规模估算
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("网络规模估算器跟踪节点出错: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// 由于查询成功,刷新此键的cpl
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return nil
}

// stopFn 判断是否停止查询
// 参数:
//   - qps: *qpeerset.QueryPeerset 查询节点集合
//
// 返回值:
//   - bool 是否停止查询
func (os *optimisticState) stopFn(qps *qpeerset.QueryPeerset) bool {
	os.peerStatesLk.Lock()
	defer os.peerStatesLk.Unlock()

	// 获取当前已知的最近节点,并检查它们中是否有任何一个已经非常接近。
	// 如果是 -> 立即存储提供者记录。
	closest := qps.GetClosestNInStates(os.dht.bucketSize, qpeerset.PeerHeard, qpeerset.PeerWaiting, qpeerset.PeerQueried)
	distances := make([]float64, os.dht.bucketSize)
	for i, p := range closest {
		// 计算节点p到目标键的距离
		distances[i] = netsize.NormedDistance(p, os.ksKey)

		// 检查我们是否已经调度了与该节点的交互或实际上已经与其交互
		if _, found := os.peerStates[p]; found {
			continue
		}

		// 检查节点是否足够近以存储提供者记录
		if distances[i] > os.individualThreshold {
			continue
		}

		// 节点确实已经非常接近 -> 直接与其存储提供者记录！
		go os.putProviderRecord(p)

		// 跟踪我们已经调度了与该节点存储提供者记录
		os.peerStates[p] = scheduled
	}

	// 计算我们已经通过上述方法调度联系或已成功联系的节点数量
	scheduledAndSuccessCount := 0
	for _, s := range os.peerStates {
		if s == scheduled || s == success {
			scheduledAndSuccessCount += 1
		}
	}

	// 如果我们已经联系/调度了超过bucketSize个节点的RPC,则停止该过程
	if scheduledAndSuccessCount >= os.dht.bucketSize {
		return true
	}

	// 计算最近节点集的平均距离
	sum := 0.0
	for _, d := range distances {
		sum += d
	}
	avg := sum / float64(len(distances))

	// 如果平均值低于设定的阈值则停止该过程
	return avg < os.setThreshold
}

// putProviderRecord 存储提供者记录
// 参数:
//   - pid: peer.ID 节点ID
func (os *optimisticState) putProviderRecord(pid peer.ID) {
	err := os.dht.protoMessenger.PutProviderAddrs(os.putCtx, pid, []byte(os.key), peer.AddrInfo{
		ID:    os.dht.self,
		Addrs: os.dht.filterAddrs(os.dht.host.Addrs()),
	})
	os.peerStatesLk.Lock()
	if err != nil {
		os.peerStates[pid] = failure
	} else {
		os.peerStates[pid] = success
	}
	os.peerStatesLk.Unlock()

	// 指示此ADD_PROVIDER RPC已完成
	os.doneChan <- struct{}{}
}

// waitForRPCs 等待一部分ADD_PROVIDER RPC完成,然后在有界通道上获取租约以提前返回给用户并防止无界异步。
// 如果已经有太多请求在进行中,我们只是等待当前集合完成。
func (os *optimisticState) waitForRPCs() {
	os.peerStatesLk.RLock()
	rpcCount := len(os.peerStates)
	os.peerStatesLk.RUnlock()

	// returnThreshold不能大于已发出的RPC总数
	if os.returnThreshold > rpcCount {
		os.returnThreshold = rpcCount
	}

	// 等待returnThreshold个ADD_PROVIDER RPC返回
	for range os.doneChan {
		if int(os.putProvDone.Add(1)) == os.returnThreshold {
			break
		}
	}
	// 此时只有一部分ADD_PROVIDER RPC已完成。
	// 我们希望尽快将控制权返回给用户,因为至少有一个剩余的RPC很可能会超时,从而减慢整个过程。
	// 提供者记录在不到总数的RPC完成时就已经可用。这已在此处进行了研究：
	// https://github.com/protocol/network-measurements/blob/master/results/rfm17-provider-record-liveness.md

	// 对于剩余的ADD_PROVIDER RPC,尝试在optProvJobsPool通道上获取租约。
	// 如果成功,我们需要消费doneChan并释放在optProvJobsPool通道上获取的租约。
	remaining := rpcCount - int(os.putProvDone.Load())
	for i := 0; i < remaining; i++ {
		select {
		case os.dht.optProvJobsPool <- struct{}{}:
			// 我们能够在optProvJobsPool通道上获取租约。
			// 消费doneChan以再次释放获取的租约。
			go os.consumeDoneChan(rpcCount)
		case <-os.doneChan:
			// 我们无法获取租约但一个ADD_PROVIDER RPC已解决。
			if int(os.putProvDone.Add(1)) == rpcCount {
				close(os.doneChan)
			}
		}
	}
}

// consumeDoneChan 消费完成通道
// 参数:
//   - until: int 完成数量
func (os *optimisticState) consumeDoneChan(until int) {
	// 等待一个RPC完成
	<-os.doneChan

	// 释放获取的租约供其他人使用
	<-os.dht.optProvJobsPool

	// 如果所有RPC都已完成,关闭通道。
	if int(os.putProvDone.Add(1)) == until {
		close(os.doneChan)
	}
}
