package dht

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	pstore "github.com/dep2p/libp2p/core/peerstore"
	"github.com/dep2p/libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dep2p/kaddht/internal"
	kb "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/kaddht/qpeerset"
	"github.com/google/uuid"
)

// ErrNoPeersQueried 表示未能连接到任何节点
var ErrNoPeersQueried = errors.New("failed to query any peers")

// queryFn 定义了查询单个节点的函数类型
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 要查询的节点ID
//
// 返回值:
//   - []*peer.AddrInfo 查询到的节点地址信息
//   - error 错误信息
type queryFn func(context.Context, peer.ID) ([]*peer.AddrInfo, error)

// stopFn 定义了判断是否停止查询的函数类型
// 参数:
//   - qps: *qpeerset.QueryPeerset 查询节点集合
//
// 返回值:
//   - bool 是否停止查询
type stopFn func(*qpeerset.QueryPeerset) bool

// query 表示一个DHT查询
type query struct {
	// id 查询实例的唯一标识符
	id uuid.UUID

	// key 查询的目标键
	key string

	// ctx 查询的上下文
	ctx context.Context

	// dht DHT实例
	dht *IpfsDHT

	// seedPeers 作为查询种子的节点集合
	seedPeers []peer.ID

	// peerTimes 记录每个成功查询的节点耗时
	peerTimes map[peer.ID]time.Duration

	// queryPeers 查询已知的节点集合及其状态
	queryPeers *qpeerset.QueryPeerset

	// terminated 当第一个工作线程遇到终止条件时设置
	// 用于确保一旦确定终止就保持终止状态
	terminated bool

	// waitGroup 确保在所有查询goroutine完成前查找不会结束
	waitGroup sync.WaitGroup

	// queryFn 用于查询单个节点的函数
	queryFn queryFn

	// stopFn 用于判断是否停止整个不相交查询
	stopFn stopFn
}

// lookupWithFollowupResult 表示带后续操作的查找结果
type lookupWithFollowupResult struct {
	peers   []peer.ID            // 查询结束时最近的K个可达节点
	state   []qpeerset.PeerState // peers切片中节点在查询结束时的状态(非最近)
	closest []peer.ID            // 查询结束时最近的K个节点

	// completed 表示查找和后续操作是否被外部条件(如上下文取消或停止函数调用)提前终止
	completed bool
}

// runLookupWithFollowup 使用给定的查询函数执行目标查找,直到上下文被取消或停止函数返回true
// 注意:如果停止函数不是粘性的(即第一次返回true后不总是返回true),则不能保证在其暂时返回true时就会导致停止
//
// 参数:
//   - ctx: context.Context 上下文
//   - target: string 目标键
//   - queryFn: queryFn 查询函数
//   - stopFn: stopFn 停止函数
//
// 返回值:
//   - *lookupWithFollowupResult 查找结果
//   - error 错误信息
func (dht *IpfsDHT) runLookupWithFollowup(ctx context.Context, target string, queryFn queryFn, stopFn stopFn) (*lookupWithFollowupResult, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.RunLookupWithFollowup", trace.WithAttributes(internal.KeyAsAttribute("Target", target)))
	defer span.End()

	// 执行查询
	lookupRes, qps, err := dht.runQuery(ctx, target, queryFn, stopFn)
	if err != nil {
		return nil, err
	}

	// 查询所有最近K个我们听说过或正在等待查询的节点
	// 这确保所有最近K个结果都被查询过,这增加了对于携带状态的查询函数(如FindProviders和GetValue)的抗波动性,
	// 并建立了无状态查询函数(如GetClosestPeers,因此也包括Provide和PutValue)所需的连接
	queryPeers := make([]peer.ID, 0, len(lookupRes.peers))
	for i, p := range lookupRes.peers {
		if state := lookupRes.state[i]; state == qpeerset.PeerHeard || state == qpeerset.PeerWaiting {
			queryPeers = append(queryPeers, p)
		}
	}

	if len(queryPeers) == 0 {
		return lookupRes, nil
	}

	// 如果查找被外部停止则返回
	if ctx.Err() != nil || stopFn(qps) {
		lookupRes.completed = false
		return lookupRes, nil
	}

	doneCh := make(chan struct{}, len(queryPeers))
	followUpCtx, cancelFollowUp := context.WithCancel(ctx)
	defer cancelFollowUp()
	for _, p := range queryPeers {
		qp := p
		go func() {
			_, _ = queryFn(followUpCtx, qp)
			doneCh <- struct{}{}
		}()
	}

	// 等待所有查询完成后返回,如果被外部停止则中止正在进行的查询
	followupsCompleted := 0
processFollowUp:
	for i := 0; i < len(queryPeers); i++ {
		select {
		case <-doneCh:
			followupsCompleted++
			if stopFn(qps) {
				cancelFollowUp()
				if i < len(queryPeers)-1 {
					lookupRes.completed = false
				}
				break processFollowUp
			}
		case <-ctx.Done():
			lookupRes.completed = false
			cancelFollowUp()
			break processFollowUp
		}
	}

	if !lookupRes.completed {
		for i := followupsCompleted; i < len(queryPeers); i++ {
			<-doneCh
		}
	}

	return lookupRes, nil
}

// runQuery 执行查询操作
// 参数:
//   - ctx: context.Context 上下文
//   - target: string 目标键
//   - queryFn: queryFn 查询函数
//   - stopFn: stopFn 停止函数
//
// 返回值:
//   - *lookupWithFollowupResult 查找结果
//   - *qpeerset.QueryPeerset 查询节点集合
//   - error 错误信息
func (dht *IpfsDHT) runQuery(ctx context.Context, target string, queryFn queryFn, stopFn stopFn) (*lookupWithFollowupResult, *qpeerset.QueryPeerset, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.RunQuery")
	defer span.End()

	// 从路由表中选择K个离目标键最近的节点
	targetKadID := kb.ConvertKey(target)
	seedPeers := dht.routingTable.NearestPeers(targetKadID, dht.bucketSize)
	if len(seedPeers) == 0 {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: kb.ErrLookupFailure.Error(),
		})
		return nil, nil, kb.ErrLookupFailure
	}

	q := &query{
		id:         uuid.New(),
		key:        target,
		ctx:        ctx,
		dht:        dht,
		queryPeers: qpeerset.NewQueryPeerset(target),
		seedPeers:  seedPeers,
		peerTimes:  make(map[peer.ID]time.Duration),
		terminated: false,
		queryFn:    queryFn,
		stopFn:     stopFn,
	}

	// 执行查询
	q.run()

	if ctx.Err() == nil {
		q.recordValuablePeers()
	}

	res := q.constructLookupResult(targetKadID)
	return res, q.queryPeers, nil
}

// recordPeerIsValuable 记录节点是否有价值
// 参数:
//   - p: peer.ID 要记录的节点ID
func (q *query) recordPeerIsValuable(p peer.ID) {
	if !q.dht.routingTable.UpdateLastUsefulAt(p, time.Now()) {
		// 不在路由表中
		return
	}
}

// recordValuablePeers 记录有价值的节点
// 有价值节点算法:
// 1. 将响应查询最快的种子节点标记为"最有价值节点"(MVP)
// 2. 响应时间在MVP时间2倍范围内的种子节点为有价值节点
// 3. 将MVP和所有其他有价值节点标记为有价值
func (q *query) recordValuablePeers() {
	mvpDuration := time.Duration(math.MaxInt64)
	for _, p := range q.seedPeers {
		if queryTime, ok := q.peerTimes[p]; ok && queryTime < mvpDuration {
			mvpDuration = queryTime
		}
	}

	for _, p := range q.seedPeers {
		if queryTime, ok := q.peerTimes[p]; ok && queryTime < mvpDuration*2 {
			q.recordPeerIsValuable(p)
		}
	}
}

// constructLookupResult 使用查询信息构造查找结果
// 参数:
//   - target: kb.ID 目标ID
//
// 返回值:
//   - *lookupWithFollowupResult 查找结果
func (q *query) constructLookupResult(target kb.ID) *lookupWithFollowupResult {
	// 判断查询是否提前终止
	completed := true

	// 查找和饥饿都是查找完成的有效方式(饥饿不意味着失败)
	// 在小型网络中不可能出现查找终止(由isLookupTermination定义)
	// 在小型网络中饥饿是成功的查询终止
	if !(q.isLookupTermination() || q.isStarvationTermination()) {
		completed = false
	}

	// 提取最近K个可达节点
	var peers []peer.ID
	peerState := make(map[peer.ID]qpeerset.PeerState)
	qp := q.queryPeers.GetClosestNInStates(q.dht.bucketSize, qpeerset.PeerHeard, qpeerset.PeerWaiting, qpeerset.PeerQueried)
	for _, p := range qp {
		state := q.queryPeers.GetState(p)
		peerState[p] = state
		peers = append(peers, p)
	}

	// 获取总体最近K个节点
	sortedPeers := kb.SortClosestPeers(peers, target)
	if len(sortedPeers) > q.dht.bucketSize {
		sortedPeers = sortedPeers[:q.dht.bucketSize]
	}

	closest := q.queryPeers.GetClosestNInStates(q.dht.bucketSize, qpeerset.PeerHeard, qpeerset.PeerWaiting, qpeerset.PeerQueried, qpeerset.PeerUnreachable)

	// 返回最近K个可达节点及其查询结束时的状态
	res := &lookupWithFollowupResult{
		peers:     sortedPeers,
		state:     make([]qpeerset.PeerState, len(sortedPeers)),
		completed: completed,
		closest:   closest,
	}

	for i, p := range sortedPeers {
		res.state[i] = peerState[p]
	}

	return res
}

// queryUpdate 表示查询更新信息
type queryUpdate struct {
	cause         peer.ID       // 导致更新的节点
	queried       []peer.ID     // 已查询的节点
	heard         []peer.ID     // 听说的节点
	unreachable   []peer.ID     // 不可达的节点
	queryDuration time.Duration // 查询耗时
}

// run 执行查询操作
func (q *query) run() {
	ctx, span := internal.StartSpan(q.ctx, "IpfsDHT.Query.Run")
	defer span.End()

	pathCtx, cancelPath := context.WithCancel(ctx)
	defer cancelPath()

	alpha := q.dht.alpha

	ch := make(chan *queryUpdate, alpha)
	ch <- &queryUpdate{cause: q.dht.self, heard: q.seedPeers}

	// 仅在所有未完成的查询完成后返回
	defer q.waitGroup.Wait()
	for {
		var cause peer.ID
		select {
		case update := <-ch:
			q.updateState(pathCtx, update)
			cause = update.cause
		case <-pathCtx.Done():
			q.terminate(pathCtx, cancelPath, LookupCancelled)
		}

		// 计算可能产生的最大查询数
		// 注意:NumWaiting将在spawnQuery中更新
		maxNumQueriesToSpawn := alpha - q.queryPeers.NumWaiting()

		// 在查找结束条件或未使用节点饥饿时触发终止
		// 同时返回下一步要查询的节点,最多返回maxNumQueriesToSpawn个节点
		ready, reason, qPeers := q.isReadyToTerminate(pathCtx, maxNumQueriesToSpawn)
		if ready {
			q.terminate(pathCtx, cancelPath, reason)
		}

		if q.terminated {
			return
		}

		// 尝试启动查询,如果没有可用节点则不会启动
		for _, p := range qPeers {
			q.spawnQuery(pathCtx, cause, p, ch)
		}
	}
}

// spawnQuery 如果找到可用的已知节点,则启动一个查询
// 参数:
//   - ctx: context.Context 上下文
//   - cause: peer.ID 导致查询的节点
//   - queryPeer: peer.ID 要查询的节点
//   - ch: chan<- *queryUpdate 查询更新通道
func (q *query) spawnQuery(ctx context.Context, cause peer.ID, queryPeer peer.ID, ch chan<- *queryUpdate) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.SpawnQuery", trace.WithAttributes(
		attribute.String("Cause", cause.String()),
		attribute.String("QueryPeer", queryPeer.String()),
	))
	defer span.End()

	PublishLookupEvent(ctx,
		NewLookupEvent(
			q.dht.self,
			q.id,
			q.key,
			NewLookupUpdateEvent(
				cause,
				q.queryPeers.GetReferrer(queryPeer),
				nil,                  // heard
				[]peer.ID{queryPeer}, // waiting
				nil,                  // queried
				nil,                  // unreachable
			),
			nil,
			nil,
		),
	)
	q.queryPeers.SetState(queryPeer, qpeerset.PeerWaiting)
	q.waitGroup.Add(1)
	go q.queryPeer(ctx, ch, queryPeer)
}

// isReadyToTerminate 判断是否准备好终止查询
// 参数:
//   - ctx: context.Context 上下文
//   - nPeersToQuery: int 要查询的节点数
//
// 返回值:
//   - bool 是否准备好终止
//   - LookupTerminationReason 终止原因
//   - []peer.ID 要查询的节点列表
func (q *query) isReadyToTerminate(ctx context.Context, nPeersToQuery int) (bool, LookupTerminationReason, []peer.ID) {
	// 给应用逻辑一个终止的机会
	if q.stopFn(q.queryPeers) {
		return true, LookupStopped, nil
	}
	if q.isStarvationTermination() {
		return true, LookupStarvation, nil
	}
	if q.isLookupTermination() {
		return true, LookupCompleted, nil
	}

	// 下一步要查询的节点应该是我们只听说过的节点
	var peersToQuery []peer.ID
	peers := q.queryPeers.GetClosestInStates(qpeerset.PeerHeard)
	count := 0
	for _, p := range peers {
		peersToQuery = append(peersToQuery, p)
		count++
		if count == nPeersToQuery {
			break
		}
	}

	return false, -1, peersToQuery
}

// isLookupTermination 判断是否满足查找终止条件
// 从所有非不可达节点中,如果最近的beta个节点都已查询,则查找可以终止
// 返回值:
//   - bool 是否满足查找终止条件
func (q *query) isLookupTermination() bool {
	peers := q.queryPeers.GetClosestNInStates(q.dht.beta, qpeerset.PeerHeard, qpeerset.PeerWaiting, qpeerset.PeerQueried)
	for _, p := range peers {
		if q.queryPeers.GetState(p) != qpeerset.PeerQueried {
			return false
		}
	}
	return true
}

// isStarvationTermination 判断是否满足饥饿终止条件
// 返回值:
//   - bool 是否满足饥饿终止条件
func (q *query) isStarvationTermination() bool {
	return q.queryPeers.NumHeard() == 0 && q.queryPeers.NumWaiting() == 0
}

// terminate 终止查询
// 参数:
//   - ctx: context.Context 上下文
//   - cancel: context.CancelFunc 取消函数
//   - reason: LookupTerminationReason 终止原因
func (q *query) terminate(ctx context.Context, cancel context.CancelFunc, reason LookupTerminationReason) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.Query.Terminate", trace.WithAttributes(attribute.Stringer("Reason", reason)))
	defer span.End()

	if q.terminated {
		return
	}

	PublishLookupEvent(ctx,
		NewLookupEvent(
			q.dht.self,
			q.id,
			q.key,
			nil,
			nil,
			NewLookupTerminateEvent(reason),
		),
	)
	cancel() // 中止未完成的查询
	q.terminated = true
}

// queryPeer 查询单个节点并在通道上报告结果
// queryPeer不访问queryPeers中的查询状态!
// 参数:
//   - ctx: context.Context 上下文
//   - ch: chan<- *queryUpdate 查询更新通道
//   - p: peer.ID 要查询的节点
func (q *query) queryPeer(ctx context.Context, ch chan<- *queryUpdate, p peer.ID) {
	defer q.waitGroup.Done()

	ctx, span := internal.StartSpan(ctx, "IpfsDHT.QueryPeer")
	defer span.End()

	dialCtx, queryCtx := ctx, ctx

	// 连接节点
	if err := q.dht.dialPeer(dialCtx, p); err != nil {
		// 如果连接失败(不是因为上下文取消)则移除节点
		if dialCtx.Err() == nil {
			q.dht.peerStoppedDHT(p)
		}
		ch <- &queryUpdate{cause: p, unreachable: []peer.ID{p}}
		return
	}

	startQuery := time.Now()
	// 向远程节点发送查询RPC
	newPeers, err := q.queryFn(queryCtx, p)
	if err != nil {
		if queryCtx.Err() == nil {
			q.dht.peerStoppedDHT(p)
		}
		ch <- &queryUpdate{cause: p, unreachable: []peer.ID{p}}
		return
	}

	queryDuration := time.Since(startQuery)

	// 查询成功,尝试添加到路由表
	q.dht.validPeerFound(p)

	// 处理新节点
	saw := []peer.ID{}
	for _, next := range newPeers {
		if next.ID == q.dht.self { // 不添加自己
			logger.Debugf("PEERS CLOSER -- worker for: %v found self", p)
			continue
		}

		// 添加候选节点的任何其他已知地址
		curInfo := q.dht.peerstore.PeerInfo(next.ID)
		next.Addrs = append(next.Addrs, curInfo.Addrs...)

		// 将地址添加到拨号器的peerstore
		//
		// 如果节点匹配查询目标,即使它不符合查询过滤器也将其添加到查询中
		// TODO: 这个行为特别适用于FindPeer的工作方式,而不是GetClosestPeers或任何其他函数
		isTarget := string(next.ID) == q.key
		if isTarget || q.dht.queryPeerFilter(q.dht, *next) {
			q.dht.maybeAddAddrs(next.ID, next.Addrs, pstore.TempAddrTTL)
			saw = append(saw, next.ID)
		}
	}

	ch <- &queryUpdate{cause: p, heard: saw, queried: []peer.ID{p}, queryDuration: queryDuration}
}

// updateState 更新查询状态
// 参数:
//   - ctx: context.Context 上下文
//   - up: *queryUpdate 查询更新信息
func (q *query) updateState(ctx context.Context, up *queryUpdate) {
	if q.terminated {
		panic("update should not be invoked after the logical lookup termination")
	}
	PublishLookupEvent(ctx,
		NewLookupEvent(
			q.dht.self,
			q.id,
			q.key,
			nil,
			NewLookupUpdateEvent(
				up.cause,
				up.cause,
				up.heard,       // heard
				nil,            // waiting
				up.queried,     // queried
				up.unreachable, // unreachable
			),
			nil,
		),
	)
	for _, p := range up.heard {
		if p == q.dht.self { // 不添加自己
			continue
		}
		q.queryPeers.TryAdd(p, up.cause)
	}
	for _, p := range up.queried {
		if p == q.dht.self { // 不添加自己
			continue
		}
		if st := q.queryPeers.GetState(p); st == qpeerset.PeerWaiting {
			q.queryPeers.SetState(p, qpeerset.PeerQueried)
			q.peerTimes[p] = up.queryDuration
		} else {
			panic(fmt.Errorf("kademlia protocol error: tried to transition to the queried state from state %v", st))
		}
	}
	for _, p := range up.unreachable {
		if p == q.dht.self { // 不添加自己
			continue
		}

		if st := q.queryPeers.GetState(p); st == qpeerset.PeerWaiting {
			q.queryPeers.SetState(p, qpeerset.PeerUnreachable)
		} else {
			panic(fmt.Errorf("kademlia protocol error: tried to transition to the unreachable state from state %v", st))
		}
	}
}

// dialPeer 连接节点
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 要连接的节点ID
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) dialPeer(ctx context.Context, p peer.ID) error {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.DialPeer", trace.WithAttributes(attribute.String("PeerID", p.String())))
	defer span.End()

	// 如果已连接则快速返回
	if dht.host.Network().Connectedness(p) == network.Connected {
		return nil
	}

	logger.Debug("not connected. dialing.")
	routing.PublishQueryEvent(ctx, &routing.QueryEvent{
		Type: routing.DialingPeer,
		ID:   p,
	})

	pi := peer.AddrInfo{ID: p}
	if err := dht.host.Connect(ctx, pi); err != nil {
		logger.Debugf("error connecting: %s", err)
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:  routing.QueryError,
			Extra: err.Error(),
			ID:    p,
		})

		return err
	}
	logger.Debugf("connected. dial success.")
	return nil
}
