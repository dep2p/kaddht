package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/peerstore"
	"github.com/dep2p/libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dep2p/kaddht/internal"
	internalConfig "github.com/dep2p/kaddht/internal/config"
	kb "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/kaddht/netsize"
	"github.com/dep2p/kaddht/qpeerset"
	record "github.com/dep2p/kaddht/record"
	u "github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// PutValue 为给定的Key添加对应的值,这是DHT的顶层"存储"操作
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - value: []byte 值
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(dhtName, ctx, key, value, opts...)
	defer func() { end(err) }()

	if !dht.enableValues {
		return routing.ErrNotSupported // 返回不支持的错误
	}

	logger.Debugw("正在存储值", "key", internal.LoggableRecordKeyString(key))

	// 不允许本地用户存储无效的值
	if err := dht.Validator.Validate(key, value); err != nil {
		return err
	}

	old, err := dht.getLocal(ctx, key)
	if err != nil {
		// 说明数据存储出现问题
		return err
	}

	// 检查是否存在与新值不同的旧值
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// 检查新值是否更好
		i, err := dht.Validator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return fmt.Errorf("不能用旧值替换新值")
		}
	}

	rec := record.MakePutRecord(key, value)
	rec.TimeReceived = u.FormatRFC3339(time.Now())
	err = dht.putLocal(ctx, key, rec)
	if err != nil {
		return err
	}

	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.Value,
				ID:   p,
			})

			err := dht.protoMessenger.PutValue(ctx, p, rec)
			if err != nil {
				logger.Debugf("向对等节点存储值失败: %s", err)
			}
		}(p)
	}
	wg.Wait()

	return nil
}

// recvdVal 存储一个值及其来源对等节点
type recvdVal struct {
	Val  []byte  // 存储的值
	From peer.ID // 来源节点ID
}

// GetValue 搜索给定Key对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - []byte 查找到的值
//   - error 错误信息
func (dht *IpfsDHT) GetValue(ctx context.Context, key string, opts ...routing.Option) (result []byte, err error) {
	ctx, end := tracer.GetValue(dhtName, ctx, key, opts...)
	defer func() { end(result, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported // 返回不支持的错误
	}

	// 应用默认法定人数(如果相关)
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, Quorum(internalConfig.GetQuorum(&cfg)))

	responses, err := dht.SearchValue(ctx, key, opts...)
	if err != nil {
		return nil, err
	}
	var best []byte

	for r := range responses {
		best = r
	}

	if ctx.Err() != nil {
		return best, ctx.Err()
	}

	if best == nil {
		return nil, routing.ErrNotFound // 返回未找到的错误
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)
	return best, nil
}

// SearchValue 搜索给定Key对应的值并流式返回结果
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - <-chan []byte 结果通道
//   - error 错误信息
func (dht *IpfsDHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (ch <-chan []byte, err error) {
	ctx, end := tracer.SearchValue(dhtName, ctx, key, opts...)
	defer func() { ch, err = end(ch, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported // 返回不支持的错误
	}

	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}

	responsesNeeded := 0
	if !cfg.Offline {
		responsesNeeded = internalConfig.GetQuorum(&cfg)
	}

	stopCh := make(chan struct{})
	valCh, lookupRes := dht.getValues(ctx, key, stopCh)

	out := make(chan []byte)
	go func() {
		defer close(out)
		best, peersWithBest, aborted := dht.searchValueQuorum(ctx, key, valCh, stopCh, out, responsesNeeded)
		if best == nil || aborted {
			return
		}

		updatePeers := make([]peer.ID, 0, dht.bucketSize)
		select {
		case l := <-lookupRes:
			if l == nil {
				return
			}

			for _, p := range l.peers {
				if _, ok := peersWithBest[p]; !ok {
					updatePeers = append(updatePeers, p)
				}
			}
		case <-ctx.Done():
			return
		}

		dht.updatePeerValues(dht.Context(), key, best, updatePeers)
	}()

	return out, nil
}

// searchValueQuorum 搜索满足法定人数要求的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - valCh: <-chan recvdVal 值通道
//   - stopCh: chan struct{} 停止通道
//   - out: chan<- []byte 输出通道
//   - nvals: int 需要的响应数
//
// 返回值:
//   - []byte 最佳值
//   - map[peer.ID]struct{} 具有最佳值的对等节点
//   - bool 是否中止
func (dht *IpfsDHT) searchValueQuorum(ctx context.Context, key string, valCh <-chan recvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v recvdVal, better bool) bool {
			numResponses++
			if better {
				select {
				case out <- v.Val:
				case <-ctx.Done():
					return false
				}
			}

			if nvals > 0 && numResponses > nvals {
				close(stopCh)
				return true
			}
			return false
		})
}

// processValues 处理接收到的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - vals: <-chan recvdVal 值通道
//   - newVal: func(context.Context, recvdVal, bool) bool 处理新值的函数
//
// 返回值:
//   - []byte 最佳值
//   - map[peer.ID]struct{} 具有最佳值的对等节点
//   - bool 是否中止
func (dht *IpfsDHT) processValues(ctx context.Context, key string, vals <-chan recvdVal,
	newVal func(ctx context.Context, v recvdVal, better bool) bool) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
loop:
	for {
		if aborted {
			return
		}

		select {
		case v, ok := <-vals:
			if !ok {
				break loop
			}

			// 选择最佳值
			if best != nil {
				if bytes.Equal(best, v.Val) {
					peersWithBest[v.From] = struct{}{}
					aborted = newVal(ctx, v, false)
					continue
				}
				sel, err := dht.Validator.Select(key, [][]byte{best, v.Val})
				if err != nil {
					logger.Warnw("选择最佳值失败", "key", internal.LoggableRecordKeyString(key), "error", err)
					continue
				}
				if sel != 1 {
					aborted = newVal(ctx, v, false)
					continue
				}
			}
			peersWithBest = make(map[peer.ID]struct{})
			peersWithBest[v.From] = struct{}{}
			best = v.Val
			aborted = newVal(ctx, v, true)
		case <-ctx.Done():
			return
		}
	}

	return
}

// updatePeerValues 更新对等节点的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - peers: []peer.ID 对等节点列表
func (dht *IpfsDHT) updatePeerValues(ctx context.Context, key string, val []byte, peers []peer.ID) {
	fixupRec := record.MakePutRecord(key, val)
	for _, p := range peers {
		go func(p peer.ID) {
			// TODO: 这是否可能?
			if p == dht.self {
				err := dht.putLocal(ctx, key, fixupRec)
				if err != nil {
					logger.Error("修正本地DHT条目时出错:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*30)
			defer cancel()
			err := dht.protoMessenger.PutValue(ctx, p, fixupRec)
			if err != nil {
				logger.Debug("修正DHT条目时出错: ", err)
			}
		}(p)
	}
}

// getValues 获取值
// 参数:
//   - ctx: context.Context 上下文
//   - key: multihash.Multihash 多哈希键
//   - stopQuery: chan struct{} 停止查询通道
//
// 返回值:
//   - <-chan recvdVal 值通道
//   - <-chan *lookupWithFollowupResult 查找结果通道
func (dht *IpfsDHT) getValues(ctx context.Context, key string, stopQuery chan struct{}) (<-chan recvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan recvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	logger.Debugw("正在查找值", "key", internal.LoggableRecordKeyString(key))

	if rec, err := dht.getLocal(ctx, key); rec != nil && err == nil {
		select {
		case valCh <- recvdVal{
			Val:  rec.GetValue(),
			From: dht.self,
		}:
		case <-ctx.Done():
		}
	}

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		lookupRes, err := dht.runLookupWithFollowup(ctx, key,
			func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
				// 用于DHT查询命令
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type: routing.SendingQuery,
					ID:   p,
				})

				rec, peers, err := dht.protoMessenger.GetValue(ctx, p, key)
				if err != nil {
					logger.Debugf("获取更近的对等节点时出错: %s", err)
					return nil, err
				}

				// 用于DHT查询命令
				routing.PublishQueryEvent(ctx, &routing.QueryEvent{
					Type:      routing.PeerResponse,
					ID:        p,
					Responses: peers,
				})

				if rec == nil {
					return peers, nil
				}

				val := rec.GetValue()
				if val == nil {
					logger.Debug("收到空记录值")
					return peers, nil
				}
				if err := dht.Validator.Validate(key, val); err != nil {
					// 确保记录有效
					logger.Debugw("收到无效记录(已丢弃)", "error", err)
					return peers, nil
				}

				// 记录存在且有效,发送进行处理
				select {
				case valCh <- recvdVal{
					Val:  val,
					From: p,
				}:
				case <-ctx.Done():
					return nil, ctx.Err()
				}

				return peers, nil
			},
			func(*qpeerset.QueryPeerset) bool {
				select {
				case <-stopQuery:
					return true
				default:
					return false
				}
			},
		)

		if err != nil {
			return
		}
		lookupResCh <- lookupRes

		if ctx.Err() == nil {
			dht.refreshRTIfNoShortcut(kb.ConvertKey(key), lookupRes)
		}
	}()

	return valCh, lookupResCh
}

// refreshRTIfNoShortcut 如果没有快捷方式则刷新路由表
// 参数:
//   - key: kb.ID 键
//   - lookupRes: *lookupWithFollowupResult 查找结果
func (dht *IpfsDHT) refreshRTIfNoShortcut(key kb.ID, lookupRes *lookupWithFollowupResult) {
	if lookupRes.completed {
		// 查询成功时刷新此键的CPL
		dht.routingTable.ResetCplRefreshedAtForID(key, time.Now())
	}
}

// Provide 使此节点宣布它可以为给定的键提供值
// 参数:
//   - ctx: context.Context 上下文
//   - key: cid.Cid CID
//   - brdcst: bool 是否广播
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) Provide(ctx context.Context, key cid.Cid, brdcst bool) (err error) {
	ctx, end := tracer.Provide(dhtName, ctx, key, brdcst)
	defer func() { end(err) }()

	if !dht.enableProviders {
		return routing.ErrNotSupported // 返回不支持的错误
	} else if !key.Defined() {
		return fmt.Errorf("无效的CID: 未定义")
	}
	keyMH := key.Hash()
	logger.Debugw("正在提供", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	// 在本地添加自己
	dht.providerStore.AddProvider(ctx, keyMH, peer.AddrInfo{ID: dht.self})
	if !brdcst {
		return nil
	}

	if dht.enableOptProv {
		err := dht.optimisticProvide(ctx, keyMH)
		if errors.Is(err, netsize.ErrNotEnoughData) {
			logger.Debugln("乐观提供的数据不足,采用经典方式")
			return dht.classicProvide(ctx, keyMH)
		}
		return err
	}
	return dht.classicProvide(ctx, keyMH)
}

// classicProvide 经典提供方式
// 参数:
//   - ctx: context.Context 上下文
//   - keyMH: multihash.Multihash 多哈希键
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) classicProvide(ctx context.Context, keyMH multihash.Multihash) error {
	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// 超时
			return context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// 为最后的put保留10%
			deadline = deadline.Add(-timeout / 10)
		} else {
			// 否则保留一秒(我们已经连接所以应该很快)
			deadline = deadline.Add(-time.Second)
		}
		var cancel context.CancelFunc
		closerCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}

	var exceededDeadline bool
	peers, err := dht.GetClosestPeers(closerCtx, string(keyMH))
	switch err {
	case context.DeadlineExceeded:
		// 如果内部截止时间已超过但外部上下文仍然正常,则向我们找到的最近的对等节点提供值,即使它们不是实际最近的对等节点
		if ctx.Err() != nil {
			return ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return err
	}

	wg := sync.WaitGroup{}
	for _, p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			logger.Debugf("putProvider(%s, %s)", internal.LoggableProviderRecordBytes(keyMH), p)
			err := dht.protoMessenger.PutProviderAddrs(ctx, p, keyMH, peer.AddrInfo{
				ID:    dht.self,
				Addrs: dht.filterAddrs(dht.host.Addrs()),
			})
			if err != nil {
				logger.Debug(err)
			}
		}(p)
	}
	wg.Wait()
	if exceededDeadline {
		return context.DeadlineExceeded
	}
	return ctx.Err()
}

// FindProviders 搜索直到上下文过期
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid CID
//
// 返回值:
//   - []peer.AddrInfo 提供者信息列表
//   - error 错误信息
func (dht *IpfsDHT) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported // 返回不支持的错误
	} else if !c.Defined() {
		return nil, fmt.Errorf("无效的CID: 未定义")
	}

	var providers []peer.AddrInfo
	for p := range dht.FindProvidersAsync(ctx, c, dht.bucketSize) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync 与FindProviders相同,但返回一个通道
// 一旦找到对等节点就会在通道上返回,甚至在搜索查询完成之前
// 如果count为零,则查询将运行直到完成。注意:不从返回的通道读取可能会阻塞查询的进度
// 参数:
//   - ctx: context.Context 上下文
//   - key: cid.Cid CID
//   - count: int 需要的提供者数量
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者信息通道
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) (ch <-chan peer.AddrInfo) {
	ctx, end := tracer.FindProvidersAsync(dhtName, ctx, key, count)
	defer func() { ch = end(ch, nil) }()

	if !dht.enableProviders || !key.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	peerOut := make(chan peer.AddrInfo)

	keyMH := key.Hash()

	logger.Debugw("正在查找提供者", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))
	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, peerOut)
	return peerOut
}

// findProvidersAsyncRoutine 异步查找提供者的例程
// 参数:
//   - ctx: context.Context 上下文
//   - key: multihash.Multihash 多哈希键
//   - count: int 需要的提供者数量
//   - peerOut: chan peer.AddrInfo 提供者输出通道
func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, peerOut chan peer.AddrInfo) {
	// 使用span因为与tracer.FindProvidersAsync不同,我们知道谁告诉我们的,这很有趣记录
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.FindProvidersAsyncRoutine")
	defer span.End()

	defer close(peerOut)

	findAll := count == 0

	ps := make(map[peer.ID]peer.AddrInfo)
	psLock := &sync.Mutex{}
	psTryAdd := func(p peer.AddrInfo) bool {
		psLock.Lock()
		defer psLock.Unlock()
		pi, ok := ps[p.ID]
		if (!ok || ((len(pi.Addrs) == 0) && len(p.Addrs) > 0)) && (len(ps) < count || findAll) {
			ps[p.ID] = p
			return true
		}
		return false
	}
	psSize := func() int {
		psLock.Lock()
		defer psLock.Unlock()
		return len(ps)
	}

	provs, err := dht.providerStore.GetProviders(ctx, key)
	if err != nil {
		return
	}
	for _, p := range provs {
		// 注意:假设此对等节点列表是唯一的
		if psTryAdd(p) {
			select {
			case peerOut <- p:
				// 为找到提供者添加跟踪事件
				span.AddEvent("找到提供者", trace.WithAttributes(
					attribute.Stringer("peer", p.ID),
					attribute.Stringer("from", dht.self),
					attribute.Int("provider_addrs_count", len(p.Addrs)),
					attribute.Bool("found_in_provider_store", true),
				))
			case <-ctx.Done():
				return
			}
		}

		// 如果我们在本地有足够的对等节点,就不要使用远程RPC
		// TODO: 这是DOS向量吗?
		if !findAll && len(ps) >= count {
			return
		}
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(key),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {

			// 用于DHT查询命令
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			provs, closest, err := dht.protoMessenger.GetProviders(ctx, p, key)
			if err != nil {
				return nil, err
			}

			logger.Debugf("%d 个提供者条目", len(provs))

			// 从请求添加唯一提供者,最多'count'个
			for _, prov := range provs {
				dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
				logger.Debugf("获得提供者: %s", prov)
				if psTryAdd(*prov) {
					logger.Debugf("使用提供者: %s", prov)
					select {
					case peerOut <- *prov:
						span.AddEvent("找到提供者", trace.WithAttributes(
							attribute.Stringer("peer", prov.ID),
							attribute.Stringer("from", p),
							attribute.Int("provider_addrs_count", len(prov.Addrs)),
						))
					case <-ctx.Done():
						logger.Debug("发送更多提供者时上下文超时")
						return nil, ctx.Err()
					}
				}
				if !findAll && psSize() >= count {
					logger.Debugf("获得足够的提供者 (%d/%d)", psSize(), count)
					return nil, nil
				}
			}

			// 将更近的对等节点返回给查询以进行查询
			logger.Debugf("获得更近的对等节点: %d %s", len(closest), closest)

			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: closest,
			})

			return closest, nil
		},
		func(*qpeerset.QueryPeerset) bool {
			return !findAll && psSize() >= count
		},
	)

	if err == nil && ctx.Err() == nil {
		dht.refreshRTIfNoShortcut(kb.ConvertKey(string(key)), lookupRes)
	}
}

// FindPeer 搜索具有给定ID的对等节点
// 参数:
//   - ctx: context.Context 上下文
//   - id: peer.ID 对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点信息
//   - error 错误信息
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (pi peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(dhtName, ctx, id)
	defer func() { end(pi, err) }()

	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}

	logger.Debugw("正在查找对等节点", "peer", id)

	// 检查是否已经连接到它们
	if pi := dht.FindLocal(ctx, id); pi.ID != "" {
		return pi, nil
	}

	lookupRes, err := dht.runLookupWithFollowup(ctx, string(id),
		func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
			// 用于DHT查询命令
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, id)
			if err != nil {
				logger.Debugf("获取更近的对等节点时出错: %s", err)
				return nil, err
			}

			// 用于DHT查询命令
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			return peers, err
		},
		func(*qpeerset.QueryPeerset) bool {
			return hasValidConnectedness(dht.host, id)
		},
	)

	if err != nil {
		return peer.AddrInfo{}, err
	}

	dialedPeerDuringQuery := false
	for i, p := range lookupRes.peers {
		if p == id {
			// 注意:我们认为PeerUnreachable是一个有效状态,因为对等节点可能不支持DHT协议,因此对等节点会查询失败
			// 返回的对等节点可能是非DHT服务器对等节点且未被识别为此类是一个错误
			dialedPeerDuringQuery = (lookupRes.state[i] == qpeerset.PeerQueried || lookupRes.state[i] == qpeerset.PeerUnreachable || lookupRes.state[i] == qpeerset.PeerWaiting)
			break
		}
	}

	// 如果我们在查询期间尝试拨号对等节点或者我们(或最近)连接到对等节点,则返回对等节点信息
	// Return peer information if we tried to dial the peer during the query or we are (or recently were) connected to the peer.
	if dialedPeerDuringQuery || hasValidConnectedness(dht.host, id) {
		return dht.peerstore.PeerInfo(id), nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}
