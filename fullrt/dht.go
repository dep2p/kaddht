package fullrt

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/multiformats/go-base32"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"

	"github.com/dep2p/kaddht/tracing"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/peerstore"
	"github.com/dep2p/libp2p/core/protocol"
	"github.com/dep2p/libp2p/core/routing"
	swarm "github.com/dep2p/libp2p/p2p/net/swarm"

	"github.com/gogo/protobuf/proto"
	u "github.com/ipfs/boxo/util"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	logging "github.com/ipfs/go-log/v2"

	kaddht "github.com/dep2p/kaddht"
	"github.com/dep2p/kaddht/crawler"
	"github.com/dep2p/kaddht/internal"
	internalConfig "github.com/dep2p/kaddht/internal/config"
	"github.com/dep2p/kaddht/internal/net"
	kb "github.com/dep2p/kaddht/kbucket"
	dht_pb "github.com/dep2p/kaddht/pb"
	"github.com/dep2p/kaddht/providers"

	record "github.com/dep2p/kaddht/record"
	recpb "github.com/dep2p/kaddht/record/pb"

	"github.com/dep2p/kaddht/xor/kademlia"
	kadkey "github.com/dep2p/kaddht/xor/key"
	"github.com/dep2p/kaddht/xor/trie"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var logger = logging.Logger("fullrtdht")

const tracer = tracing.Tracer("dep2p-kad-dht/fullrt")
const dhtName = "FullRT"

const rtRefreshLimitsMsg = `由于资源管理器限制，加速的DHT客户端无法完全刷新其路由表，这可能会降低内容路由性能。请考虑增加资源限制。有关详细信息，请参阅"dht-crawler"子系统的调试日志。`

// FullRT 是一个正在开发中的实验性DHT客户端。在稳定之前,此客户端可能会发生重大变更。
type FullRT struct {
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	enableValues, enableProviders bool
	Validator                     record.Validator
	ProviderManager               *providers.ProviderManager
	datastore                     ds.Datastore
	h                             host.Host

	crawlerInterval time.Duration
	lastCrawlTime   time.Time

	crawler        crawler.Crawler
	protoMessenger *dht_pb.ProtocolMessenger
	messageSender  dht_pb.MessageSender

	filterFromTable kaddht.QueryFilterFunc
	rtLk            sync.RWMutex
	rt              *trie.Trie

	kMapLk       sync.RWMutex
	keyToPeerMap map[string]peer.ID

	peerAddrsLk sync.RWMutex
	peerAddrs   map[peer.ID][]multiaddr.Multiaddr

	bootstrapPeers []*peer.AddrInfo

	bucketSize int

	triggerRefresh chan struct{}

	waitFrac     float64
	timeoutPerOp time.Duration

	bulkSendParallelism int

	self peer.ID
}

// NewFullRT 创建一个跟踪整个网络的DHT客户端
// 参数:
//   - h: host.Host 主机实例
//   - protocolPrefix: protocol.ID 协议前缀,例如/ipfs/kad/1.0.0的前缀是/ipfs
//   - options: ...Option 配置选项
//
// 返回值:
//   - *FullRT DHT客户端实例
//   - error 错误信息
//
// 注意: FullRT是一个正在开发中的实验性DHT客户端。在稳定之前,此客户端可能会发生重大变更。
// 并非所有标准DHT选项都在此DHT中受支持。
func NewFullRT(h host.Host, protocolPrefix protocol.ID, options ...Option) (*FullRT, error) {
	fullrtcfg := config{
		crawlInterval:       time.Hour,
		bulkSendParallelism: 20,
		waitFrac:            0.3,
		timeoutPerOp:        5 * time.Second,
	}
	if err := fullrtcfg.apply(options...); err != nil {
		return nil, err
	}

	dhtcfg := &internalConfig.Config{
		Datastore:        dssync.MutexWrap(ds.NewMapDatastore()),
		Validator:        record.NamespacedValidator{},
		ValidatorChanged: false,
		EnableProviders:  true,
		EnableValues:     true,
		ProtocolPrefix:   protocolPrefix,
	}

	if err := dhtcfg.Apply(fullrtcfg.dhtOpts...); err != nil {
		return nil, err
	}
	if err := dhtcfg.ApplyFallbacks(h); err != nil {
		return nil, err
	}

	if err := dhtcfg.Validate(); err != nil {
		return nil, err
	}

	ms := net.NewMessageSenderImpl(h, []protocol.ID{dhtcfg.ProtocolPrefix + "/kad/1.0.0"})
	protoMessenger, err := dht_pb.NewProtocolMessenger(ms)
	if err != nil {
		return nil, err
	}

	if fullrtcfg.crawler == nil {
		fullrtcfg.crawler, err = crawler.NewDefaultCrawler(h, crawler.WithParallelism(200))
		if err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithCancel(context.Background())

	self := h.ID()
	pm, err := providers.NewProviderManager(self, h.Peerstore(), dhtcfg.Datastore, fullrtcfg.pmOpts...)
	if err != nil {
		cancel()
		return nil, err
	}

	var bsPeers []*peer.AddrInfo

	for _, ai := range dhtcfg.BootstrapPeers() {
		tmpai := ai
		bsPeers = append(bsPeers, &tmpai)
	}

	rt := &FullRT{
		ctx:    ctx,
		cancel: cancel,

		enableValues:    dhtcfg.EnableValues,
		enableProviders: dhtcfg.EnableProviders,
		Validator:       dhtcfg.Validator,
		ProviderManager: pm,
		datastore:       dhtcfg.Datastore,
		h:               h,
		crawler:         fullrtcfg.crawler,
		messageSender:   ms,
		protoMessenger:  protoMessenger,
		filterFromTable: kaddht.PublicQueryFilter,
		rt:              trie.New(),
		keyToPeerMap:    make(map[string]peer.ID),
		bucketSize:      dhtcfg.BucketSize,

		peerAddrs:      make(map[peer.ID][]multiaddr.Multiaddr),
		bootstrapPeers: bsPeers,

		triggerRefresh: make(chan struct{}),

		waitFrac:     fullrtcfg.waitFrac,
		timeoutPerOp: fullrtcfg.timeoutPerOp,

		crawlerInterval: fullrtcfg.crawlInterval,

		bulkSendParallelism: fullrtcfg.bulkSendParallelism,

		self: self,
	}

	rt.wg.Add(1)
	go rt.runCrawler(ctx)

	return rt, nil
}

type crawlVal struct {
	addrs []multiaddr.Multiaddr
	key   kadkey.Key
}

// TriggerRefresh 触发路由表刷新
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) TriggerRefresh(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case dht.triggerRefresh <- struct{}{}:
		return nil
	case <-dht.ctx.Done():
		return fmt.Errorf("dht已关闭")
	}
}

// Stat 返回当前路由表状态
// 返回值:
//   - map[string]peer.ID 键到节点ID的映射
func (dht *FullRT) Stat() map[string]peer.ID {
	newMap := make(map[string]peer.ID)

	dht.kMapLk.RLock()
	for k, v := range dht.keyToPeerMap {
		newMap[k] = v
	}
	dht.kMapLk.RUnlock()
	return newMap
}

// Ready 检查DHT客户端是否就绪
// 返回值:
//   - bool 是否就绪
func (dht *FullRT) Ready() bool {
	dht.rtLk.RLock()
	lastCrawlTime := dht.lastCrawlTime
	dht.rtLk.RUnlock()

	if time.Since(lastCrawlTime) > dht.crawlerInterval {
		return false
	}

	// TODO: 需要更好地定义此函数。可能基于遍历节点映射并查看上次连接到任何节点的时间。
	dht.peerAddrsLk.RLock()
	rtSize := len(dht.keyToPeerMap)
	dht.peerAddrsLk.RUnlock()

	return rtSize > len(dht.bootstrapPeers)+1
}

// Host 返回主机实例
// 返回值:
//   - host.Host 主机实例
func (dht *FullRT) Host() host.Host {
	return dht.h
}

// runCrawler 运行爬虫
// 参数:
//   - ctx: context.Context 上下文
func (dht *FullRT) runCrawler(ctx context.Context) {
	defer dht.wg.Done()
	t := time.NewTicker(dht.crawlerInterval)

	m := make(map[peer.ID]*crawlVal)
	mxLk := sync.Mutex{}

	initialTrigger := make(chan struct{}, 1)
	initialTrigger <- struct{}{}

	for {
		select {
		case <-t.C:
		case <-initialTrigger:
		case <-dht.triggerRefresh:
		case <-ctx.Done():
			return
		}

		var addrs []*peer.AddrInfo
		dht.peerAddrsLk.Lock()
		for k := range m {
			addrs = append(addrs, &peer.AddrInfo{ID: k}) // Addrs: v.addrs
		}

		addrs = append(addrs, dht.bootstrapPeers...)
		dht.peerAddrsLk.Unlock()

		for k := range m {
			delete(m, k)
		}

		start := time.Now()
		limitErrOnce := sync.Once{}
		dht.crawler.Run(ctx, addrs,
			func(p peer.ID, rtPeers []*peer.AddrInfo) {
				conns := dht.h.Network().ConnsToPeer(p)
				var addrs []multiaddr.Multiaddr
				for _, conn := range conns {
					addr := conn.RemoteMultiaddr()
					addrs = append(addrs, addr)
				}

				if len(addrs) == 0 {
					logger.Debugf("成功查询后无法连接到 %v。保留peerstore中的地址", p)
					addrs = dht.h.Peerstore().Addrs(p)
				}

				keep := kaddht.PublicRoutingTableFilter(dht, p)
				if !keep {
					return
				}

				mxLk.Lock()
				defer mxLk.Unlock()
				m[p] = &crawlVal{
					addrs: addrs,
				}
			},
			func(p peer.ID, err error) {
				dialErr, ok := err.(*swarm.DialError)
				if ok {
					for _, transportErr := range dialErr.DialErrors {
						if errors.Is(transportErr.Cause, network.ErrResourceLimitExceeded) {
							limitErrOnce.Do(func() { logger.Errorf(rtRefreshLimitsMsg) })
						}
					}
				}
				// 注意DialError实现了Unwrap()返回Cause,所以这里也涵盖了这种情况
				if errors.Is(err, network.ErrResourceLimitExceeded) {
					limitErrOnce.Do(func() { logger.Errorf(rtRefreshLimitsMsg) })
				}
			})
		dur := time.Since(start)
		logger.Infof("爬取耗时 %v", dur)

		peerAddrs := make(map[peer.ID][]multiaddr.Multiaddr)
		kPeerMap := make(map[string]peer.ID)
		newRt := trie.New()
		for k, v := range m {
			v.key = kadkey.KbucketIDToKey(kb.ConvertPeerID(k))
			peerAddrs[k] = v.addrs
			kPeerMap[string(v.key)] = k
			newRt.Add(v.key)
		}

		dht.peerAddrsLk.Lock()
		dht.peerAddrs = peerAddrs
		dht.peerAddrsLk.Unlock()

		dht.kMapLk.Lock()
		dht.keyToPeerMap = kPeerMap
		dht.kMapLk.Unlock()

		dht.rtLk.Lock()
		dht.rt = newRt
		dht.lastCrawlTime = time.Now()
		dht.rtLk.Unlock()
	}
}

// Close 关闭DHT客户端
// 返回值:
//   - error 错误信息
func (dht *FullRT) Close() error {
	dht.cancel()
	dht.wg.Wait()
	return dht.ProviderManager.Close()
}

// Bootstrap 引导DHT客户端
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) Bootstrap(ctx context.Context) (err error) {
	_, end := tracer.Bootstrap(dhtName, ctx)
	defer func() { end(err) }()

	// TODO: 这应该阻塞直到第一次爬取完成。

	return nil
}

// CheckPeers 检查节点连接状态
// 参数:
//   - ctx: context.Context 上下文
//   - peers: ...peer.ID 要检查的节点ID列表
//
// 返回值:
//   - int 成功连接的节点数
//   - int 总节点数
func (dht *FullRT) CheckPeers(ctx context.Context, peers ...peer.ID) (int, int) {
	ctx, span := internal.StartSpan(ctx, "FullRT.CheckPeers", trace.WithAttributes(attribute.Int("NumPeers", len(peers))))
	defer span.End()

	var peerAddrs chan interface{}
	var total int
	if len(peers) == 0 {
		dht.peerAddrsLk.RLock()
		total = len(dht.peerAddrs)
		peerAddrs = make(chan interface{}, total)
		for k, v := range dht.peerAddrs {
			peerAddrs <- peer.AddrInfo{
				ID:    k,
				Addrs: v,
			}
		}
		close(peerAddrs)
		dht.peerAddrsLk.RUnlock()
	} else {
		total = len(peers)
		peerAddrs = make(chan interface{}, total)
		dht.peerAddrsLk.RLock()
		for _, p := range peers {
			peerAddrs <- peer.AddrInfo{
				ID:    p,
				Addrs: dht.peerAddrs[p],
			}
		}
		close(peerAddrs)
		dht.peerAddrsLk.RUnlock()
	}

	var success uint64

	workers(100, func(i interface{}) {
		a := i.(peer.AddrInfo)
		dialctx, dialcancel := context.WithTimeout(ctx, time.Second*3)
		if err := dht.h.Connect(dialctx, a); err == nil {
			atomic.AddUint64(&success, 1)
		}
		dialcancel()
	}, peerAddrs)
	return int(success), total
}

// workers 启动工作协程处理任务
// 参数:
//   - numWorkers: int 工作协程数量
//   - fn: func(interface{}) 处理函数
//   - inputs: <-chan interface{} 输入通道
func workers(numWorkers int, fn func(interface{}), inputs <-chan interface{}) {
	jobs := make(chan interface{})
	defer close(jobs)
	for i := 0; i < numWorkers; i++ {
		go func() {
			for j := range jobs {
				fn(j)
			}
		}()
	}
	for i := range inputs {
		jobs <- i
	}
}

// GetClosestPeers 获取与给定键最近的节点
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//
// 返回值:
//   - []peer.ID 最近节点的ID列表
//   - error 错误信息
func (dht *FullRT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	_, span := internal.StartSpan(ctx, "FullRT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	kbID := kb.ConvertKey(key)
	kadKey := kadkey.KbucketIDToKey(kbID)
	dht.rtLk.RLock()
	closestKeys := kademlia.ClosestN(kadKey, dht.rt, dht.bucketSize)
	dht.rtLk.RUnlock()

	peers := make([]peer.ID, 0, len(closestKeys))
	for _, k := range closestKeys {
		dht.kMapLk.RLock()
		p, ok := dht.keyToPeerMap[string(k)]
		if !ok {
			logger.Errorf("在映射中未找到键")
		}
		dht.kMapLk.RUnlock()
		dht.peerAddrsLk.RLock()
		peerAddrs := dht.peerAddrs[p]
		dht.peerAddrsLk.RUnlock()

		dht.h.Peerstore().AddAddrs(p, peerAddrs, peerstore.TempAddrTTL)
		peers = append(peers, p)
	}
	return peers, nil
}

// PutValue 添加与给定键对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - value: []byte 值
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) PutValue(ctx context.Context, key string, value []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(dhtName, ctx, key, value, opts...)
	defer func() { end(err) }()

	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	logger.Debugw("正在存储值", "key", internal.LoggableRecordKeyString(key))

	// 不允许本地用户存储无效值
	if err := dht.Validator.Validate(key, value); err != nil {
		return err
	}

	old, err := dht.getLocal(ctx, key)
	if err != nil {
		// 表示数据存储出现问题
		return err
	}

	// 检查是否有与新值不同的旧值
	if old != nil && !bytes.Equal(old.GetValue(), value) {
		// 检查新值是否更好
		i, err := dht.Validator.Select(key, [][]byte{value, old.GetValue()})
		if err != nil {
			return err
		}
		if i != 0 {
			return fmt.Errorf("无法用旧值替换新值")
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

	successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.Value,
			ID:   p,
		})
		err := dht.protoMessenger.PutValue(ctx, p, rec)
		return err
	}, peers, true)

	if successes == 0 {
		return fmt.Errorf("存储操作失败")
	}

	return nil
}

// RecvdVal 存储值及其来源节点
type RecvdVal struct {
	Val  []byte
	From peer.ID
}

// GetValue 搜索与给定键对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - []byte 找到的值
//   - error 错误信息
func (dht *FullRT) GetValue(ctx context.Context, key string, opts ...routing.Option) (result []byte, err error) {
	ctx, end := tracer.GetValue(dhtName, ctx, key, opts...)
	defer func() { end(result, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	// 应用默认仲裁数
	var cfg routing.Options
	if err := cfg.Apply(opts...); err != nil {
		return nil, err
	}
	opts = append(opts, kaddht.Quorum(internalConfig.GetQuorum(&cfg)))

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
		return nil, routing.ErrNotFound
	}
	logger.Debugf("GetValue %v %x", internal.LoggableRecordKeyString(key), best)
	return best, nil
}

// SearchValue 搜索与给定键对应的值并流式返回结果
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - <-chan []byte 值的通道
//   - error 错误信息
func (dht *FullRT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (ch <-chan []byte, err error) {
	ctx, end := tracer.SearchValue(dhtName, ctx, key, opts...)
	defer func() { ch, err = end(ch, err) }()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
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

		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		dht.updatePeerValues(ctx, key, best, updatePeers)
		cancel()
	}()

	return out, nil
}

// searchValueQuorum 搜索值直到达到所需的响应数量
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - valCh: <-chan RecvdVal 接收值的通道
//   - stopCh: chan struct{} 停止信号通道
//   - out: chan<- []byte 输出通道
//   - nvals: int 所需响应数量
//
// 返回值:
//   - []byte 最佳值
//   - map[peer.ID]struct{} 具有最佳值的节点集合
//   - bool 是否中止
func (dht *FullRT) searchValueQuorum(ctx context.Context, key string, valCh <-chan RecvdVal, stopCh chan struct{},
	out chan<- []byte, nvals int) ([]byte, map[peer.ID]struct{}, bool) {
	numResponses := 0
	return dht.processValues(ctx, key, valCh,
		func(ctx context.Context, v RecvdVal, better bool) bool {
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

// processValues 处理接收到的值并选择最佳值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - vals: <-chan RecvdVal 接收值的通道
//   - newVal: func 处理新值的函数
//
// 返回值:
//   - []byte 最佳值
//   - map[peer.ID]struct{} 具有最佳值的节点集合
//   - bool 是否中止
func (dht *FullRT) processValues(ctx context.Context, key string, vals <-chan RecvdVal,
	newVal func(ctx context.Context, v RecvdVal, better bool) bool) (best []byte, peersWithBest map[peer.ID]struct{}, aborted bool) {
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

// updatePeerValues 更新节点的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - peers: []peer.ID 节点列表
func (dht *FullRT) updatePeerValues(ctx context.Context, key string, val []byte, peers []peer.ID) {
	fixupRec := record.MakePutRecord(key, val)
	for _, p := range peers {
		go func(p peer.ID) {
			if p == dht.h.ID() {
				err := dht.putLocal(ctx, key, fixupRec)
				if err != nil {
					logger.Error("更正本地DHT条目时出错:", err)
				}
				return
			}
			ctx, cancel := context.WithTimeout(ctx, time.Second*5)
			defer cancel()
			err := dht.protoMessenger.PutValue(ctx, p, fixupRec)
			if err != nil {
				logger.Debug("更正DHT条目时出错: ", err)
			}
		}(p)
	}
}

// lookupWithFollowupResult 查找结果
type lookupWithFollowupResult struct {
	peers []peer.ID // 查询结束时最接近的K个可达节点
}

// getValues 获取值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - stopQuery: chan struct{} 停止查询信号通道
//
// 返回值:
//   - <-chan RecvdVal 接收值的通道
//   - <-chan *lookupWithFollowupResult 查找结果通道
func (dht *FullRT) getValues(ctx context.Context, key string, stopQuery chan struct{}) (<-chan RecvdVal, <-chan *lookupWithFollowupResult) {
	valCh := make(chan RecvdVal, 1)
	lookupResCh := make(chan *lookupWithFollowupResult, 1)

	logger.Debugw("查找值", "key", internal.LoggableRecordKeyString(key))

	if rec, err := dht.getLocal(ctx, key); rec != nil && err == nil {
		select {
		case valCh <- RecvdVal{
			Val:  rec.GetValue(),
			From: dht.h.ID(),
		}:
		case <-ctx.Done():
		}
	}
	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		lookupResCh <- &lookupWithFollowupResult{}
		close(valCh)
		close(lookupResCh)
		return valCh, lookupResCh
	}

	go func() {
		defer close(valCh)
		defer close(lookupResCh)
		queryFn := func(ctx context.Context, p peer.ID) error {
			// DHT查询命令
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type: routing.SendingQuery,
				ID:   p,
			})

			rec, peers, err := dht.protoMessenger.GetValue(ctx, p, key)
			if err != nil {
				return err
			}

			// DHT查询命令
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:      routing.PeerResponse,
				ID:        p,
				Responses: peers,
			})

			if rec == nil {
				return nil
			}

			val := rec.GetValue()
			if val == nil {
				logger.Debug("收到空记录值")
				return nil
			}
			if err := dht.Validator.Validate(key, val); err != nil {
				// 确保记录有效
				logger.Debugw("收到无效记录(已丢弃)", "error", err)
				return nil
			}

			// 记录有效，发送进行处理
			select {
			case valCh <- RecvdVal{
				Val:  val,
				From: p,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}

			return nil
		}

		dht.execOnMany(ctx, queryFn, peers, false)
		lookupResCh <- &lookupWithFollowupResult{peers: peers}
	}()
	return valCh, lookupResCh
}

// Provide 宣告此节点可以为给定的键提供值
// 参数:
//   - ctx: context.Context 上下文
//   - key: cid.Cid 内容标识符
//   - brdcst: bool 是否广播
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) Provide(ctx context.Context, key cid.Cid, brdcst bool) (err error) {
	ctx, end := tracer.Provide(dhtName, ctx, key, brdcst)
	defer func() { end(err) }()

	if !dht.enableProviders {
		return routing.ErrNotSupported
	} else if !key.Defined() {
		return fmt.Errorf("无效的CID: 未定义")
	}
	keyMH := key.Hash()
	logger.Debugw("正在提供", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))

	// 本地添加自己
	dht.ProviderManager.AddProvider(ctx, keyMH, peer.AddrInfo{ID: dht.h.ID()})
	if !brdcst {
		return nil
	}

	closerCtx := ctx
	if deadline, ok := ctx.Deadline(); ok {
		now := time.Now()
		timeout := deadline.Sub(now)

		if timeout < 0 {
			// 超时
			return context.DeadlineExceeded
		} else if timeout < 10*time.Second {
			// 为最后的放置保留10%
			deadline = deadline.Add(-timeout / 10)
		} else {
			// 否则，保留一秒(我们已经连接所以应该很快)
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
		// 如果内部截止时间已超过但外部上下文仍然正常，则向我们找到的最近的节点提供值，即使它们不是实际最近的节点
		if ctx.Err() != nil {
			return ctx.Err()
		}
		exceededDeadline = true
	case nil:
	default:
		return err
	}

	successes := dht.execOnMany(ctx, func(ctx context.Context, p peer.ID) error {
		err := dht.protoMessenger.PutProviderAddrs(ctx, p, keyMH, peer.AddrInfo{
			ID:    dht.self,
			Addrs: dht.h.Addrs(),
		})
		return err
	}, peers, true)

	if exceededDeadline {
		return context.DeadlineExceeded
	}

	if successes == 0 {
		return fmt.Errorf("提供失败")
	}

	return ctx.Err()
}

// execOnMany 在每个对等节点上执行给定的函数，但可能只等待一定数量的对等节点响应后就认为结果"足够好"并返回。
// 参数:
//   - ctx: context.Context 上下文
//   - fn: func(context.Context, peer.ID) error 要执行的函数
//   - peers: []peer.ID 要执行的对等节点列表
//   - sloppyExit: bool 是否允许提前退出
//
// 返回值:
//   - int 成功执行的次数
//
// 如果 sloppyExit 为 true，则此函数将在其所有内部 goroutine 关闭之前返回。
// 如果 sloppyExit 为 true，则传入的函数必须能够在 execOnMany 返回后的任意时间内安全完成
// (例如不要写入可能被关闭或设置为 nil 的资源，这会导致 panic 而不是仅仅返回错误)。
func (dht *FullRT) execOnMany(ctx context.Context, fn func(context.Context, peer.ID) error, peers []peer.ID, sloppyExit bool) int {
	if len(peers) == 0 {
		return 0
	}

	// 有一个可以容纳所有元素的缓冲区基本上是一个技巧，允许在函数完成后而不是之前进行松散退出来清理 goroutine
	errCh := make(chan error, len(peers))
	numSuccessfulToWaitFor := int(float64(len(peers)) * dht.waitFrac)

	putctx, cancel := context.WithTimeout(ctx, dht.timeoutPerOp)
	defer cancel()

	for _, p := range peers {
		go func(p peer.ID) {
			errCh <- fn(putctx, p)
		}(p)
	}

	var numDone, numSuccess, successSinceLastTick int
	var ticker *time.Ticker
	var tickChan <-chan time.Time

	for numDone < len(peers) {
		select {
		case err := <-errCh:
			numDone++
			if err == nil {
				numSuccess++
				if numSuccess >= numSuccessfulToWaitFor && ticker == nil {
					// 一旦有足够的成功，再多等一会儿
					ticker = time.NewTicker(time.Millisecond * 500)
					defer ticker.Stop()
					tickChan = ticker.C
					successSinceLastTick = numSuccess
				}
				// 这等同于 numSuccess * 2 + numFailures >= len(peers)，这是一个似乎表现合理的启发式方法。
				// TODO: 使这个指标更可配置
				// TODO: 在这个函数中有更好的启发式方法，无论是通过观察静态网络属性还是动态计算它们
				if numSuccess+numDone >= len(peers) {
					cancel()
					if sloppyExit {
						return numSuccess
					}
				}
			}
		case <-tickChan:
			if numSuccess > successSinceLastTick {
				// 如果有额外的成功，则再等一个 tick
				successSinceLastTick = numSuccess
			} else {
				cancel()
				if sloppyExit {
					return numSuccess
				}
			}
		}
	}
	return numSuccess
}

// ProvideMany 为多个键提供记录
// 参数:
//   - ctx: context.Context 上下文
//   - keys: []multihash.Multihash 要提供的键列表
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) ProvideMany(ctx context.Context, keys []multihash.Multihash) (err error) {
	ctx, end := tracer.ProvideMany(dhtName, ctx, keys)
	defer func() { end(err) }()

	if !dht.enableProviders {
		return routing.ErrNotSupported
	}

	// 一次性计算所有提供的地址
	pi := peer.AddrInfo{
		ID:    dht.h.ID(),
		Addrs: dht.h.Addrs(),
	}
	pbPeers := dht_pb.RawPeerInfosToPBPeers([]peer.AddrInfo{pi})

	// TODO: 我们可能想限制我们的提供者记录中的地址类型。例如，在仅 WAN 的 DHT 中禁止共享非 WAN 地址(如 192.168.0.100)
	if len(pi.Addrs) < 1 {
		return fmt.Errorf("没有已知的自身地址，无法添加提供者")
	}

	fn := func(ctx context.Context, p, k peer.ID) error {
		pmes := dht_pb.NewMessage(dht_pb.Message_ADD_PROVIDER, multihash.Multihash(k), 0)
		pmes.ProviderPeers = pbPeers

		return dht.messageSender.SendMessage(ctx, p, pmes)
	}

	keysAsPeerIDs := make([]peer.ID, 0, len(keys))
	for _, k := range keys {
		keysAsPeerIDs = append(keysAsPeerIDs, peer.ID(k))
	}

	return dht.bulkMessageSend(ctx, keysAsPeerIDs, fn, true)
}

// PutMany 存储多个键值对
// 参数:
//   - ctx: context.Context 上下文
//   - keys: []string 键列表
//   - values: [][]byte 值列表
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) PutMany(ctx context.Context, keys []string, values [][]byte) error {
	ctx, span := internal.StartSpan(ctx, "FullRT.PutMany", trace.WithAttributes(attribute.Int("NumKeys", len(keys))))
	defer span.End()

	if !dht.enableValues {
		return routing.ErrNotSupported
	}

	if len(keys) != len(values) {
		return fmt.Errorf("键的数量与值的数量不匹配")
	}

	keysAsPeerIDs := make([]peer.ID, 0, len(keys))
	keyRecMap := make(map[string][]byte)
	for i, k := range keys {
		keysAsPeerIDs = append(keysAsPeerIDs, peer.ID(k))
		keyRecMap[k] = values[i]
	}

	if len(keys) != len(keyRecMap) {
		return fmt.Errorf("不支持重复的键")
	}

	fn := func(ctx context.Context, p, k peer.ID) error {
		keyStr := string(k)
		return dht.protoMessenger.PutValue(ctx, p, record.MakePutRecord(keyStr, keyRecMap[keyStr]))
	}

	return dht.bulkMessageSend(ctx, keysAsPeerIDs, fn, false)
}

// bulkMessageSend 批量发送消息
// 参数:
//   - ctx: context.Context 上下文
//   - keys: []peer.ID 键列表
//   - fn: func(ctx context.Context, target, k peer.ID) error 要执行的函数
//   - isProvRec: bool 是否为提供者记录
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) bulkMessageSend(ctx context.Context, keys []peer.ID, fn func(ctx context.Context, target, k peer.ID) error, isProvRec bool) error {
	ctx, span := internal.StartSpan(ctx, "FullRT.BulkMessageSend")
	defer span.End()

	if len(keys) == 0 {
		return nil
	}

	type report struct {
		successes   int
		failures    int
		lastSuccess time.Time
		mx          sync.RWMutex
	}

	keySuccesses := make(map[peer.ID]*report, len(keys))
	var numSkipped int64

	for _, k := range keys {
		keySuccesses[k] = &report{}
	}

	logger.Infof("批量发送: 键的数量 %d, 唯一 %d", len(keys), len(keySuccesses))
	numSuccessfulToWaitFor := int(float64(dht.bucketSize) * dht.waitFrac * 1.2)

	sortedKeys := make([]peer.ID, 0, len(keySuccesses))
	for k := range keySuccesses {
		sortedKeys = append(sortedKeys, k)
	}

	sortedKeys = kb.SortClosestPeers(sortedKeys, kb.ID(make([]byte, 32)))

	dht.kMapLk.RLock()
	numPeers := len(dht.keyToPeerMap)
	dht.kMapLk.RUnlock()

	chunkSize := (len(sortedKeys) * dht.bucketSize * 2) / numPeers
	if chunkSize == 0 {
		chunkSize = 1
	}

	connmgrTag := fmt.Sprintf("dht-bulk-provide-tag-%d", rand.Int())

	type workMessage struct {
		p    peer.ID
		keys []peer.ID
	}

	workCh := make(chan workMessage, 1)
	wg := sync.WaitGroup{}
	wg.Add(dht.bulkSendParallelism)
	for i := 0; i < dht.bulkSendParallelism; i++ {
		go func() {
			defer wg.Done()
			defer logger.Debugf("批量发送 goroutine 完成")
			for wmsg := range workCh {
				p, workKeys := wmsg.p, wmsg.keys
				dht.peerAddrsLk.RLock()
				peerAddrs := dht.peerAddrs[p]
				dht.peerAddrsLk.RUnlock()
				dialCtx, dialCancel := context.WithTimeout(ctx, dht.timeoutPerOp)
				if err := dht.h.Connect(dialCtx, peer.AddrInfo{ID: p, Addrs: peerAddrs}); err != nil {
					dialCancel()
					atomic.AddInt64(&numSkipped, 1)
					continue
				}
				dialCancel()
				dht.h.ConnManager().Protect(p, connmgrTag)
				for _, k := range workKeys {
					keyReport := keySuccesses[k]

					queryTimeout := dht.timeoutPerOp
					keyReport.mx.RLock()
					if keyReport.successes >= numSuccessfulToWaitFor {
						if time.Since(keyReport.lastSuccess) > time.Millisecond*500 {
							keyReport.mx.RUnlock()
							continue
						}
						queryTimeout = time.Millisecond * 500
					}
					keyReport.mx.RUnlock()

					fnCtx, fnCancel := context.WithTimeout(ctx, queryTimeout)
					if err := fn(fnCtx, p, k); err == nil {
						keyReport.mx.Lock()
						keyReport.successes++
						if keyReport.successes >= numSuccessfulToWaitFor {
							keyReport.lastSuccess = time.Now()
						}
						keyReport.mx.Unlock()
					} else {
						keyReport.mx.Lock()
						keyReport.failures++
						keyReport.mx.Unlock()
						if ctx.Err() != nil {
							fnCancel()
							break
						}
					}
					fnCancel()
				}

				dht.h.ConnManager().Unprotect(p, connmgrTag)
			}
		}()
	}

	keyGroups := divideByChunkSize(sortedKeys, chunkSize)
	sendsSoFar := 0
	for _, g := range keyGroups {
		if ctx.Err() != nil {
			break
		}

		keysPerPeer := make(map[peer.ID][]peer.ID)
		for _, k := range g {
			peers, err := dht.GetClosestPeers(ctx, string(k))
			if err == nil {
				for _, p := range peers {
					keysPerPeer[p] = append(keysPerPeer[p], k)
				}
			}
		}

		logger.Debugf("批量发送: 组大小 %d 的对等节点数量 %d", len(keysPerPeer), len(g))

	keyloop:
		for p, workKeys := range keysPerPeer {
			select {
			case workCh <- workMessage{p: p, keys: workKeys}:
			case <-ctx.Done():
				break keyloop
			}
		}
		sendsSoFar += len(g)
		logger.Infof("批量发送: %.1f%% 完成 - %d/%d 完成", 100*float64(sendsSoFar)/float64(len(keySuccesses)), sendsSoFar, len(keySuccesses))
	}

	close(workCh)

	logger.Debugf("批量发送完成，等待 goroutine 关闭")

	wg.Wait()

	numSendsSuccessful := 0
	numFails := 0
	// 生成每个键成功发送次数的直方图
	successHist := make(map[int]int)
	// 生成每个键失败发送次数的直方图
	// 这不包括被跳过且根本没有向其发送任何消息的对等节点
	failHist := make(map[int]int)
	for _, v := range keySuccesses {
		if v.successes > 0 {
			numSendsSuccessful++
		}
		successHist[v.successes]++
		failHist[v.failures]++
		numFails += v.failures
	}

	if numSendsSuccessful == 0 {
		logger.Infof("批量发送失败")
		return fmt.Errorf("批量发送失败")
	}

	logger.Infof("批量发送完成: %d 个键, %d 个唯一, %d 个成功, %d 个跳过的对等节点, %d 个失败",
		len(keys), len(keySuccesses), numSendsSuccessful, numSkipped, numFails)

	logger.Infof("批量发送摘要: 成功直方图 %v, 失败直方图 %v", successHist, failHist)

	return nil
}

// divideByChunkSize 将键集合分成最大大小为 chunkSize 的组。块大小必须大于 0。
// 参数:
//   - keys: []peer.ID 要分组的键列表
//   - chunkSize: int 每组的大小
//
// 返回值:
//   - [][]peer.ID 分组后的键列表
func divideByChunkSize(keys []peer.ID, chunkSize int) [][]peer.ID {
	if len(keys) == 0 {
		return nil
	}

	if chunkSize < 1 {
		panic(fmt.Sprintf("fullrt: 分组: 无效的块大小 %d", chunkSize))
	}

	var keyChunks [][]peer.ID
	var nextChunk []peer.ID
	chunkProgress := 0
	for _, k := range keys {
		nextChunk = append(nextChunk, k)
		chunkProgress++
		if chunkProgress == chunkSize {
			keyChunks = append(keyChunks, nextChunk)
			chunkProgress = 0
			nextChunk = make([]peer.ID, 0, len(nextChunk))
		}
	}
	if chunkProgress != 0 {
		keyChunks = append(keyChunks, nextChunk)
	}
	return keyChunks
}

// FindProviders 搜索直到上下文过期。
// 参数:
//   - ctx: context.Context 上下文
//   - c: cid.Cid 要查找的内容标识符
//
// 返回值:
//   - []peer.AddrInfo 提供者地址信息列表
//   - error 错误信息
func (dht *FullRT) FindProviders(ctx context.Context, c cid.Cid) ([]peer.AddrInfo, error) {
	if !dht.enableProviders {
		return nil, routing.ErrNotSupported
	} else if !c.Defined() {
		return nil, fmt.Errorf("无效的 cid: 未定义")
	}

	var providers []peer.AddrInfo
	for p := range dht.FindProvidersAsync(ctx, c, dht.bucketSize) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync 与 FindProviders 相同，但返回一个通道。
// 一旦找到对等节点，即使在搜索查询完成之前，也会立即在通道上返回它们。
// 如果 count 为零，则查询将一直运行到完成。
// 注意：不从返回的通道读取可能会阻塞查询的进度。
// 参数:
//   - ctx: context.Context 上下文
//   - key: cid.Cid 要查找的内容标识符
//   - count: int 要查找的提供者数量
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者地址信息通道
func (dht *FullRT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) (ch <-chan peer.AddrInfo) {
	ctx, end := tracer.FindProvidersAsync(dhtName, ctx, key, count)
	defer func() { ch = end(ch, nil) }()

	if !dht.enableProviders || !key.Defined() {
		peerOut := make(chan peer.AddrInfo)
		close(peerOut)
		return peerOut
	}

	peerOut := make(chan peer.AddrInfo)

	keyMH := key.Hash()

	logger.Debugw("查找提供者", "cid", key, "mh", internal.LoggableProviderRecordBytes(keyMH))
	go dht.findProvidersAsyncRoutine(ctx, keyMH, count, peerOut)
	return peerOut
}

// findProvidersAsyncRoutine 异步查找提供者的例程
// 参数:
//   - ctx: context.Context 上下文
//   - key: multihash.Multihash 要查找的内容哈希
//   - count: int 要查找的提供者数量
//   - peerOut: chan peer.AddrInfo 提供者地址信息输出通道
func (dht *FullRT) findProvidersAsyncRoutine(ctx context.Context, key multihash.Multihash, count int, peerOut chan peer.AddrInfo) {
	// 使用span因为与tracer.FindProvidersAsync不同,我们知道谁告诉我们这个信息,这很有趣可以记录
	ctx, span := internal.StartSpan(ctx, "FullRT.FindProvidersAsyncRoutine")
	defer span.End()

	defer close(peerOut)

	findAll := count == 0
	ps := make(map[peer.ID]struct{})
	psLock := &sync.Mutex{}
	psTryAdd := func(p peer.ID) bool {
		psLock.Lock()
		defer psLock.Unlock()
		_, ok := ps[p]
		if !ok && (len(ps) < count || findAll) {
			ps[p] = struct{}{}
			return true
		}
		return false
	}
	psSize := func() int {
		psLock.Lock()
		defer psLock.Unlock()
		return len(ps)
	}

	provs, err := dht.ProviderManager.GetProviders(ctx, key)
	if err != nil {
		return
	}
	for _, p := range provs {
		// 注意:假设这个对等节点列表是唯一的
		if psTryAdd(p.ID) {
			select {
			case peerOut <- p:
				span.AddEvent("找到提供者", trace.WithAttributes(
					attribute.Stringer("peer", p.ID),
					attribute.Stringer("from", dht.self),
				))
			case <-ctx.Done():
				return
			}
		}

		// 如果我们在本地有足够的对等节点,就不用进行远程RPC了
		// TODO: 这是一个DOS攻击向量吗?
		if !findAll && psSize() >= count {
			return
		}
	}

	peers, err := dht.GetClosestPeers(ctx, string(key))
	if err != nil {
		return
	}

	queryctx, cancelquery := context.WithCancel(ctx)
	defer cancelquery()

	fn := func(ctx context.Context, p peer.ID) error {
		// 用于DHT查询命令
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		provs, closest, err := dht.protoMessenger.GetProviders(ctx, p, key)
		if err != nil {
			return err
		}

		logger.Debugf("%d 个提供者条目", len(provs))

		// 从请求中添加唯一的提供者,直到'count'个
		for _, prov := range provs {
			dht.maybeAddAddrs(prov.ID, prov.Addrs, peerstore.TempAddrTTL)
			logger.Debugf("获得提供者: %s", prov)
			if psTryAdd(prov.ID) {
				logger.Debugf("使用提供者: %s", prov)
				select {
				case peerOut <- *prov:
					span.AddEvent("找到提供者", trace.WithAttributes(
						attribute.Stringer("peer", prov.ID),
						attribute.Stringer("from", p),
					))
				case <-ctx.Done():
					logger.Debug("发送更多提供者时上下文超时")
					return ctx.Err()
				}
			}
			if !findAll && psSize() >= count {
				logger.Debugf("获得足够的提供者 (%d/%d)", psSize(), count)
				cancelquery()
				return nil
			}
		}

		// 将更近的对等节点返回给查询以进行查询
		logger.Debugf("获得更近的对等节点: %d %s", len(closest), closest)

		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: closest,
		})
		return nil
	}

	dht.execOnMany(queryctx, fn, peers, false)
}

// FindPeer 搜索具有给定ID的对等节点
// 参数:
//   - ctx: context.Context 上下文
//   - id: peer.ID 要查找的对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点地址信息
//   - error 错误信息
func (dht *FullRT) FindPeer(ctx context.Context, id peer.ID) (pi peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(dhtName, ctx, id)
	defer func() { end(pi, err) }()

	if err := id.Validate(); err != nil {
		return peer.AddrInfo{}, err
	}

	logger.Debugw("查找对等节点", "peer", id)

	// 检查我们是否已经连接到他们
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	peers, err := dht.GetClosestPeers(ctx, string(id))
	if err != nil {
		return peer.AddrInfo{}, err
	}

	queryctx, cancelquery := context.WithCancel(ctx)
	defer cancelquery()

	addrsCh := make(chan *peer.AddrInfo, 1)
	newAddrs := make([]multiaddr.Multiaddr, 0)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		addrsSoFar := make(map[multiaddr.Multiaddr]struct{})
		for {
			select {
			case ai, ok := <-addrsCh:
				if !ok {
					return
				}

				for _, a := range ai.Addrs {
					_, found := addrsSoFar[a]
					if !found {
						newAddrs = append(newAddrs, a)
						addrsSoFar[a] = struct{}{}
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	fn := func(ctx context.Context, p peer.ID) error {
		// 用于DHT查询命令
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, id)
		if err != nil {
			logger.Debugf("获取更近的对等节点时出错: %s", err)
			return err
		}

		// 用于DHT查询命令
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		for _, a := range peers {
			if a.ID == id {
				select {
				case addrsCh <- a:
				case <-ctx.Done():
					return ctx.Err()
				}
				return nil
			}
		}
		return nil
	}

	dht.execOnMany(queryctx, fn, peers, false)

	close(addrsCh)
	wg.Wait()

	if len(newAddrs) > 0 {
		connctx, cancelconn := context.WithTimeout(ctx, time.Second*5)
		defer cancelconn()
		_ = dht.h.Connect(connctx, peer.AddrInfo{
			ID:    id,
			Addrs: newAddrs,
		})
	}

	// 如果我们在查询期间尝试拨号对等节点,或者我们(最近)连接到该对等节点,则返回对等节点信息
	if hasValidConnectedness(dht.h, id) {
		return dht.h.Peerstore().PeerInfo(id), nil
	}

	return peer.AddrInfo{}, routing.ErrNotFound
}

var _ routing.Routing = (*FullRT)(nil)

// getLocal 尝试从数据存储中检索值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//
// 返回值:
//   - *recpb.Record 记录
//   - error 错误信息
//
// 当没有找到任何内容或找到的值无法正确验证时,返回 nil, nil
// 当出现*数据存储*错误时(即出现严重错误),返回 nil, some_error
func (dht *FullRT) getLocal(ctx context.Context, key string) (*recpb.Record, error) {
	logger.Debugw("在数据存储中查找值", "key", internal.LoggableRecordKeyString(key))

	rec, err := dht.getRecordFromDatastore(ctx, mkDsKey(key))
	if err != nil {
		logger.Warnw("获取本地失败", "key", internal.LoggableRecordKeyString(key), "error", err)
		return nil, err
	}

	// 再次检查键。不会有坏处。
	if rec != nil && string(rec.GetKey()) != key {
		logger.Errorw("BUG: 找到一个与其键不匹配的DHT记录", "expected", internal.LoggableRecordKeyString(key), "got", rec.GetKey())
		return nil, nil

	}
	return rec, nil
}

// putLocal 将键值对存储在数据存储中
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - rec: *recpb.Record 记录
//
// 返回值:
//   - error 错误信息
func (dht *FullRT) putLocal(ctx context.Context, key string, rec *recpb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		logger.Warnw("为本地存储序列化记录失败", "error", err, "key", internal.LoggableRecordKeyString(key))
		return err
	}

	return dht.datastore.Put(ctx, mkDsKey(key), data)
}

// mkDsKey 创建数据存储键
// 参数:
//   - s: string 原始键
//
// 返回值:
//   - ds.Key 数据存储键
func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

// getRecordFromDatastore 从数据存储中获取记录
// 参数:
//   - ctx: context.Context 上下文
//   - dskey: ds.Key 数据存储键
//
// 返回值:
//   - *recpb.Record 记录
//   - error 错误信息
//
// 当没有找到任何内容或找到的值无法正确验证时,返回 nil, nil
// 当出现*数据存储*错误时(即出现严重错误),返回 nil, some_error
func (dht *FullRT) getRecordFromDatastore(ctx context.Context, dskey ds.Key) (*recpb.Record, error) {
	buf, err := dht.datastore.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		logger.Errorw("从数据存储检索记录时出错", "key", dskey, "error", err)
		return nil, err
	}
	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		// 数据存储中的数据错误,记录它但不返回错误,我们将覆盖它
		logger.Errorw("从数据存储反序列化记录失败", "key", dskey, "error", err)
		return nil, nil
	}

	err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// 数据存储中的记录无效,可能已过期但不返回错误,
		// 我们将覆盖它
		logger.Debugw("本地记录验证失败", "key", rec.GetKey(), "error", err)
		return nil, nil
	}

	return rec, nil
}

// FindLocal 查找连接到此DHT的具有给定ID的对等节点,并返回对等节点和找到它的表
// 参数:
//   - id: peer.ID 要查找的对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点地址信息
func (dht *FullRT) FindLocal(id peer.ID) peer.AddrInfo {
	if hasValidConnectedness(dht.h, id) {
		return dht.h.Peerstore().PeerInfo(id)
	}
	return peer.AddrInfo{}
}

// maybeAddAddrs 可能添加对等节点地址
// 参数:
//   - p: peer.ID 对等节点ID
//   - addrs: []multiaddr.Multiaddr 地址列表
//   - ttl: time.Duration 生存时间
func (dht *FullRT) maybeAddAddrs(p peer.ID, addrs []multiaddr.Multiaddr, ttl time.Duration) {
	// 不要为自己或已连接的对等节点添加地址。我们有更好的地址。
	if p == dht.h.ID() || hasValidConnectedness(dht.h, p) {
		return
	}
	dht.h.Peerstore().AddAddrs(p, addrs, ttl)
}

// hasValidConnectedness 检查对等节点是否有有效的连接状态
// 参数:
//   - host: host.Host 主机
//   - id: peer.ID 对等节点ID
//
// 返回值:
//   - bool 是否有有效的连接状态
func hasValidConnectedness(host host.Host, id peer.ID) bool {
	connectedness := host.Network().Connectedness(id)
	return connectedness == network.Connected || connectedness == network.Limited
}
