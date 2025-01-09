package dht

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/dep2p/kaddht/tracing"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/peerstore"
	"github.com/dep2p/libp2p/core/protocol"
	"github.com/dep2p/libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/dep2p/kaddht/internal"
	dhtcfg "github.com/dep2p/kaddht/internal/config"
	kb "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/kaddht/kbucket/peerdiversity"
	"github.com/dep2p/kaddht/metrics"
	"github.com/dep2p/kaddht/netsize"
	pb "github.com/dep2p/kaddht/pb"
	"github.com/dep2p/kaddht/providers"
	record "github.com/dep2p/kaddht/record"
	recpb "github.com/dep2p/kaddht/record/pb"
	"github.com/dep2p/kaddht/rtrefresh"

	"github.com/gogo/protobuf/proto"
	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-base32"
	ma "github.com/multiformats/go-multiaddr"
	"go.opencensus.io/tag"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

// tracer 跟踪器
const tracer = tracing.Tracer("dep2p-kad-dht")

// dhtName DHT名称
const dhtName = "IpfsDHT"

var (
	// logger 日志记录器
	logger = logging.Logger("dht")
	// baseLogger 基础日志记录器
	baseLogger = logger.Desugar()

	// rtFreezeTimeout 路由表冻结超时时间
	rtFreezeTimeout = 1 * time.Minute
)

const (
	// BaseConnMgrScore 是连接管理器"kbucket"标签设置的基础分数。
	// 它与两个对等ID之间的公共前缀长度相加。
	baseConnMgrScore = 5
)

// mode DHT运行模式
type mode int

const (
	// modeServer 服务器模式
	modeServer mode = iota + 1
	// modeClient 客户端模式
	modeClient
)

const (
	// kad1 Kademlia协议ID
	kad1 protocol.ID = "/kad/1.0.0"
)

const (
	// kbucketTag k桶标签
	kbucketTag = "kbucket"
	// protectedBuckets 受保护的桶数量
	protectedBuckets = 2
)

// IpfsDHT 是一个带有S/Kademlia修改的Kademlia实现。
// 它用于实现基本的路由模块。
type IpfsDHT struct {
	host      host.Host           // 网络服务
	self      peer.ID             // 本地节点ID
	selfKey   kb.ID               // 本地节点密钥
	peerstore peerstore.Peerstore // 对等节点注册表

	datastore ds.Datastore // 本地数据存储

	routingTable  *kb.RoutingTable        // 不同距离节点的路由表数组
	providerStore providers.ProviderStore // 提供者存储,用于存储和管理此DHT对等节点的提供者记录

	// rtRefreshManager 管理路由表刷新
	rtRefreshManager *rtrefresh.RtRefreshManager

	birth time.Time // 节点启动时间

	Validator record.Validator // 验证器

	ctx    context.Context    // 上下文
	cancel context.CancelFunc // 取消函数
	wg     sync.WaitGroup     // 等待组

	protoMessenger *pb.ProtocolMessenger          // 协议消息发送器
	msgSender      pb.MessageSenderWithDisconnect // 消息发送器

	stripedPutLocks [256]sync.Mutex // 分片锁

	// protocols 我们查询的DHT协议。只有支持这些协议的对等节点才会被添加到路由表中。
	protocols []protocol.ID

	// serverProtocols 我们可以响应的DHT协议
	serverProtocols []protocol.ID

	auto   ModeOpt    // 自动模式选项
	mode   mode       // 运行模式
	modeLk sync.Mutex // 模式锁

	bucketSize int // 桶大小
	alpha      int // 每个路径的并发参数
	beta       int // 查询路径终止所需的最近对等节点响应数

	queryPeerFilter        QueryFilterFunc                 // 查询对等节点过滤器
	routingTablePeerFilter RouteTableFilterFunc            // 路由表对等节点过滤器
	rtPeerDiversityFilter  peerdiversity.PeerIPGroupFilter // 路由表对等节点多样性过滤器

	autoRefresh bool // 是否自动刷新

	// lookupCheckTimeout 查找检查操作的超时时间
	lookupCheckTimeout time.Duration
	// lookupCheckCapacity 并发查找检查操作的数量
	lookupCheckCapacity int
	lookupChecksLk      sync.Mutex // 查找检查锁

	// bootstrapPeers 返回一组引导对等节点的函数,当所有其他修复路由表的尝试失败时(或者,例如,这是该节点第一次连接到网络)时使用。
	bootstrapPeers func() []peer.AddrInfo

	maxRecordAge time.Duration // 记录最大存活时间

	// enableProviders,enableValues 允许禁用DHT子系统。这些应该只在"分叉"的DHT上设置(例如,具有自定义协议和/或私有网络的DHT)。
	enableProviders, enableValues bool

	disableFixLowPeers bool          // 是否禁用修复低对等节点数
	fixLowPeersChan    chan struct{} // 修复低对等节点数通道

	addPeerToRTChan   chan peer.ID  // 添加对等节点到路由表通道
	refreshFinishedCh chan struct{} // 刷新完成通道

	rtFreezeTimeout time.Duration // 路由表冻结超时时间

	// nsEstimator 网络规模估算器
	nsEstimator   *netsize.Estimator
	enableOptProv bool // 是否启用优化提供

	// optProvJobsPool 用于限制正在进行的ADD_PROVIDER RPC的异步性的有界通道
	optProvJobsPool chan struct{}

	// testAddressUpdateProcessing 测试地址更新处理的配置变量
	testAddressUpdateProcessing bool

	// addrFilter 用于过滤我们放入对等存储的地址。
	// 主要用于过滤掉localhost和本地地址。
	addrFilter func([]ma.Multiaddr) []ma.Multiaddr
}

// 断言IPFS关于接口的假设没有被破坏。
// 这些不是保证,但我们可以用它们来帮助重构。
var (
	_ routing.ContentRouting = (*IpfsDHT)(nil)
	_ routing.Routing        = (*IpfsDHT)(nil)
	_ routing.PeerRouting    = (*IpfsDHT)(nil)
	_ routing.PubKeyFetcher  = (*IpfsDHT)(nil)
	_ routing.ValueStore     = (*IpfsDHT)(nil)
)

// New 使用指定的主机和选项创建一个新的DHT。
// 请注意,连接到DHT对等节点并不一定意味着它也在DHT路由表中。
// 如果路由表有超过"minRTRefreshThreshold"个对等节点,我们只在成功从对等节点获得查询响应或它向我们发送查询时才将其视为路由表候选者。
//
// 参数:
//   - ctx: context.Context 上下文
//   - h: host.Host 主机
//   - options: ...Option 选项
//
// 返回值:
//   - *IpfsDHT DHT实例
//   - error 错误信息
func New(ctx context.Context, h host.Host, options ...Option) (*IpfsDHT, error) {
	var cfg dhtcfg.Config
	if err := cfg.Apply(append([]Option{dhtcfg.Defaults}, options...)...); err != nil {
		return nil, err
	}
	if err := cfg.ApplyFallbacks(h); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	dht, err := makeDHT(h, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create DHT, err=%s", err)
	}

	dht.autoRefresh = cfg.RoutingTable.AutoRefresh

	dht.maxRecordAge = cfg.MaxRecordAge
	dht.enableProviders = cfg.EnableProviders
	dht.enableValues = cfg.EnableValues
	dht.disableFixLowPeers = cfg.DisableFixLowPeers

	dht.Validator = cfg.Validator
	dht.msgSender = cfg.MsgSenderBuilder(h, dht.protocols)
	dht.protoMessenger, err = pb.NewProtocolMessenger(dht.msgSender)
	if err != nil {
		return nil, err
	}

	dht.testAddressUpdateProcessing = cfg.TestAddressUpdateProcessing

	dht.auto = cfg.Mode
	switch cfg.Mode {
	case ModeAuto, ModeClient:
		dht.mode = modeClient
	case ModeAutoServer, ModeServer:
		dht.mode = modeServer
	default:
		return nil, fmt.Errorf("invalid dht mode %d", cfg.Mode)
	}

	if dht.mode == modeServer {
		if err := dht.moveToServerMode(); err != nil {
			return nil, err
		}
	}

	// 注册事件总线和网络通知
	if err := dht.startNetworkSubscriber(); err != nil {
		return nil, err
	}

	// go-routine确保我们始终在peerstore中有RT对等地址,因为RT成员资格与连接性解耦
	go dht.persistRTPeersInPeerStore()

	dht.rtPeerLoop()

	// 用当前连接的DHT服务器填充路由表
	for _, p := range dht.host.Network().Peers() {
		dht.peerFound(p)
	}

	dht.rtRefreshManager.Start()

	// 监听修复低对等节点通道并尝试修复路由表
	if !dht.disableFixLowPeers {
		dht.runFixLowPeersLoop()
	}

	return dht, nil
}

// NewDHT 使用给定的对等节点作为"本地"主机创建一个新的DHT对象。
// 使用此函数初始化的IpfsDHT将响应DHT请求,而使用NewDHTClient初始化的IpfsDHT则不会。
//
// 参数:
//   - ctx: context.Context 上下文
//   - h: host.Host 主机
//   - dstore: ds.Batching 数据存储
//
// 返回值:
//   - *IpfsDHT DHT实例
func NewDHT(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
	dht, err := New(ctx, h, Datastore(dstore))
	if err != nil {
		panic(err)
	}
	return dht
}

// NewDHTClient 使用给定的对等节点作为"本地"主机创建一个新的DHT对象。
// 使用此函数初始化的IpfsDHT客户端将不会响应DHT请求。如果需要对等节点响应DHT请求,请使用NewDHT。
//
// 参数:
//   - ctx: context.Context 上下文
//   - h: host.Host 主机
//   - dstore: ds.Batching 数据存储
//
// 返回值:
//   - *IpfsDHT DHT实例
func NewDHTClient(ctx context.Context, h host.Host, dstore ds.Batching) *IpfsDHT {
	dht, err := New(ctx, h, Datastore(dstore), Mode(ModeClient))
	if err != nil {
		panic(err)
	}
	return dht
}

// makeDHT 创建一个新的DHT实例
//
// 参数:
//   - h: host.Host 主机
//   - cfg: dhtcfg.Config 配置
//
// 返回值:
//   - *IpfsDHT DHT实例
//   - error 错误信息
func makeDHT(h host.Host, cfg dhtcfg.Config) (*IpfsDHT, error) {
	var protocols, serverProtocols []protocol.ID

	v1proto := cfg.ProtocolPrefix + kad1

	if cfg.V1ProtocolOverride != "" {
		v1proto = cfg.V1ProtocolOverride
	}

	protocols = []protocol.ID{v1proto}
	serverProtocols = []protocol.ID{v1proto}

	dht := &IpfsDHT{
		datastore:              cfg.Datastore,
		self:                   h.ID(),
		selfKey:                kb.ConvertPeerID(h.ID()),
		peerstore:              h.Peerstore(),
		host:                   h,
		birth:                  time.Now(),
		protocols:              protocols,
		serverProtocols:        serverProtocols,
		bucketSize:             cfg.BucketSize,
		alpha:                  cfg.Concurrency,
		beta:                   cfg.Resiliency,
		lookupCheckCapacity:    cfg.LookupCheckConcurrency,
		queryPeerFilter:        cfg.QueryPeerFilter,
		routingTablePeerFilter: cfg.RoutingTable.PeerFilter,
		rtPeerDiversityFilter:  cfg.RoutingTable.DiversityFilter,
		addrFilter:             cfg.AddressFilter,

		fixLowPeersChan: make(chan struct{}, 1),

		addPeerToRTChan:   make(chan peer.ID),
		refreshFinishedCh: make(chan struct{}),

		enableOptProv:   cfg.EnableOptimisticProvide,
		optProvJobsPool: nil,
	}

	var maxLastSuccessfulOutboundThreshold time.Duration

	// 阈值是根据在刷新周期中查询对等节点之前应该经过的预期时间计算的。
	// 要理解产生这些精确方程的数学魔法,请耐心等待,因为解释它的文档很快就会发布。
	if cfg.Concurrency < cfg.BucketSize { // (alpha < K)
		l1 := math.Log(float64(1) / float64(cfg.BucketSize))                              // (Log(1/K))
		l2 := math.Log(float64(1) - (float64(cfg.Concurrency) / float64(cfg.BucketSize))) // Log(1 - (alpha / K))
		maxLastSuccessfulOutboundThreshold = time.Duration(l1 / l2 * float64(cfg.RoutingTable.RefreshInterval))
	} else {
		maxLastSuccessfulOutboundThreshold = cfg.RoutingTable.RefreshInterval
	}

	// 构造路由表
	// 使用理论有用性阈值的两倍来保持旧对等节点更长时间
	rt, err := makeRoutingTable(dht, cfg, 2*maxLastSuccessfulOutboundThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to construct routing table,err=%s", err)
	}
	dht.routingTable = rt
	dht.bootstrapPeers = cfg.BootstrapPeers

	dht.lookupCheckTimeout = cfg.RoutingTable.RefreshQueryTimeout

	// 初始化网络规模估算器
	dht.nsEstimator = netsize.NewEstimator(h.ID(), rt, cfg.BucketSize)

	if dht.enableOptProv {
		dht.optProvJobsPool = make(chan struct{}, cfg.OptimisticProvideJobsPoolSize)
	}

	// rt刷新管理器
	dht.rtRefreshManager, err = makeRtRefreshManager(dht, cfg, maxLastSuccessfulOutboundThreshold)
	if err != nil {
		return nil, fmt.Errorf("failed to construct RT Refresh Manager,err=%s", err)
	}

	// 从原始上下文创建带标签的上下文
	// DHT上下文应在进程关闭时完成
	dht.ctx, dht.cancel = context.WithCancel(dht.newContextWithLocalTags(context.Background()))

	if cfg.ProviderStore != nil {
		dht.providerStore = cfg.ProviderStore
	} else {
		dht.providerStore, err = providers.NewProviderManager(h.ID(), dht.peerstore, cfg.Datastore)
		if err != nil {
			return nil, fmt.Errorf("initializing default provider manager (%v)", err)
		}
	}

	dht.rtFreezeTimeout = rtFreezeTimeout

	return dht, nil
}

// lookupCheck 对远程对等节点执行查找请求,验证它是否能够正确回答
//
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) lookupCheck(ctx context.Context, p peer.ID) error {
	// 向p发送查找请求,请求其自己的peer.ID
	peerids, err := dht.protoMessenger.GetClosestPeers(ctx, p, p)
	// p预期至少返回1个peer id,除非我们的路由表中的对等节点少于bucketSize,在这种情况下,我们不会对添加到路由表的对象太挑剔。
	if err == nil && len(peerids) == 0 && dht.routingTable.Size() >= dht.bucketSize {
		return fmt.Errorf("peer %s failed to return its closest peers, got %d", p, len(peerids))
	}
	return err
}

// makeRtRefreshManager 创建路由表刷新管理器
//
// 参数:
//   - dht: *IpfsDHT DHT实例
//   - cfg: dhtcfg.Config 配置
//   - maxLastSuccessfulOutboundThreshold: time.Duration 最大最后成功出站阈值
//
// 返回值:
//   - *rtrefresh.RtRefreshManager 路由表刷新管理器
//   - error 错误信息
func makeRtRefreshManager(dht *IpfsDHT, cfg dhtcfg.Config, maxLastSuccessfulOutboundThreshold time.Duration) (*rtrefresh.RtRefreshManager, error) {
	keyGenFnc := func(cpl uint) (string, error) {
		p, err := dht.routingTable.GenRandPeerID(cpl)
		return string(p), err
	}

	queryFnc := func(ctx context.Context, key string) error {
		_, err := dht.GetClosestPeers(ctx, key)
		return err
	}

	r, err := rtrefresh.NewRtRefreshManager(
		dht.host, dht.routingTable, cfg.RoutingTable.AutoRefresh,
		keyGenFnc,
		queryFnc,
		dht.lookupCheck,
		cfg.RoutingTable.RefreshQueryTimeout,
		cfg.RoutingTable.RefreshInterval,
		maxLastSuccessfulOutboundThreshold,
		dht.refreshFinishedCh)

	return r, err
}

// makeRoutingTable 创建路由表
//
// 参数:
//   - dht: *IpfsDHT DHT实例
//   - cfg: dhtcfg.Config 配置
//   - maxLastSuccessfulOutboundThreshold: time.Duration 最大最后成功出站阈值
//
// 返回值:
//   - *kb.RoutingTable 路由表
//   - error 错误信息
func makeRoutingTable(dht *IpfsDHT, cfg dhtcfg.Config, maxLastSuccessfulOutboundThreshold time.Duration) (*kb.RoutingTable, error) {
	// 创建路由表多样性过滤器
	var filter *peerdiversity.Filter
	if dht.rtPeerDiversityFilter != nil {
		df, err := peerdiversity.NewFilter(dht.rtPeerDiversityFilter, "rt/diversity", func(p peer.ID) int {
			return kb.CommonPrefixLen(dht.selfKey, kb.ConvertPeerID(p))
		})

		if err != nil {
			return nil, fmt.Errorf("failed to construct peer diversity filter: %w", err)
		}

		filter = df
	}

	rt, err := kb.NewRoutingTable(cfg.BucketSize, dht.selfKey, time.Minute, dht.host.Peerstore(), maxLastSuccessfulOutboundThreshold, filter)
	if err != nil {
		return nil, err
	}

	cmgr := dht.host.ConnManager()

	rt.PeerAdded = func(p peer.ID) {
		commonPrefixLen := kb.CommonPrefixLen(dht.selfKey, kb.ConvertPeerID(p))
		if commonPrefixLen < protectedBuckets {
			cmgr.Protect(p, kbucketTag)
		} else {
			cmgr.TagPeer(p, kbucketTag, baseConnMgrScore)
		}
	}
	rt.PeerRemoved = func(p peer.ID) {
		cmgr.Unprotect(p, kbucketTag)
		cmgr.UntagPeer(p, kbucketTag)

		// 尝试修复RT
		dht.fixRTIfNeeded()
	}

	return rt, err
}

// ProviderStore 返回用于存储和检索提供者记录的提供者存储对象。
//
// 返回值:
//   - providers.ProviderStore 提供者存储
func (dht *IpfsDHT) ProviderStore() providers.ProviderStore {
	return dht.providerStore
}

// GetRoutingTableDiversityStats 返回路由表的多样性统计信息。
//
// 返回值:
//   - []peerdiversity.CplDiversityStats 多样性统计信息
func (dht *IpfsDHT) GetRoutingTableDiversityStats() []peerdiversity.CplDiversityStats {
	return dht.routingTable.GetDiversityStats()
}

// Mode 允许检查DHT的操作模式
//
// 返回值:
//   - ModeOpt 模式选项
func (dht *IpfsDHT) Mode() ModeOpt {
	return dht.auto
}

// runFixLowPeersLoop 管理同时对fixLowPeers的请求
func (dht *IpfsDHT) runFixLowPeersLoop() {
	dht.wg.Add(1)
	go func() {
		defer dht.wg.Done()

		dht.fixLowPeers()

		ticker := time.NewTicker(periodicBootstrapInterval)
		defer ticker.Stop()

		for {
			select {
			case <-dht.fixLowPeersChan:
			case <-ticker.C:
			case <-dht.ctx.Done():
				return
			}

			dht.fixLowPeers()
		}
	}()
}

// fixLowPeers 如果我们低于阈值,尝试获取更多对等节点到路由表中
func (dht *IpfsDHT) fixLowPeers() {
	if dht.routingTable.Size() > minRTRefreshThreshold {
		return
	}

	// 我们尝试将所有已连接的对等节点添加到路由表中,以防它们还不在那里。
	for _, p := range dht.host.Network().Peers() {
		dht.peerFound(p)
	}

	// TODO 主动引导
	// 在连接到引导节点之前,我们应该首先使用来自路由表先前快照的非引导对等节点。
	// 参见 https://github.com/dep2p/kaddht/issues/387。
	if dht.routingTable.Size() == 0 && dht.bootstrapPeers != nil {
		bootstrapPeers := dht.bootstrapPeers()
		if len(bootstrapPeers) == 0 {
			// 没有对等节点,继续下去没有意义!
			return
		}

		found := 0
		for _, i := range rand.Perm(len(bootstrapPeers)) {
			ai := bootstrapPeers[i]
			err := dht.Host().Connect(dht.ctx, ai)
			if err == nil {
				found++
			} else {
				logger.Warnw("failed to bootstrap", "peer", ai.ID, "error", err)
			}

			// 等待两个引导对等节点,或尝试所有对等节点。
			//
			// 为什么是两个?理论上,一个应该足够了。但是,如果网络重新启动并且每个人都连接到一个引导节点,我们最终会得到一个大部分分区的网络。
			//
			// 所以我们总是用两个随机对等节点引导。
			if found == maxNBoostrappers {
				break
			}
		}
	}

	// 如果我们的路由表中仍然没有对等节点(可能是因为Identify尚未完成),触发刷新是没有意义的。
	if dht.routingTable.Size() == 0 {
		return
	}

	if dht.autoRefresh {
		dht.rtRefreshManager.RefreshNoWait()
	}
}

// TODO 这是一个黑客,可怕,程序员需要被叫做仓鼠。
// 一旦 https://github.com/dep2p/libp2pissues/800 进入就应该删除。
func (dht *IpfsDHT) persistRTPeersInPeerStore() {
	tickr := time.NewTicker(peerstore.RecentlyConnectedAddrTTL / 3)
	defer tickr.Stop()

	for {
		select {
		case <-tickr.C:
			ps := dht.routingTable.ListPeers()
			for _, p := range ps {
				dht.peerstore.UpdateAddrs(p, peerstore.RecentlyConnectedAddrTTL, peerstore.RecentlyConnectedAddrTTL)
			}
		case <-dht.ctx.Done():
			return
		}
	}
}

// getLocal 尝试从数据存储中检索值。
//
// 当没有找到任何内容或找到的值无法正确验证时,返回nil, nil。
// 当出现*数据存储*错误时(即,出现严重错误)返回nil, some_error。
//
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//
// 返回值:
//   - *recpb.Record 记录
//   - error 错误信息
func (dht *IpfsDHT) getLocal(ctx context.Context, key string) (*recpb.Record, error) {
	logger.Debugw("finding value in datastore", "key", internal.LoggableRecordKeyString(key))

	rec, err := dht.getRecordFromDatastore(ctx, mkDsKey(key))
	if err != nil {
		logger.Warnw("get local failed", "key", internal.LoggableRecordKeyString(key), "error", err)
		return nil, err
	}

	// 再次检查键。不会有坏处。
	if rec != nil && string(rec.GetKey()) != key {
		logger.Errorw("BUG: found a DHT record that didn't match it's key", "expected", internal.LoggableRecordKeyString(key), "got", rec.GetKey())
		return nil, nil

	}
	return rec, nil
}

// putLocal 将键值对存储在数据存储中
//
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - rec: *recpb.Record 记录
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) putLocal(ctx context.Context, key string, rec *recpb.Record) error {
	data, err := proto.Marshal(rec)
	if err != nil {
		logger.Warnw("failed to put marshal record for local put", "error", err, "key", internal.LoggableRecordKeyString(key))
		return err
	}

	return dht.datastore.Put(ctx, mkDsKey(key), data)
}

func (dht *IpfsDHT) rtPeerLoop() {
	dht.wg.Add(1)
	go func() {
		defer dht.wg.Done()

		var bootstrapCount uint
		var isBootsrapping bool
		var timerCh <-chan time.Time

		for {
			select {
			case <-timerCh:
				dht.routingTable.MarkAllPeersIrreplaceable()
			case p := <-dht.addPeerToRTChan:
				if dht.routingTable.Size() == 0 {
					isBootsrapping = true
					bootstrapCount = 0
					timerCh = nil
				}
				// queryPeer设置为true,因为我们只尝试将查询的对等节点添加到RT
				newlyAdded, err := dht.routingTable.TryAddPeer(p, true, isBootsrapping)
				if err != nil {
					// 对等节点未添加。
					continue
				}
				if newlyAdded {
					// peer was added to the RT, it can now be fixed if needed.
					dht.fixRTIfNeeded()
				} else {
					// the peer is already in our RT, but we just successfully queried it and so let's give it a bump on the query time so we don't ping it too soon for a liveliness check.
					dht.routingTable.UpdateLastSuccessfulOutboundQueryAt(p, time.Now())
				}
			case <-dht.refreshFinishedCh:
				bootstrapCount = bootstrapCount + 1
				if bootstrapCount == 2 {
					timerCh = time.NewTimer(dht.rtFreezeTimeout).C
				}

				old := isBootsrapping
				isBootsrapping = false
				if old {
					dht.rtRefreshManager.RefreshNoWait()
				}

			case <-dht.ctx.Done():
				return
			}
		}
	}()
}

// peerFound 验证发现的对等节点是否支持DHT协议，并探测它以确保它按预期响应DHT查询。
// 如果它无法响应，则不会被添加到路由表中。
// 参数:
//   - p: peer.ID 对等节点ID
func (dht *IpfsDHT) peerFound(p peer.ID) {
	// 如果对等节点已经在路由表中或相应的桶已满，则不尝试添加新的对等节点ID
	if !dht.routingTable.UsefulNewPeer(p) {
		return
	}

	// 验证远程对等节点是否支持正确的DHT协议
	b, err := dht.validRTPeer(p)
	if err != nil {
		logger.Errorw("验证对等节点是否为DHT节点失败", "peer", p, "error", err)
	} else if b {

		// 检查是否达到最大并发查找检查数
		dht.lookupChecksLk.Lock()
		if dht.lookupCheckCapacity == 0 {
			dht.lookupChecksLk.Unlock()
			// 如果达到最大并发查找检查数，则丢弃新的对等节点ID
			return
		}
		dht.lookupCheckCapacity--
		dht.lookupChecksLk.Unlock()

		go func() {
			livelinessCtx, cancel := context.WithTimeout(dht.ctx, dht.lookupCheckTimeout)
			defer cancel()

			// 执行FIND_NODE查询
			err := dht.lookupCheck(livelinessCtx, p)

			dht.lookupChecksLk.Lock()
			dht.lookupCheckCapacity++
			dht.lookupChecksLk.Unlock()

			if err != nil {
				logger.Debugw("已连接的对等节点未按预期响应DHT请求", "peer", p, "error", err)
				return
			}

			// 如果FIND_NODE成功，则认为该对等节点有效
			dht.validPeerFound(p)
		}()
	}
}

// validPeerFound 向路由表发出信号，表明我们找到了一个支持DHT协议并正确响应DHT FindPeers的对等节点
// 参数:
//   - p: peer.ID 对等节点ID
func (dht *IpfsDHT) validPeerFound(p peer.ID) {
	if c := baseLogger.Check(zap.DebugLevel, "找到对等节点"); c != nil {
		c.Write(zap.String("peer", p.String()))
	}

	select {
	case dht.addPeerToRTChan <- p:
	case <-dht.ctx.Done():
		return
	}
}

// peerStoppedDHT 向路由表发出信号，表明对等节点不能再响应DHT查询
// 参数:
//   - p: peer.ID 对等节点ID
func (dht *IpfsDHT) peerStoppedDHT(p peer.ID) {
	logger.Debugw("对等节点停止DHT", "peer", p)
	// 不支持DHT协议的对等节点对我们来说是无效的
	// 在它再次开始支持DHT协议之前，没有必要与其通信
	dht.routingTable.RemovePeer(p)
}

// fixRTIfNeeded 在需要时修复路由表
func (dht *IpfsDHT) fixRTIfNeeded() {
	select {
	case dht.fixLowPeersChan <- struct{}{}:
	default:
	}
}

// FindLocal 查找连接到此DHT的具有给定ID的对等节点，并返回对等节点及其所在的表
// 参数:
//   - ctx: context.Context 上下文
//   - id: peer.ID 对等节点ID
//
// 返回值:
//   - peer.AddrInfo 对等节点地址信息
func (dht *IpfsDHT) FindLocal(ctx context.Context, id peer.ID) peer.AddrInfo {
	_, span := internal.StartSpan(ctx, "IpfsDHT.FindLocal", trace.WithAttributes(attribute.Stringer("PeerID", id)))
	defer span.End()

	if hasValidConnectedness(dht.host, id) {
		return dht.peerstore.PeerInfo(id)
	}
	return peer.AddrInfo{}
}

// nearestPeersToQuery 返回路由表中最近的对等节点
// 参数:
//   - pmes: *pb.Message 消息
//   - count: int 返回的对等节点数量
//
// 返回值:
//   - []peer.ID 对等节点ID列表
func (dht *IpfsDHT) nearestPeersToQuery(pmes *pb.Message, count int) []peer.ID {
	closer := dht.routingTable.NearestPeers(kb.ConvertKey(string(pmes.GetKey())), count)
	return closer
}

// betterPeersToQuery 返回经过额外过滤的nearestPeersToQuery结果
// 参数:
//   - pmes: *pb.Message 消息
//   - from: peer.ID 来源对等节点ID
//   - count: int 返回的对等节点数量
//
// 返回值:
//   - []peer.ID 对等节点ID列表
func (dht *IpfsDHT) betterPeersToQuery(pmes *pb.Message, from peer.ID, count int) []peer.ID {
	closer := dht.nearestPeersToQuery(pmes, count)

	// 没有节点？返回nil
	if closer == nil {
		logger.Infow("没有更近的对等节点可发送", from)
		return nil
	}

	filtered := make([]peer.ID, 0, len(closer))
	for _, clp := range closer {

		// 等于自身？这很糟糕
		if clp == dht.self {
			logger.Error("BUG betterPeersToQuery: 试图返回自身！这不应该发生...")
			return nil
		}
		// 不要将对等节点发送回它们自己
		if clp == from {
			continue
		}

		filtered = append(filtered, clp)
	}

	// 看起来是更近的节点
	return filtered
}

// setMode 设置DHT模式
// 参数:
//   - m: mode DHT模式
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) setMode(m mode) error {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()

	if m == dht.mode {
		return nil
	}

	switch m {
	case modeServer:
		return dht.moveToServerMode()
	case modeClient:
		return dht.moveToClientMode()
	default:
		return fmt.Errorf("无法识别的DHT模式: %d", m)
	}
}

// moveToServerMode 通过libp2p identify更新广播我们能够响应DHT查询，并设置相应的流处理程序
// 注意：我们可能支持使用主要协议之外的协议响应查询，以支持与旧版本DHT协议的互操作性
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) moveToServerMode() error {
	dht.mode = modeServer
	for _, p := range dht.serverProtocols {
		dht.host.SetStreamHandler(p, dht.handleNewStream)
	}
	return nil
}

// moveToClientMode 停止广播（并通过libp2p identify更新撤销广播）我们能够响应DHT查询，并移除相应的流处理程序
// 我们还会终止所有使用已处理协议的入站流
// 注意：我们可能支持使用主要协议之外的协议响应查询，以支持与旧版本DHT协议的互操作性
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) moveToClientMode() error {
	dht.mode = modeClient
	for _, p := range dht.serverProtocols {
		dht.host.RemoveStreamHandler(p)
	}

	pset := make(map[protocol.ID]bool)
	for _, p := range dht.serverProtocols {
		pset[p] = true
	}

	for _, c := range dht.host.Network().Conns() {
		for _, s := range c.GetStreams() {
			if pset[s.Protocol()] {
				if s.Stat().Direction == network.DirInbound {
					_ = s.Reset()
				}
			}
		}
	}
	return nil
}

// getMode 获取DHT模式
// 返回值:
//   - mode DHT模式
func (dht *IpfsDHT) getMode() mode {
	dht.modeLk.Lock()
	defer dht.modeLk.Unlock()
	return dht.mode
}

// Context 返回DHT的上下文
// 返回值:
//   - context.Context DHT上下文
func (dht *IpfsDHT) Context() context.Context {
	return dht.ctx
}

// RoutingTable 返回DHT的路由表
// 返回值:
//   - *kb.RoutingTable 路由表
func (dht *IpfsDHT) RoutingTable() *kb.RoutingTable {
	return dht.routingTable
}

// Close 调用Process Close
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) Close() error {
	dht.cancel()
	dht.wg.Wait()

	var wg sync.WaitGroup
	closes := [...]func() error{
		dht.rtRefreshManager.Close,
		dht.providerStore.Close,
	}
	var errors [len(closes)]error
	wg.Add(len(errors))
	for i, c := range closes {
		go func(i int, c func() error) {
			defer wg.Done()
			errors[i] = c()
		}(i, c)
	}
	wg.Wait()

	return multierr.Combine(errors[:]...)
}

// mkDsKey 创建数据存储键
// 参数:
//   - s: string 键字符串
//
// 返回值:
//   - ds.Key 数据存储键
func mkDsKey(s string) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString([]byte(s)))
}

// PeerID 返回DHT节点的对等节点ID
// 返回值:
//   - peer.ID 对等节点ID
func (dht *IpfsDHT) PeerID() peer.ID {
	return dht.self
}

// PeerKey 返回从DHT节点的对等节点ID转换而来的DHT键
// 返回值:
//   - []byte DHT键
func (dht *IpfsDHT) PeerKey() []byte {
	return kb.ConvertPeerID(dht.self)
}

// Host 返回此DHT正在使用的libp2p主机
// 返回值:
//   - host.Host libp2p主机
func (dht *IpfsDHT) Host() host.Host {
	return dht.host
}

// Ping 向指定的对等节点发送ping消息并等待响应
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) Ping(ctx context.Context, p peer.ID) error {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.Ping", trace.WithAttributes(attribute.Stringer("PeerID", p)))
	defer span.End()
	return dht.protoMessenger.Ping(ctx, p)
}

// NetworkSize 返回DHT网络大小的最新估计
// 实验性：我们不保证此方法将继续存在于代码库中。使用时风险自负
// 返回值:
//   - int32 网络大小
//   - error 错误信息
func (dht *IpfsDHT) NetworkSize() (int32, error) {
	return dht.nsEstimator.NetworkSize()
}

// newContextWithLocalTags 返回一个新的上下文
// 上下文中包含InstanceID和PeerID键
// 它还将接受任何需要作为tag.Mutators添加到上下文中的额外标签
// 参数:
//   - ctx: context.Context 上下文
//   - extraTags: ...tag.Mutator 额外标签
//
// 返回值:
//   - context.Context 新的上下文
func (dht *IpfsDHT) newContextWithLocalTags(ctx context.Context, extraTags ...tag.Mutator) context.Context {
	extraTags = append(
		extraTags,
		tag.Upsert(metrics.KeyPeerID, dht.self.String()),
		tag.Upsert(metrics.KeyInstanceID, fmt.Sprintf("%p", dht)),
	)
	ctx, _ = tag.New(
		ctx,
		extraTags...,
	) // 忽略错误，因为它与此代码的实际功能无关
	return ctx
}

// maybeAddAddrs 可能添加地址
// 参数:
//   - p: peer.ID 对等节点ID
//   - addrs: []ma.Multiaddr 多地址列表
//   - ttl: time.Duration 生存时间
func (dht *IpfsDHT) maybeAddAddrs(p peer.ID, addrs []ma.Multiaddr, ttl time.Duration) {
	// 不要为自己或已连接的对等节点添加地址。我们有更好的地址
	if p == dht.self || hasValidConnectedness(dht.host, p) {
		return
	}
	dht.peerstore.AddAddrs(p, dht.filterAddrs(addrs), ttl)
}

// filterAddrs 过滤地址
// 参数:
//   - addrs: []ma.Multiaddr 多地址列表
//
// 返回值:
//   - []ma.Multiaddr 过滤后的多地址列表
func (dht *IpfsDHT) filterAddrs(addrs []ma.Multiaddr) []ma.Multiaddr {
	if f := dht.addrFilter; f != nil {
		return f(addrs)
	}
	return addrs
}
