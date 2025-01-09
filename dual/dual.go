// Package dual provides an implementation of a split or "dual" dht, where two parallel instances are maintained for the global internet and the local LAN respectively.
package dual

import (
	"context"
	"fmt"
	"sync"

	dht "github.com/dep2p/kaddht"
	"github.com/dep2p/kaddht/internal"
	"github.com/dep2p/kaddht/tracing"

	helper "github.com/dep2p/kaddht/helpers"
	kb "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/kaddht/kbucket/peerdiversity"
	ci "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/protocol"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/ipfs/go-cid"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/hashicorp/go-multierror"
)

// tracer 跟踪器实例
const tracer = tracing.Tracer("dep2p-kad-dht/dual")

// dualName 双重DHT名称
const dualName = "Dual"

// DHT 实现了路由接口,提供两个具体的DHT实现,用于支持全局网络用户和局域网用户
type DHT struct {
	WAN *dht.IpfsDHT // 广域网DHT
	LAN *dht.IpfsDHT // 局域网DHT
}

// LanExtension 用于区分局域网协议请求和广域网DHT请求
const LanExtension protocol.ID = "/lan"

// 断言IPFS对接口的假设没有被破坏。这些不是保证,但可以帮助重构。
var (
	_ routing.ContentRouting = (*DHT)(nil)
	_ routing.Routing        = (*DHT)(nil)
	_ routing.PeerRouting    = (*DHT)(nil)
	_ routing.PubKeyFetcher  = (*DHT)(nil)
	_ routing.ValueStore     = (*DHT)(nil)
)

var (
	maxPrefixCountPerCpl = 2 // 每个CPL的最大前缀数
	maxPrefixCount       = 3 // 最大前缀总数
)

// config DHT配置
type config struct {
	wan, lan []dht.Option // 广域网和局域网DHT选项
}

// apply 应用DHT选项
// 参数:
//   - opts: ...Option DHT选项列表
//
// 返回值:
//   - error 错误信息
func (cfg *config) apply(opts ...Option) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("dual dht option %d failed: %w", i, err)
		}
	}
	return nil
}

// Option 用于配置双重DHT的选项类型
type Option func(*config) error

// WanDHTOption 将给定的DHT选项应用于广域网DHT
// 参数:
//   - opts: ...dht.Option DHT选项列表
//
// 返回值:
//   - Option 配置选项函数
func WanDHTOption(opts ...dht.Option) Option {
	return func(c *config) error {
		c.wan = append(c.wan, opts...)
		return nil
	}
}

// LanDHTOption 将给定的DHT选项应用于局域网DHT
// 参数:
//   - opts: ...dht.Option DHT选项列表
//
// 返回值:
//   - Option 配置选项函数
func LanDHTOption(opts ...dht.Option) Option {
	return func(c *config) error {
		c.lan = append(c.lan, opts...)
		return nil
	}
}

// DHTOption 将给定的DHT选项同时应用于广域网和局域网DHT
// 参数:
//   - opts: ...dht.Option DHT选项列表
//
// 返回值:
//   - Option 配置选项函数
func DHTOption(opts ...dht.Option) Option {
	return func(c *config) error {
		c.lan = append(c.lan, opts...)
		c.wan = append(c.wan, opts...)
		return nil
	}
}

// New 创建一个新的双重DHT实例。提供的选项将转发到两个具体的IpfsDHT内部构造,除了双重DHT用于强制区分局域网和广域网的附加选项。
// 注意:作为参数提供的查询或路由表功能选项将被此构造函数覆盖。
// 参数:
//   - ctx: context.Context 上下文
//   - h: host.Host libp2p主机
//   - options: ...Option 配置选项
//
// 返回值:
//   - *DHT DHT实例
//   - error 错误信息
func New(ctx context.Context, h host.Host, options ...Option) (*DHT, error) {
	var cfg config
	err := cfg.apply(
		WanDHTOption(
			dht.QueryFilter(dht.PublicQueryFilter),
			dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
			dht.RoutingTablePeerDiversityFilter(dht.NewRTPeerDiversityFilter(h, maxPrefixCountPerCpl, maxPrefixCount)),
			// 过滤所有私有地址
			dht.AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr { return ma.FilterAddrs(addrs, manet.IsPublicAddr) }),
		),
	)
	if err != nil {
		return nil, err
	}
	err = cfg.apply(
		LanDHTOption(
			dht.ProtocolExtension(LanExtension),
			dht.QueryFilter(dht.PrivateQueryFilter),
			dht.RoutingTableFilter(dht.PrivateRoutingTableFilter),
			// 过滤本地回环IP地址
			dht.AddressFilter(func(addrs []ma.Multiaddr) []ma.Multiaddr {
				return ma.FilterAddrs(addrs, func(a ma.Multiaddr) bool { return !manet.IsIPLoopback(a) })
			}),
		),
	)
	if err != nil {
		return nil, err
	}
	err = cfg.apply(options...)
	if err != nil {
		return nil, err
	}

	wan, err := dht.New(ctx, h, cfg.wan...)
	if err != nil {
		return nil, err
	}

	// 除非被用户提供的选项覆盖,否则局域网DHT应默认为'AutoServer'模式
	if wan.Mode() != dht.ModeClient {
		cfg.lan = append(cfg.lan, dht.Mode(dht.ModeServer))
	}
	lan, err := dht.New(ctx, h, cfg.lan...)
	if err != nil {
		return nil, err
	}

	impl := DHT{wan, lan}
	return &impl, nil
}

// Close 关闭DHT上下文
// 返回值:
//   - error 错误信息
func (dht *DHT) Close() error {
	return combineErrors(dht.WAN.Close(), dht.LAN.Close())
}

// WANActive 当广域网DHT活跃(有对等节点)时返回true
// 返回值:
//   - bool 是否活跃
func (dht *DHT) WANActive() bool {
	return dht.WAN.RoutingTable().Size() > 0
}

// Provide 将给定的CID添加到内容路由系统
// 参数:
//   - ctx: context.Context 上下文
//   - key: cid.Cid 内容标识符
//   - announce: bool 是否公告
//
// 返回值:
//   - error 错误信息
func (dht *DHT) Provide(ctx context.Context, key cid.Cid, announce bool) (err error) {
	ctx, end := tracer.Provide(dualName, ctx, key, announce)
	defer func() { end(err) }()

	if dht.WANActive() {
		return dht.WAN.Provide(ctx, key, announce)
	}
	return dht.LAN.Provide(ctx, key, announce)
}

// GetRoutingTableDiversityStats 获取路由表多样性统计信息
// 返回值:
//   - []peerdiversity.CplDiversityStats 多样性统计信息
func (dht *DHT) GetRoutingTableDiversityStats() []peerdiversity.CplDiversityStats {
	if dht.WANActive() {
		return dht.WAN.GetRoutingTableDiversityStats()
	}
	return nil
}

// FindProvidersAsync 异步搜索能够提供给定键的对等节点
// 参数:
//   - ctx: context.Context 上下文
//   - key: cid.Cid 内容标识符
//   - count: int 需要的提供者数量
//
// 返回值:
//   - <-chan peer.AddrInfo 提供者信息通道
func (dht *DHT) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) (ch <-chan peer.AddrInfo) {
	ctx, end := tracer.FindProvidersAsync(dualName, ctx, key, count)
	defer func() { ch = end(ch, nil) }()

	reqCtx, cancel := context.WithCancel(ctx)
	outCh := make(chan peer.AddrInfo)

	// Register for and merge query events if we care about them.
	subCtx := reqCtx
	var evtCh <-chan *routing.QueryEvent
	if routing.SubscribesToQueryEvents(ctx) {
		subCtx, evtCh = routing.RegisterForQueryEvents(reqCtx)
	}

	subCtx, span := internal.StartSpan(subCtx, "Dual.worker")
	wanCh := dht.WAN.FindProvidersAsync(subCtx, key, count)
	lanCh := dht.LAN.FindProvidersAsync(subCtx, key, count)
	zeroCount := (count == 0)
	go func() {
		defer span.End()

		defer cancel()
		defer close(outCh)

		found := make(map[peer.ID]struct{}, count)
		var pi peer.AddrInfo
		var qEv *routing.QueryEvent
		for (zeroCount || count > 0) && (wanCh != nil || lanCh != nil) {
			var ok bool
			select {
			case qEv, ok = <-evtCh:
				if !ok {
					evtCh = nil
				} else if qEv != nil && qEv.Type != routing.QueryError {
					routing.PublishQueryEvent(reqCtx, qEv)
				}
				continue
			case pi, ok = <-wanCh:
				if !ok {
					span.AddEvent("wan finished")
					wanCh = nil
					continue
				}
			case pi, ok = <-lanCh:
				if !ok {
					span.AddEvent("lan finished")
					lanCh = nil
					continue
				}
			}
			// already found
			if _, ok = found[pi.ID]; ok {
				continue
			}

			select {
			case outCh <- pi:
				found[pi.ID] = struct{}{}
				count--
			case <-ctx.Done():
				return
			}
		}
		if qEv != nil && qEv.Type == routing.QueryError && len(found) == 0 {
			routing.PublishQueryEvent(reqCtx, qEv)
		}
	}()
	return outCh
}

// FindPeer 搜索具有给定ID的对等节点
// 注意: 使用签名的对等节点记录时,我们可以在任一DHT返回时短路
// 参数:
//   - ctx: context.Context 上下文
//   - pid: peer.ID 对等节点ID
//
// 返回值:
//   - pi: peer.AddrInfo 对等节点地址信息
//   - err: error 错误信息
func (dht *DHT) FindPeer(ctx context.Context, pid peer.ID) (pi peer.AddrInfo, err error) {
	ctx, end := tracer.FindPeer(dualName, ctx, pid)
	defer func() { end(pi, err) }()

	var wg sync.WaitGroup
	wg.Add(2)
	var wanInfo, lanInfo peer.AddrInfo
	var wanErr, lanErr error
	go func() {
		defer wg.Done()
		wanInfo, wanErr = dht.WAN.FindPeer(ctx, pid)
	}()
	go func() {
		defer wg.Done()
		lanInfo, lanErr = dht.LAN.FindPeer(ctx, pid)
	}()

	wg.Wait()

	// 合并地址。尽量避免不必要的工作。
	// 注意: 我们暂时忽略错误,因为许多DHT命令可以同时返回结果和错误。
	ai := peer.AddrInfo{ID: pid}
	if len(wanInfo.Addrs) == 0 {
		ai.Addrs = lanInfo.Addrs
	} else if len(lanInfo.Addrs) == 0 {
		ai.Addrs = wanInfo.Addrs
	} else {
		// 合并地址
		deduped := make(map[string]ma.Multiaddr, len(wanInfo.Addrs)+len(lanInfo.Addrs))
		for _, addr := range wanInfo.Addrs {
			deduped[string(addr.Bytes())] = addr
		}
		for _, addr := range lanInfo.Addrs {
			deduped[string(addr.Bytes())] = addr
		}
		ai.Addrs = make([]ma.Multiaddr, 0, len(deduped))
		for _, addr := range deduped {
			ai.Addrs = append(ai.Addrs, addr)
		}
	}

	// 如果其中一个命令成功,不返回错误
	if wanErr == nil || lanErr == nil {
		return ai, nil
	}

	// 否则,返回我们所拥有的内容并返回错误
	return ai, combineErrors(wanErr, lanErr)
}

// combineErrors 合并两个错误
// 参数:
//   - erra: error 第一个错误
//   - errb: error 第二个错误
//
// 返回值:
//   - error 合并后的错误
func combineErrors(erra, errb error) error {
	// 如果错误相同,只返回一个
	if erra == errb {
		return erra
	}

	// 如果其中一个错误是kb查找失败(路由表中没有对等节点),返回另一个
	if erra == kb.ErrLookupFailure {
		return errb
	} else if errb == kb.ErrLookupFailure {
		return erra
	}
	return multierror.Append(erra, errb).ErrorOrNil()
}

// Bootstrap 允许调用者提示路由系统进入引导状态并保持该状态
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - err: error 错误信息
func (dht *DHT) Bootstrap(ctx context.Context) (err error) {
	ctx, end := tracer.Bootstrap(dualName, ctx)
	defer func() { end(err) }()

	erra := dht.WAN.Bootstrap(ctx)
	errb := dht.LAN.Bootstrap(ctx)
	return combineErrors(erra, errb)
}

// PutValue 添加与给定键对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - err: error 错误信息
func (dht *DHT) PutValue(ctx context.Context, key string, val []byte, opts ...routing.Option) (err error) {
	ctx, end := tracer.PutValue(dualName, ctx, key, val, opts...)
	defer func() { end(err) }()

	if dht.WANActive() {
		return dht.WAN.PutValue(ctx, key, val, opts...)
	}
	return dht.LAN.PutValue(ctx, key, val, opts...)
}

// GetValue 搜索与给定键对应的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - result: []byte 结果值
//   - err: error 错误信息
func (d *DHT) GetValue(ctx context.Context, key string, opts ...routing.Option) (result []byte, err error) {
	ctx, end := tracer.GetValue(dualName, ctx, key, opts...)
	defer func() { end(result, err) }()

	lanCtx, cancelLan := context.WithCancel(ctx)
	defer cancelLan()

	var (
		lanVal    []byte
		lanErr    error
		lanWaiter sync.WaitGroup
	)
	lanWaiter.Add(1)
	go func() {
		defer lanWaiter.Done()
		lanVal, lanErr = d.LAN.GetValue(lanCtx, key, opts...)
	}()

	wanVal, wanErr := d.WAN.GetValue(ctx, key, opts...)
	if wanErr == nil {
		cancelLan()
	}
	lanWaiter.Wait()
	if wanErr == nil {
		return wanVal, nil
	}
	if lanErr == nil {
		return lanVal, nil
	}
	return nil, combineErrors(wanErr, lanErr)
}

// SearchValue 从此值搜索更好的值
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - ch: <-chan []byte 结果通道
//   - err: error 错误信息
func (dht *DHT) SearchValue(ctx context.Context, key string, opts ...routing.Option) (ch <-chan []byte, err error) {
	ctx, end := tracer.SearchValue(dualName, ctx, key, opts...)
	defer func() { ch, err = end(ch, err) }()

	p := helper.Parallel{Routers: []routing.Routing{dht.WAN, dht.LAN}, Validator: dht.WAN.Validator}
	return p.SearchValue(ctx, key, opts...)
}

// GetPublicKey 返回给定对等节点的公钥
// 参数:
//   - ctx: context.Context 上下文
//   - pid: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (dht *DHT) GetPublicKey(ctx context.Context, pid peer.ID) (ci.PubKey, error) {
	p := helper.Parallel{Routers: []routing.Routing{dht.WAN, dht.LAN}, Validator: dht.WAN.Validator}
	return p.GetPublicKey(ctx, pid)
}
