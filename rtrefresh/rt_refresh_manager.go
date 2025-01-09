package rtrefresh

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dep2p/kaddht/internal"
	kbucket "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/hashicorp/go-multierror"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-base32"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var logger = logging.Logger("dht/RtRefreshManager")

const (
	peerPingTimeout = 10 * time.Second
)

// triggerRefreshReq 触发刷新请求
type triggerRefreshReq struct {
	respCh          chan error // 响应通道
	forceCplRefresh bool       // 是否强制刷新CPL
}

// RtRefreshManager 路由表刷新管理器
type RtRefreshManager struct {
	ctx      context.Context
	cancel   context.CancelFunc
	refcount sync.WaitGroup

	// DHT节点的对等ID,即自身对等ID
	h         host.Host
	dhtPeerId peer.ID
	rt        *kbucket.RoutingTable

	enableAutoRefresh   bool                                        // 是否启用自动刷新
	refreshKeyGenFnc    func(cpl uint) (string, error)              // 生成用于刷新此CPL的查询键
	refreshQueryFnc     func(ctx context.Context, key string) error // 运行刷新查询
	refreshPingFnc      func(ctx context.Context, p peer.ID) error  // 检查远程节点存活性的请求
	refreshQueryTimeout time.Duration                               // 单个刷新查询的超时时间

	// 两次周期性刷新之间的间隔
	// 如果距离上次刷新的时间未超过此间隔,CPL不会被刷新,除非执行"强制"刷新
	refreshInterval                    time.Duration
	successfulOutboundQueryGracePeriod time.Duration

	triggerRefresh chan *triggerRefreshReq // 写入刷新请求的通道

	refreshDoneCh chan struct{} // 每次刷新完成后写入此通道
}

// NewRtRefreshManager 创建新的路由表刷新管理器
// 参数:
//   - h: host.Host 主机
//   - rt: *kbucket.RoutingTable 路由表
//   - autoRefresh: bool 是否自动刷新
//   - refreshKeyGenFnc: func(cpl uint) (string, error) 生成刷新键的函数
//   - refreshQueryFnc: func(ctx context.Context, key string) error 执行刷新查询的函数
//   - refreshPingFnc: func(ctx context.Context, p peer.ID) error 执行ping的函数
//   - refreshQueryTimeout: time.Duration 刷新查询超时时间
//   - refreshInterval: time.Duration 刷新间隔
//   - successfulOutboundQueryGracePeriod: time.Duration 成功外发查询的宽限期
//   - refreshDoneCh: chan struct{} 刷新完成通道
//
// 返回值:
//   - *RtRefreshManager 刷新管理器实例
//   - error 错误信息
func NewRtRefreshManager(h host.Host, rt *kbucket.RoutingTable, autoRefresh bool,
	refreshKeyGenFnc func(cpl uint) (string, error),
	refreshQueryFnc func(ctx context.Context, key string) error,
	refreshPingFnc func(ctx context.Context, p peer.ID) error,
	refreshQueryTimeout time.Duration,
	refreshInterval time.Duration,
	successfulOutboundQueryGracePeriod time.Duration,
	refreshDoneCh chan struct{}) (*RtRefreshManager, error) {

	ctx, cancel := context.WithCancel(context.Background())
	return &RtRefreshManager{
		ctx:       ctx,
		cancel:    cancel,
		h:         h,
		dhtPeerId: h.ID(),
		rt:        rt,

		enableAutoRefresh: autoRefresh,
		refreshKeyGenFnc:  refreshKeyGenFnc,
		refreshQueryFnc:   refreshQueryFnc,
		refreshPingFnc:    refreshPingFnc,

		refreshQueryTimeout:                refreshQueryTimeout,
		refreshInterval:                    refreshInterval,
		successfulOutboundQueryGracePeriod: successfulOutboundQueryGracePeriod,

		triggerRefresh: make(chan *triggerRefreshReq),
		refreshDoneCh:  refreshDoneCh,
	}, nil
}

// Start 启动刷新管理器
func (r *RtRefreshManager) Start() {
	r.refcount.Add(1)
	go r.loop()
}

// Close 关闭刷新管理器
// 返回值:
//   - error 错误信息
func (r *RtRefreshManager) Close() error {
	r.cancel()
	r.refcount.Wait()
	return nil
}

// Refresh 请求刷新管理器刷新路由表
// 如果force参数设置为true,所有桶都将被刷新,不考虑上次刷新时间
//
// 返回的通道将阻塞直到刷新完成,然后返回错误并关闭。该通道是带缓冲的,可以安全地忽略
// 参数:
//   - force: bool 是否强制刷新
//
// 返回值:
//   - <-chan error 错误通道
func (r *RtRefreshManager) Refresh(force bool) <-chan error {
	resp := make(chan error, 1)
	r.refcount.Add(1)
	go func() {
		defer r.refcount.Done()
		select {
		case r.triggerRefresh <- &triggerRefreshReq{respCh: resp, forceCplRefresh: force}:
		case <-r.ctx.Done():
			resp <- r.ctx.Err()
			close(resp)
		}
	}()

	return resp
}

// RefreshNoWait 请求刷新管理器刷新路由表
// 如果请求无法通过,则继续执行而不阻塞
func (r *RtRefreshManager) RefreshNoWait() {
	select {
	case r.triggerRefresh <- &triggerRefreshReq{}:
	default:
	}
}

// pingAndEvictPeers ping路由表中在应该被听到/联系的时间间隔内没有响应的节点,如果它们不回复则将其驱逐
// 参数:
//   - ctx: context.Context 上下文
func (r *RtRefreshManager) pingAndEvictPeers(ctx context.Context) {
	ctx, span := internal.StartSpan(ctx, "RefreshManager.PingAndEvictPeers")
	defer span.End()

	var peersChecked int
	var alive int64
	var wg sync.WaitGroup
	peers := r.rt.GetPeerInfos()
	for _, ps := range peers {
		if time.Since(ps.LastSuccessfulOutboundQueryAt) <= r.successfulOutboundQueryGracePeriod {
			continue
		}

		peersChecked++
		wg.Add(1)
		go func(ps kbucket.PeerInfo) {
			defer wg.Done()

			livelinessCtx, cancel := context.WithTimeout(ctx, peerPingTimeout)
			defer cancel()
			peerIdStr := ps.Id.String()
			livelinessCtx, span := internal.StartSpan(livelinessCtx, "RefreshManager.PingAndEvictPeers.worker", trace.WithAttributes(attribute.String("peer", peerIdStr)))
			defer span.End()

			if err := r.h.Connect(livelinessCtx, peer.AddrInfo{ID: ps.Id}); err != nil {
				logger.Debugw("连接失败后驱逐节点", "peer", peerIdStr, "error", err)
				span.RecordError(err)
				r.rt.RemovePeer(ps.Id)
				return
			}

			if err := r.refreshPingFnc(livelinessCtx, ps.Id); err != nil {
				logger.Debugw("ping失败后驱逐节点", "peer", peerIdStr, "error", err)
				span.RecordError(err)
				r.rt.RemovePeer(ps.Id)
				return
			}

			atomic.AddInt64(&alive, 1)
		}(ps)
	}
	wg.Wait()

	span.SetAttributes(attribute.Int("NumPeersChecked", peersChecked), attribute.Int("NumPeersSkipped", len(peers)-peersChecked), attribute.Int64("NumPeersAlive", alive))
}

// loop 刷新管理器的主循环
func (r *RtRefreshManager) loop() {
	defer r.refcount.Done()

	var refreshTickrCh <-chan time.Time
	if r.enableAutoRefresh {
		err := r.doRefresh(r.ctx, true)
		if err != nil {
			logger.Warn("刷新路由表失败", err)
		}
		t := time.NewTicker(r.refreshInterval)
		defer t.Stop()
		refreshTickrCh = t.C
	}

	for {
		var waiting []chan<- error
		var forced bool
		select {
		case <-refreshTickrCh:
		case triggerRefreshReq := <-r.triggerRefresh:
			if triggerRefreshReq.respCh != nil {
				waiting = append(waiting, triggerRefreshReq.respCh)
			}
			forced = forced || triggerRefreshReq.forceCplRefresh
		case <-r.ctx.Done():
			return
		}

		// 如果同时有多个刷新请求在等待,则批量处理它们
	OuterLoop:
		for {
			select {
			case triggerRefreshReq := <-r.triggerRefresh:
				if triggerRefreshReq.respCh != nil {
					waiting = append(waiting, triggerRefreshReq.respCh)
				}
				forced = forced || triggerRefreshReq.forceCplRefresh
			default:
				break OuterLoop
			}
		}

		ctx, span := internal.StartSpan(r.ctx, "RefreshManager.Refresh")

		r.pingAndEvictPeers(ctx)

		// 查询自身并刷新所需的桶
		err := r.doRefresh(ctx, forced)
		for _, w := range waiting {
			w <- err
			close(w)
		}
		if err != nil {
			logger.Warnw("刷新路由表失败", "error", err)
		}

		span.End()
	}
}

// doRefresh 执行刷新操作
// 参数:
//   - ctx: context.Context 上下文
//   - forceRefresh: bool 是否强制刷新
//
// 返回值:
//   - error 错误信息
func (r *RtRefreshManager) doRefresh(ctx context.Context, forceRefresh bool) error {
	ctx, span := internal.StartSpan(ctx, "RefreshManager.doRefresh")
	defer span.End()

	var merr error

	if err := r.queryForSelf(ctx); err != nil {
		merr = multierror.Append(merr, err)
	}

	refreshCpls := r.rt.GetTrackedCplsForRefresh()

	rfnc := func(cpl uint) (err error) {
		if forceRefresh {
			err = r.refreshCpl(ctx, cpl)
		} else {
			err = r.refreshCplIfEligible(ctx, cpl, refreshCpls[cpl])
		}
		return
	}

	for c := range refreshCpls {
		cpl := uint(c)
		if err := rfnc(cpl); err != nil {
			merr = multierror.Append(merr, err)
		} else {
			// 如果在路由表的某个CPL处看到间隙,我们只刷新到路由表中最大的CPL或(2 * (有间隙的CPL + 1)),取较小值
			// 这是为了防止刷新网络中没有节点但恰好在我们有节点的很高CPL之前的CPL
			// 2 * (CPL + 1)这个数字是可以证明的,如果程序员在大学数学课上更专心一点就会在这里写出证明
			// 所以请耐心等待,相关文档很快会发布
			if r.rt.NPeersForCpl(cpl) == 0 {
				lastCpl := min(2*(c+1), len(refreshCpls)-1)
				for i := c + 1; i < lastCpl+1; i++ {
					if err := rfnc(uint(i)); err != nil {
						merr = multierror.Append(merr, err)
					}
				}
				return merr
			}
		}
	}

	select {
	case r.refreshDoneCh <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return merr
}

// min 返回两个整数中的较小值
// 参数:
//   - a: int 第一个整数
//   - b: int 第二个整数
//
// 返回值:
//   - int 较小的整数
func min(a int, b int) int {
	if a <= b {
		return a
	}

	return b
}

// refreshCplIfEligible 如果CPL符合刷新条件则刷新它
// 参数:
//   - ctx: context.Context 上下文
//   - cpl: uint CPL值
//   - lastRefreshedAt: time.Time 上次刷新时间
//
// 返回值:
//   - error 错误信息
func (r *RtRefreshManager) refreshCplIfEligible(ctx context.Context, cpl uint, lastRefreshedAt time.Time) error {
	if time.Since(lastRefreshedAt) <= r.refreshInterval {
		logger.Debugf("不刷新cpl %d,因为距离上次刷新的时间未超过间隔", cpl)
		return nil
	}

	return r.refreshCpl(ctx, cpl)
}

// refreshCpl 刷新指定的CPL
// 参数:
//   - ctx: context.Context 上下文
//   - cpl: uint CPL值
//
// 返回值:
//   - error 错误信息
func (r *RtRefreshManager) refreshCpl(ctx context.Context, cpl uint) error {
	ctx, span := internal.StartSpan(ctx, "RefreshManager.refreshCpl", trace.WithAttributes(attribute.Int("cpl", int(cpl))))
	defer span.End()

	// 生成用于刷新CPL的查询键
	key, err := r.refreshKeyGenFnc(cpl)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("为cpl=%d生成查询键失败, err=%s", cpl, err)
	}

	logger.Infof("开始使用键%s刷新cpl %d (路由表大小为%d)",
		loggableRawKeyString(key), cpl, r.rt.Size())

	if err := r.runRefreshDHTQuery(ctx, key); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("刷新cpl=%d失败, err=%s", cpl, err)
	}

	sz := r.rt.Size()
	logger.Infof("完成刷新cpl %d, 路由表大小现在为%d", cpl, sz)
	span.SetAttributes(attribute.Int("NewSize", sz))
	return nil
}

// queryForSelf 查询自身
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (r *RtRefreshManager) queryForSelf(ctx context.Context) error {
	ctx, span := internal.StartSpan(ctx, "RefreshManager.queryForSelf")
	defer span.End()

	if err := r.runRefreshDHTQuery(ctx, string(r.dhtPeerId)); err != nil {
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("查询自身失败, err=%s", err)
	}
	return nil
}

// runRefreshDHTQuery 执行刷新DHT查询
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 查询键
//
// 返回值:
//   - error 错误信息
func (r *RtRefreshManager) runRefreshDHTQuery(ctx context.Context, key string) error {
	queryCtx, cancel := context.WithTimeout(ctx, r.refreshQueryTimeout)
	defer cancel()

	err := r.refreshQueryFnc(queryCtx, key)

	if err == nil || (err == context.DeadlineExceeded && queryCtx.Err() == context.DeadlineExceeded) {
		return nil
	}

	return err
}

// loggableRawKeyString 可记录的原始键字符串类型
type loggableRawKeyString string

// String 实现Stringer接口
// 返回值:
//   - string 编码后的字符串
func (lk loggableRawKeyString) String() string {
	k := string(lk)

	if len(k) == 0 {
		return k
	}

	encStr := base32.RawStdEncoding.EncodeToString([]byte(k))

	return encStr
}
