package crawler

import (
	"context"
	"sync"
	"time"

	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/protocol"

	logging "github.com/ipfs/go-log/v2"
	//lint:ignore SA1019 TODO migrate away from gogo pb
	"github.com/libp2p/go-msgio/protoio"

	kbucket "github.com/dep2p/kaddht/kbucket"
	pb "github.com/dep2p/kaddht/pb"
)

var (
	// DHT爬虫的日志记录器
	logger = logging.Logger("dht-crawler")

	_ Crawler = (*DefaultCrawler)(nil)
)

type (
	// Crawler 连接到DHT中的主机以跟踪对等节点的路由表
	Crawler interface {
		// Run 从startingPeers开始爬取DHT,根据是否成功联系到对等节点调用handleSuccess或handleFail
		// 参数:
		//   - ctx: context.Context 上下文
		//   - startingPeers: []*peer.AddrInfo 起始对等节点列表
		//   - handleSuccess: HandleQueryResult 查询成功的回调函数
		//   - handleFail: HandleQueryFail 查询失败的回调函数
		Run(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess HandleQueryResult, handleFail HandleQueryFail)
	}

	// DefaultCrawler 提供Crawler接口的默认实现
	DefaultCrawler struct {
		parallelism          int                   // 并行度
		connectTimeout       time.Duration         // 连接超时时间
		queryTimeout         time.Duration         // 查询超时时间
		host                 host.Host             // libp2p主机
		dhtRPC               *pb.ProtocolMessenger // DHT RPC消息器
		dialAddressExtendDur time.Duration         // 地址扩展持续时间
	}
)

// NewDefaultCrawler 创建一个新的DefaultCrawler
// 参数:
//   - host: host.Host libp2p主机
//   - opts: ...Option 配置选项
//
// 返回值:
//   - *DefaultCrawler 爬虫实例
//   - error 错误信息
func NewDefaultCrawler(host host.Host, opts ...Option) (*DefaultCrawler, error) {
	o := new(options)
	if err := defaults(o); err != nil {
		return nil, err
	}
	for _, opt := range opts {
		if err := opt(o); err != nil {
			return nil, err
		}
	}

	pm, err := pb.NewProtocolMessenger(&messageSender{h: host, protocols: o.protocols, timeout: o.perMsgTimeout})
	if err != nil {
		return nil, err
	}

	return &DefaultCrawler{
		parallelism:          o.parallelism,
		connectTimeout:       o.connectTimeout,
		queryTimeout:         3 * o.connectTimeout,
		host:                 host,
		dhtRPC:               pm,
		dialAddressExtendDur: o.dialAddressExtendDur,
	}, nil
}

// MessageSender 处理向指定对等节点发送线路协议消息
type messageSender struct {
	h         host.Host     // libp2p主机
	protocols []protocol.ID // 协议ID列表
	timeout   time.Duration // 超时时间
}

// SendRequest 向对等节点发送消息并等待响应
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//   - pmes: *pb.Message 要发送的消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (ms *messageSender) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	tctx, cancel := context.WithTimeout(ctx, ms.timeout)
	defer cancel()

	s, err := ms.h.NewStream(tctx, p, ms.protocols...)
	if err != nil {
		return nil, err
	}

	w := protoio.NewDelimitedWriter(s)
	if err := w.WriteMsg(pmes); err != nil {
		return nil, err
	}

	r := protoio.NewDelimitedReader(s, network.MessageSizeMax)
	defer func() { _ = s.Close() }()

	msg := new(pb.Message)
	if err := ctxReadMsg(tctx, r, msg); err != nil {
		_ = s.Reset()
		return nil, err
	}

	return msg, nil
}

// ctxReadMsg 在上下文约束下读取消息
// 参数:
//   - ctx: context.Context 上下文
//   - rc: protoio.ReadCloser 读取器
//   - mes: *pb.Message 消息存储
//
// 返回值:
//   - error 错误信息
func ctxReadMsg(ctx context.Context, rc protoio.ReadCloser, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r protoio.ReadCloser) {
		defer close(errc)
		err := r.ReadMsg(mes)
		errc <- err
	}(rc)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// SendMessage 向对等节点发送消息但不等待响应
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//   - pmes: *pb.Message 要发送的消息
//
// 返回值:
//   - error 错误信息
func (ms *messageSender) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	s, err := ms.h.NewStream(ctx, p, ms.protocols...)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	w := protoio.NewDelimitedWriter(s)
	return w.WriteMsg(pmes)
}

// HandleQueryResult 查询对等节点成功时的回调函数类型
type HandleQueryResult func(p peer.ID, rtPeers []*peer.AddrInfo)

// HandleQueryFail 查询对等节点失败时的回调函数类型
type HandleQueryFail func(p peer.ID, err error)

// Run 从初始种子startingPeers开始爬取DHT对等节点
// 参数:
//   - ctx: context.Context 上下文
//   - startingPeers: []*peer.AddrInfo 起始对等节点列表
//   - handleSuccess: HandleQueryResult 查询成功的回调函数
//   - handleFail: HandleQueryFail 查询失败的回调函数
func (c *DefaultCrawler) Run(ctx context.Context, startingPeers []*peer.AddrInfo, handleSuccess HandleQueryResult, handleFail HandleQueryFail) {
	jobs := make(chan peer.ID, 1)
	results := make(chan *queryResult, 1)

	// 启动工作协程
	var wg sync.WaitGroup
	wg.Add(c.parallelism)
	for i := 0; i < c.parallelism; i++ {
		go func() {
			defer wg.Done()
			for p := range jobs {
				qctx, cancel := context.WithTimeout(ctx, c.queryTimeout)
				res := c.queryPeer(qctx, p)
				cancel() // 不要延迟,每个任务后清理
				results <- res
			}
		}()
	}

	defer wg.Wait()
	defer close(jobs)

	var toDial []*peer.AddrInfo
	peersSeen := make(map[peer.ID]struct{})

	numSkipped := 0
	for _, ai := range startingPeers {
		extendAddrs := c.host.Peerstore().Addrs(ai.ID)
		if len(ai.Addrs) > 0 {
			extendAddrs = append(extendAddrs, ai.Addrs...)
			c.host.Peerstore().AddAddrs(ai.ID, extendAddrs, c.dialAddressExtendDur)
		}
		if len(extendAddrs) == 0 {
			numSkipped++
			continue
		}

		toDial = append(toDial, ai)
		peersSeen[ai.ID] = struct{}{}
	}

	if numSkipped > 0 {
		logger.Infof("由于缺少地址跳过了%d个起始节点。开始爬取%d个节点", numSkipped, len(toDial))
	}

	numQueried := 0
	outstanding := 0

	for len(toDial) > 0 || outstanding > 0 {
		var jobCh chan peer.ID
		var nextPeerID peer.ID
		if len(toDial) > 0 {
			jobCh = jobs
			nextPeerID = toDial[0].ID
		}

		select {
		case res := <-results:
			if len(res.data) > 0 {
				logger.Debugf("节点%v有%d个对等节点", res.peer, len(res.data))
				rtPeers := make([]*peer.AddrInfo, 0, len(res.data))
				for p, ai := range res.data {
					c.host.Peerstore().AddAddrs(p, ai.Addrs, c.dialAddressExtendDur)
					if _, ok := peersSeen[p]; !ok {
						peersSeen[p] = struct{}{}
						toDial = append(toDial, ai)
					}
					rtPeers = append(rtPeers, ai)
				}
				if handleSuccess != nil {
					handleSuccess(res.peer, rtPeers)
				}
			} else if handleFail != nil {
				handleFail(res.peer, res.err)
			}
			outstanding--
		case jobCh <- nextPeerID:
			outstanding++
			numQueried++
			toDial = toDial[1:]
			logger.Debugf("开始第%d个,共%d个", numQueried, len(peersSeen))
		}
	}
}

// queryResult 查询结果
type queryResult struct {
	peer peer.ID                    // 对等节点ID
	data map[peer.ID]*peer.AddrInfo // 查询到的数据
	err  error                      // 错误信息
}

// queryPeer 查询单个对等节点
// 参数:
//   - ctx: context.Context 上下文
//   - nextPeer: peer.ID 要查询的对等节点ID
//
// 返回值:
//   - *queryResult 查询结果
func (c *DefaultCrawler) queryPeer(ctx context.Context, nextPeer peer.ID) *queryResult {
	tmpRT, err := kbucket.NewRoutingTable(20, kbucket.ConvertPeerID(nextPeer), time.Hour, c.host.Peerstore(), time.Hour, nil)
	if err != nil {
		logger.Errorf("为节点%v创建路由表时出错: %v", nextPeer, err)
		return &queryResult{nextPeer, nil, err}
	}

	connCtx, cancel := context.WithTimeout(ctx, c.connectTimeout)
	defer cancel()
	err = c.host.Connect(connCtx, peer.AddrInfo{ID: nextPeer})
	if err != nil {
		logger.Debugf("无法连接到节点%v: %v", nextPeer, err)
		return &queryResult{nextPeer, nil, err}
	}

	localPeers := make(map[peer.ID]*peer.AddrInfo)
	var retErr error
	for cpl := 0; cpl <= 15; cpl++ {
		generatePeer, err := tmpRT.GenRandPeerID(uint(cpl))
		if err != nil {
			panic(err)
		}
		peers, err := c.dhtRPC.GetClosestPeers(ctx, nextPeer, generatePeer)
		if err != nil {
			logger.Debugf("在节点%v上查找CPL %d的数据时出错: %v", nextPeer, cpl, err)
			retErr = err
			break
		}
		for _, ai := range peers {
			if _, ok := localPeers[ai.ID]; !ok {
				localPeers[ai.ID] = ai
			}
		}
	}

	if retErr != nil {
		return &queryResult{nextPeer, nil, retErr}
	}

	return &queryResult{nextPeer, localPeers, retErr}
}
