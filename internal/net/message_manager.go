package net

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/protocol"

	logging "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-msgio"

	//lint:ignore SA1019 TODO migrate away from gogo pb
	"github.com/libp2p/go-msgio/protoio"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"

	"github.com/dep2p/kaddht/internal"
	"github.com/dep2p/kaddht/metrics"
	pb "github.com/dep2p/kaddht/pb"
)

// DHT读取消息超时时间
var dhtReadMessageTimeout = 10 * time.Second

// ErrReadTimeout 在超时期间内未读取到消息时发生的错误
var ErrReadTimeout = fmt.Errorf("读取响应超时")

var logger = logging.Logger("dht")

// messageSenderImpl 负责高效地向对等点发送请求和消息,包括重用流。
// 它还跟踪已发送请求和消息的指标。
type messageSenderImpl struct {
	host      host.Host // 需要的网络服务
	smlk      sync.Mutex
	strmap    map[peer.ID]*peerMessageSender
	protocols []protocol.ID
}

// NewMessageSenderImpl 创建新的消息发送器实现
// 参数:
//   - h: host.Host 主机
//   - protos: []protocol.ID 协议列表
//
// 返回值:
//   - pb.MessageSenderWithDisconnect 消息发送器接口
func NewMessageSenderImpl(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect {
	return &messageSenderImpl{
		host:      h,
		strmap:    make(map[peer.ID]*peerMessageSender),
		protocols: protos,
	}
}

// OnDisconnect 处理对等点断开连接事件
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等点ID
func (m *messageSenderImpl) OnDisconnect(ctx context.Context, p peer.ID) {
	m.smlk.Lock()
	defer m.smlk.Unlock()
	ms, ok := m.strmap[p]
	if !ok {
		return
	}
	delete(m.strmap, p)

	// 异步执行,因为ms.lk可能会阻塞一段时间
	go func() {
		if err := ms.lk.Lock(ctx); err != nil {
			return
		}
		defer ms.lk.Unlock()
		ms.invalidate()
	}()
}

// SendRequest 发送请求,同时确保测量RTT以进行延迟测量
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等点ID
//   - pmes: *pb.Message 要发送的消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (m *messageSenderImpl) SendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := m.messageSenderForPeer(ctx, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("打开消息发送器失败", "error", err, "to", p)
		return nil, err
	}

	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		stats.Record(ctx,
			metrics.SentRequests.M(1),
			metrics.SentRequestErrors.M(1),
		)
		logger.Debugw("请求失败", "error", err, "to", p)
		return nil, err
	}

	stats.Record(ctx,
		metrics.SentRequests.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
		metrics.OutboundRequestLatency.M(float64(time.Since(start))/float64(time.Millisecond)),
	)
	m.host.Peerstore().RecordLatency(p, time.Since(start))
	return rpmes, nil
}

// SendMessage 发送消息
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等点ID
//   - pmes: *pb.Message 要发送的消息
//
// 返回值:
//   - error 错误信息
func (m *messageSenderImpl) SendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ctx, _ = tag.New(ctx, metrics.UpsertMessageType(pmes))

	ms, err := m.messageSenderForPeer(ctx, p)
	if err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("打开消息发送器失败", "error", err, "to", p)
		return err
	}

	if err := ms.SendMessage(ctx, pmes); err != nil {
		stats.Record(ctx,
			metrics.SentMessages.M(1),
			metrics.SentMessageErrors.M(1),
		)
		logger.Debugw("消息发送失败", "error", err, "to", p)
		return err
	}

	stats.Record(ctx,
		metrics.SentMessages.M(1),
		metrics.SentBytes.M(int64(pmes.Size())),
	)
	return nil
}

// messageSenderForPeer 获取或创建指定对等点的消息发送器
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等点ID
//
// 返回值:
//   - *peerMessageSender 消息发送器
//   - error 错误信息
func (m *messageSenderImpl) messageSenderForPeer(ctx context.Context, p peer.ID) (*peerMessageSender, error) {
	m.smlk.Lock()
	ms, ok := m.strmap[p]
	if ok {
		m.smlk.Unlock()
		return ms, nil
	}
	ms = &peerMessageSender{p: p, m: m, lk: internal.NewCtxMutex()}
	m.strmap[p] = ms
	m.smlk.Unlock()

	if err := ms.prepOrInvalidate(ctx); err != nil {
		m.smlk.Lock()
		defer m.smlk.Unlock()

		if msCur, ok := m.strmap[p]; ok {
			// 已更改。使用新的,旧的无效且不在map中,可以直接丢弃
			if ms != msCur {
				return msCur, nil
			}
			// 未更改,从map中删除现在无效的流
			delete(m.strmap, p)
		}
		// 无效但不在map中。一定是被断开连接移除了
		return nil, err
	}
	// 一切就绪
	return ms, nil
}

// peerMessageSender 负责向特定对等点发送请求和消息
type peerMessageSender struct {
	s  network.Stream
	r  msgio.ReadCloser
	lk internal.CtxMutex
	p  peer.ID
	m  *messageSenderImpl

	invalid   bool
	singleMes int
}

// invalidate 在从strmap中删除此peerMessageSender之前调用。
// 它防止peerMessageSender被重用/重新初始化然后被遗忘(保持流打开)。
func (ms *peerMessageSender) invalidate() {
	ms.invalid = true
	if ms.s != nil {
		_ = ms.s.Reset()
		ms.s = nil
	}
}

// prepOrInvalidate 准备或使消息发送器无效
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (ms *peerMessageSender) prepOrInvalidate(ctx context.Context) error {
	if err := ms.lk.Lock(ctx); err != nil {
		return err
	}
	defer ms.lk.Unlock()

	if err := ms.prep(ctx); err != nil {
		ms.invalidate()
		return err
	}
	return nil
}

// prep 准备消息发送器
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (ms *peerMessageSender) prep(ctx context.Context) error {
	if ms.invalid {
		return fmt.Errorf("消息发送器已失效")
	}
	if ms.s != nil {
		return nil
	}

	// 我们只想使用主要协议与对等点通信。我们不想查询仅使用我们支持的次要"服务器"协议的对等点(例如,出于向后兼容性原因我们可以响应的旧节点)。
	nstr, err := ms.m.host.NewStream(ctx, ms.p, ms.m.protocols...)
	if err != nil {
		return err
	}

	ms.r = msgio.NewVarintReaderSize(nstr, network.MessageSizeMax)
	ms.s = nstr

	return nil
}

// streamReuseTries 是在放弃并恢复到旧的每流一条消息行为之前,我们将尝试重用到给定对等点的流的次数
const streamReuseTries = 3

// SendMessage 发送消息
// 参数:
//   - ctx: context.Context 上下文
//   - pmes: *pb.Message 要发送的消息
//
// 返回值:
//   - error 错误信息
func (ms *peerMessageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {
	if err := ms.lk.Lock(ctx); err != nil {
		return err
	}
	defer ms.lk.Unlock()

	retry := false
	for {
		if err := ms.prep(ctx); err != nil {
			return err
		}

		if err := ms.writeMsg(pmes); err != nil {
			_ = ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Debugw("写入消息出错", "error", err)
				return err
			}
			logger.Debugw("写入消息出错", "error", err, "retrying", true)
			retry = true
			continue
		}

		var err error
		if ms.singleMes > streamReuseTries {
			err = ms.s.Close()
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return err
	}
}

// SendRequest 发送请求
// 参数:
//   - ctx: context.Context 上下文
//   - pmes: *pb.Message 要发送的消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (ms *peerMessageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
	if err := ms.lk.Lock(ctx); err != nil {
		return nil, err
	}
	defer ms.lk.Unlock()

	retry := false
	for {
		if err := ms.prep(ctx); err != nil {
			return nil, err
		}

		if err := ms.writeMsg(pmes); err != nil {
			_ = ms.s.Reset()
			ms.s = nil

			if retry {
				logger.Debugw("写入消息出错", "error", err)
				return nil, err
			}
			logger.Debugw("写入消息出错", "error", err, "retrying", true)
			retry = true
			continue
		}

		mes := new(pb.Message)
		if err := ms.ctxReadMsg(ctx, mes); err != nil {
			_ = ms.s.Reset()
			ms.s = nil
			if err == context.Canceled {
				// 重试会得到相同的错误
				return nil, err
			}
			if retry {
				logger.Debugw("读取消息出错", "error", err)
				return nil, err
			}
			logger.Debugw("读取消息出错", "error", err, "retrying", true)
			retry = true
			continue
		}

		var err error
		if ms.singleMes > streamReuseTries {
			err = ms.s.Close()
			ms.s = nil
		} else if retry {
			ms.singleMes++
		}

		return mes, err
	}
}

// writeMsg 写入消息
// 参数:
//   - pmes: *pb.Message 要写入的消息
//
// 返回值:
//   - error 错误信息
func (ms *peerMessageSender) writeMsg(pmes *pb.Message) error {
	return WriteMsg(ms.s, pmes)
}

// ctxReadMsg 从上下文中读取消息
// 参数:
//   - ctx: context.Context 上下文
//   - mes: *pb.Message 消息对象
//
// 返回值:
//   - error 错误信息
func (ms *peerMessageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r msgio.ReadCloser) {
		defer close(errc)
		bytes, err := r.ReadMsg()
		defer r.ReleaseMsg(bytes)
		if err != nil {
			errc <- err
			return
		}
		errc <- mes.Unmarshal(bytes)
	}(ms.r)

	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
}

// bufferedDelimitedWriter 在写入消息时执行多个小写入。
// 我们需要缓冲这些写入,以确保不会为每个写入发送新数据包。
type bufferedDelimitedWriter struct {
	*bufio.Writer
	protoio.WriteCloser
}

var writerPool = sync.Pool{
	New: func() interface{} {
		w := bufio.NewWriter(nil)
		return &bufferedDelimitedWriter{
			Writer:      w,
			WriteCloser: protoio.NewDelimitedWriter(w),
		}
	},
}

// WriteMsg 写入消息
// 参数:
//   - w: io.Writer 写入器
//   - mes: *pb.Message 要写入的消息
//
// 返回值:
//   - error 错误信息
func WriteMsg(w io.Writer, mes *pb.Message) error {
	bw := writerPool.Get().(*bufferedDelimitedWriter)
	bw.Reset(w)
	err := bw.WriteMsg(mes)
	if err == nil {
		err = bw.Flush()
	}
	bw.Reset(nil)
	writerPool.Put(bw)
	return err
}

// Flush 刷新缓冲区
// 返回值:
//   - error 错误信息
func (w *bufferedDelimitedWriter) Flush() error {
	return w.Writer.Flush()
}
