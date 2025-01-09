package dht

import (
	"io"
	"time"

	"github.com/dep2p/libp2p/core/network"

	"github.com/dep2p/kaddht/internal/net"
	"github.com/dep2p/kaddht/metrics"
	pb "github.com/dep2p/kaddht/pb"

	"github.com/libp2p/go-msgio"
	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
	"go.uber.org/zap"
)

// DHT流空闲超时时间
var dhtStreamIdleTimeout = 1 * time.Minute

// ErrReadTimeout 在超时期间内未读取到消息时发生的错误
var ErrReadTimeout = net.ErrReadTimeout

// handleNewStream 实现 network.StreamHandler 接口
// 参数:
//   - s: network.Stream 网络流
func (dht *IpfsDHT) handleNewStream(s network.Stream) {
	if dht.handleNewMessage(s) {
		// 如果没有错误退出,优雅关闭
		_ = s.Close()
	} else {
		// 否则,发送错误
		_ = s.Reset()
	}
}

// handleNewMessage 处理新消息
// 参数:
//   - s: network.Stream 网络流
//
// 返回值:
//   - bool 如果写入完成正常则返回true,可以关闭流
func (dht *IpfsDHT) handleNewMessage(s network.Stream) bool {
	ctx := dht.ctx
	r := msgio.NewVarintReaderSize(s, network.MessageSizeMax)

	mPeer := s.Conn().RemotePeer()

	timer := time.AfterFunc(dhtStreamIdleTimeout, func() { _ = s.Reset() })
	defer timer.Stop()

	for {
		if dht.getMode() != modeServer {
			logger.Debugf("非服务器模式下忽略传入的DHT消息")
			return false
		}

		var req pb.Message
		msgbytes, err := r.ReadMsg()
		msgLen := len(msgbytes)
		if err != nil {
			r.ReleaseMsg(msgbytes)
			if err == io.EOF {
				return true
			}
			// 这个字符串测试是必要的,因为没有使用单一的流重置错误实例
			if c := baseLogger.Check(zap.DebugLevel, "读取消息时出错"); c != nil && err.Error() != "stream reset" {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			if msgLen > 0 {
				_ = stats.RecordWithTags(ctx,
					[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
					metrics.ReceivedMessages.M(1),
					metrics.ReceivedMessageErrors.M(1),
					metrics.ReceivedBytes.M(int64(msgLen)),
				)
			}
			return false
		}
		err = req.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			if c := baseLogger.Check(zap.DebugLevel, "解析消息时出错"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Error(err))
			}
			_ = stats.RecordWithTags(ctx,
				[]tag.Mutator{tag.Upsert(metrics.KeyMessageType, "UNKNOWN")},
				metrics.ReceivedMessages.M(1),
				metrics.ReceivedMessageErrors.M(1),
				metrics.ReceivedBytes.M(int64(msgLen)),
			)
			return false
		}

		timer.Reset(dhtStreamIdleTimeout)

		startTime := time.Now()
		ctx, _ := tag.New(ctx,
			tag.Upsert(metrics.KeyMessageType, req.GetType().String()),
		)

		stats.Record(ctx,
			metrics.ReceivedMessages.M(1),
			metrics.ReceivedBytes.M(int64(msgLen)),
		)

		handler := dht.handlerForMsgType(req.GetType())
		if handler == nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "无法处理接收到的消息"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())))
			}
			return false
		}

		if c := baseLogger.Check(zap.DebugLevel, "正在处理消息"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()))
		}
		resp, err := handler(ctx, mPeer, &req)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "处理消息时出错"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		if c := baseLogger.Check(zap.DebugLevel, "消息处理完成"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", time.Since(startTime)))
		}

		if resp == nil {
			continue
		}

		// 发送响应消息
		err = net.WriteMsg(s, resp)
		if err != nil {
			stats.Record(ctx, metrics.ReceivedMessageErrors.M(1))
			if c := baseLogger.Check(zap.DebugLevel, "写入响应时出错"); c != nil {
				c.Write(zap.String("from", mPeer.String()),
					zap.Int32("type", int32(req.GetType())),
					zap.Binary("key", req.GetKey()),
					zap.Error(err))
			}
			return false
		}

		elapsedTime := time.Since(startTime)

		if c := baseLogger.Check(zap.DebugLevel, "已响应消息"); c != nil {
			c.Write(zap.String("from", mPeer.String()),
				zap.Int32("type", int32(req.GetType())),
				zap.Binary("key", req.GetKey()),
				zap.Duration("time", elapsedTime))
		}

		latencyMillis := float64(elapsedTime) / float64(time.Millisecond)
		stats.Record(ctx, metrics.InboundRequestLatency.M(latencyMillis))
	}
}
