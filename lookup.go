package dht

import (
	"context"
	"fmt"
	"time"

	"github.com/dep2p/kaddht/internal"
	kb "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/kaddht/metrics"
	"github.com/dep2p/kaddht/qpeerset"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"go.opentelemetry.io/otel/trace"
)

// GetClosestPeers 执行 Kademlia '节点查找'操作
// 返回一个包含距离给定key最近的K个节点的通道
//
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 查找的键值
//
// 返回值:
//   - []peer.ID 最近的K个节点ID
//   - error 错误信息
//
// 如果上下文被取消,该函数将返回上下文错误以及目前找到的最近的K个节点
func (dht *IpfsDHT) GetClosestPeers(ctx context.Context, key string) ([]peer.ID, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetClosestPeers", trace.WithAttributes(internal.KeyAsAttribute("Key", key)))
	defer span.End()

	if key == "" {
		return nil, fmt.Errorf("不能查找空键值")
	}

	//TODO: 我可以打破接口! 返回 []peer.ID
	lookupRes, err := dht.runLookupWithFollowup(ctx, key, dht.pmGetClosestPeers(key), func(*qpeerset.QueryPeerset) bool { return false })

	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil || !lookupRes.completed {
		return lookupRes.peers, err
	}

	// 跟踪查找结果用于网络规模估算
	if err = dht.nsEstimator.Track(key, lookupRes.closest); err != nil {
		logger.Warnf("网络规模估算器跟踪节点: %s", err)
	}

	if ns, err := dht.nsEstimator.NetworkSize(); err == nil {
		metrics.NetworkSize.M(int64(ns))
	}

	// 由于查询成功,刷新此键值的cpl
	dht.routingTable.ResetCplRefreshedAtForID(kb.ConvertKey(key), time.Now())

	return lookupRes.peers, nil
}

// pmGetClosestPeers 是GetClosestPeer查询函数的协议消息版本
//
// 参数:
//   - ctx: context.Context 上下文
//   - key: string 查找的键值
//
// 返回值:
//   - queryFn 查询函数
func (dht *IpfsDHT) pmGetClosestPeers(key string) queryFn {
	return func(ctx context.Context, p peer.ID) ([]*peer.AddrInfo, error) {
		// DHT查询命令
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type: routing.SendingQuery,
			ID:   p,
		})

		peers, err := dht.protoMessenger.GetClosestPeers(ctx, p, peer.ID(key))
		if err != nil {
			logger.Debugf("获取更近节点时出错: %s", err)
			routing.PublishQueryEvent(ctx, &routing.QueryEvent{
				Type:  routing.QueryError,
				ID:    p,
				Extra: err.Error(),
			})
			return nil, err
		}

		// DHT查询命令
		routing.PublishQueryEvent(ctx, &routing.QueryEvent{
			Type:      routing.PeerResponse,
			ID:        p,
			Responses: peers,
		})

		return peers, err
	}
}
