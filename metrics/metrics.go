package metrics

import (
	pb "github.com/dep2p/kaddht/pb"
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

var (
	// 默认字节分布
	defaultBytesDistribution = view.Distribution(1024, 2048, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824, 4294967296)
	// 默认毫秒分布
	defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000)
)

// Keys 标签键
var (
	KeyMessageType, _ = tag.NewKey("message_type")
	KeyPeerID, _      = tag.NewKey("peer_id")
	// KeyInstanceID 通过指针地址标识DHT实例
	// 用于区分具有相同对等ID的不同DHT
	KeyInstanceID, _ = tag.NewKey("instance_id")
)

// UpsertMessageType 将pb.Message的消息类型更新到KeyMessageType中
// 参数:
//   - m: *pb.Message 消息对象
//
// 返回值:
//   - tag.Mutator 标签变更器
func UpsertMessageType(m *pb.Message) tag.Mutator {
	return tag.Upsert(KeyMessageType, m.Type.String())
}

// Measures 度量指标
var (
	ReceivedMessages       = stats.Int64("libp2p.io/dht/kad/received_messages", "每个RPC接收的消息总数", stats.UnitDimensionless)
	ReceivedMessageErrors  = stats.Int64("libp2p.io/dht/kad/received_message_errors", "每个RPC接收消息的错误总数", stats.UnitDimensionless)
	ReceivedBytes          = stats.Int64("libp2p.io/dht/kad/received_bytes", "每个RPC接收的总字节数", stats.UnitBytes)
	InboundRequestLatency  = stats.Float64("libp2p.io/dht/kad/inbound_request_latency", "每个RPC的延迟", stats.UnitMilliseconds)
	OutboundRequestLatency = stats.Float64("libp2p.io/dht/kad/outbound_request_latency", "每个RPC的延迟", stats.UnitMilliseconds)
	SentMessages           = stats.Int64("libp2p.io/dht/kad/sent_messages", "每个RPC发送的消息总数", stats.UnitDimensionless)
	SentMessageErrors      = stats.Int64("libp2p.io/dht/kad/sent_message_errors", "每个RPC发送消息的错误总数", stats.UnitDimensionless)
	SentRequests           = stats.Int64("libp2p.io/dht/kad/sent_requests", "每个RPC发送的请求总数", stats.UnitDimensionless)
	SentRequestErrors      = stats.Int64("libp2p.io/dht/kad/sent_request_errors", "每个RPC发送请求的错误总数", stats.UnitDimensionless)
	SentBytes              = stats.Int64("libp2p.io/dht/kad/sent_bytes", "每个RPC发送的总字节数", stats.UnitBytes)
	NetworkSize            = stats.Int64("libp2p.io/dht/kad/network_size", "网络规模估计", stats.UnitDimensionless)
)

// Views 视图定义
var (
	ReceivedMessagesView = &view.View{
		Measure:     ReceivedMessages,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	ReceivedMessageErrorsView = &view.View{
		Measure:     ReceivedMessageErrors,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	ReceivedBytesView = &view.View{
		Measure:     ReceivedBytes,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: defaultBytesDistribution,
	}
	InboundRequestLatencyView = &view.View{
		Measure:     InboundRequestLatency,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: defaultMillisecondsDistribution,
	}
	OutboundRequestLatencyView = &view.View{
		Measure:     OutboundRequestLatency,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: defaultMillisecondsDistribution,
	}
	SentMessagesView = &view.View{
		Measure:     SentMessages,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	SentMessageErrorsView = &view.View{
		Measure:     SentMessageErrors,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	SentRequestsView = &view.View{
		Measure:     SentRequests,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	SentRequestErrorsView = &view.View{
		Measure:     SentRequestErrors,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
	SentBytesView = &view.View{
		Measure:     SentBytes,
		TagKeys:     []tag.Key{KeyMessageType, KeyPeerID, KeyInstanceID},
		Aggregation: defaultBytesDistribution,
	}
	NetworkSizeView = &view.View{
		Measure:     NetworkSize,
		TagKeys:     []tag.Key{KeyPeerID, KeyInstanceID},
		Aggregation: view.Count(),
	}
)

// DefaultViews 包含所有默认视图
// 返回值:
//   - []*view.View 视图列表
var DefaultViews = []*view.View{
	ReceivedMessagesView,
	ReceivedMessageErrorsView,
	ReceivedBytesView,
	InboundRequestLatencyView,
	OutboundRequestLatencyView,
	SentMessagesView,
	SentMessageErrorsView,
	SentRequestsView,
	SentRequestErrorsView,
	SentBytesView,
	NetworkSizeView,
}
