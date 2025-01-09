package config

import (
	"fmt"
	"time"

	"github.com/dep2p/kaddht/amino"
	"github.com/dep2p/kaddht/internal/net"
	"github.com/dep2p/kaddht/ipns"
	"github.com/dep2p/kaddht/kbucket/peerdiversity"
	pb "github.com/dep2p/kaddht/pb"
	"github.com/dep2p/kaddht/providers"
	record "github.com/dep2p/kaddht/record"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/protocol"
	ds "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	ma "github.com/multiformats/go-multiaddr"
)

// DefaultPrefix 是默认情况下附加到所有DHT协议的应用程序特定前缀
const DefaultPrefix protocol.ID = amino.ProtocolPrefix

// ModeOpt 描述DHT应该在什么模式下运行
type ModeOpt int

// QueryFilterFunc 是在查询时考虑要拨号的对等点时应用的过滤器
// 参数:
//   - dht: interface{} DHT实例
//   - ai: peer.AddrInfo 对等点地址信息
//
// 返回值:
//   - bool 是否允许该对等点
type QueryFilterFunc func(dht interface{}, ai peer.AddrInfo) bool

// RouteTableFilterFunc 是在考虑要保留在本地路由表中的连接时应用的过滤器
// 参数:
//   - dht: interface{} DHT实例
//   - p: peer.ID 对等点ID
//
// 返回值:
//   - bool 是否保留该连接
type RouteTableFilterFunc func(dht interface{}, p peer.ID) bool

// Config 是构造DHT时可以使用的所有选项的结构
type Config struct {
	Datastore              ds.Batching
	Validator              record.Validator
	ValidatorChanged       bool // 如果为true,表示验证器已更改,不应使用默认值
	Mode                   ModeOpt
	ProtocolPrefix         protocol.ID
	V1ProtocolOverride     protocol.ID
	BucketSize             int
	Concurrency            int
	Resiliency             int
	MaxRecordAge           time.Duration
	EnableProviders        bool
	EnableValues           bool
	ProviderStore          providers.ProviderStore
	QueryPeerFilter        QueryFilterFunc
	LookupCheckConcurrency int
	MsgSenderBuilder       func(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect

	RoutingTable struct {
		RefreshQueryTimeout time.Duration
		RefreshInterval     time.Duration
		AutoRefresh         bool
		LatencyTolerance    time.Duration
		CheckInterval       time.Duration
		PeerFilter          RouteTableFilterFunc
		DiversityFilter     peerdiversity.PeerIPGroupFilter
	}

	BootstrapPeers func() []peer.AddrInfo
	AddressFilter  func([]ma.Multiaddr) []ma.Multiaddr

	// 测试特定的配置选项
	DisableFixLowPeers          bool
	TestAddressUpdateProcessing bool

	EnableOptimisticProvide       bool
	OptimisticProvideJobsPoolSize int
}

// EmptyQueryFilter 空的查询过滤器
// 参数:
//   - _: interface{} DHT实例(未使用)
//   - ai: peer.AddrInfo 对等点地址信息(未使用)
//
// 返回值:
//   - bool 总是返回true
func EmptyQueryFilter(_ interface{}, ai peer.AddrInfo) bool { return true }

// EmptyRTFilter 空的路由表过滤器
// 参数:
//   - _: interface{} DHT实例(未使用)
//   - p: peer.ID 对等点ID(未使用)
//
// 返回值:
//   - bool 总是返回true
func EmptyRTFilter(_ interface{}, p peer.ID) bool { return true }

// Apply 将给定的选项应用到此配置
// 参数:
//   - opts: ...Option 要应用的选项列表
//
// 返回值:
//   - error 错误信息
func (c *Config) Apply(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(c); err != nil {
			return fmt.Errorf("DHT选项 %d 失败: %s", i, err)
		}
	}
	return nil
}

// ApplyFallbacks 设置在配置创建期间无法应用的默认值,因为它们依赖于其他配置参数(例如optA默认为optB的2倍)和/或主机
// 参数:
//   - h: host.Host 主机实例
//
// 返回值:
//   - error 错误信息
func (c *Config) ApplyFallbacks(h host.Host) error {
	if !c.ValidatorChanged {
		nsval, ok := c.Validator.(record.NamespacedValidator)
		if ok {
			if _, pkFound := nsval["pk"]; !pkFound {
				nsval["pk"] = record.PublicKeyValidator{}
			}
			if _, ipnsFound := nsval["ipns"]; !ipnsFound {
				nsval["ipns"] = ipns.Validator{KeyBook: h.Peerstore()}
			}
		} else {
			return fmt.Errorf("默认验证器被更改但未标记为已更改")
		}
	}
	return nil
}

// Option DHT选项类型
type Option func(*Config) error

// Defaults 是默认的DHT选项。此选项将自动添加到传递给DHT构造函数的任何选项之前
var Defaults = func(o *Config) error {
	o.Validator = record.NamespacedValidator{}
	o.Datastore = dssync.MutexWrap(ds.NewMapDatastore())
	o.ProtocolPrefix = DefaultPrefix
	o.EnableProviders = true
	o.EnableValues = true
	o.QueryPeerFilter = EmptyQueryFilter
	o.MsgSenderBuilder = net.NewMessageSenderImpl

	o.RoutingTable.LatencyTolerance = 10 * time.Second
	o.RoutingTable.RefreshQueryTimeout = 10 * time.Second
	o.RoutingTable.RefreshInterval = 10 * time.Minute
	o.RoutingTable.AutoRefresh = true
	o.RoutingTable.PeerFilter = EmptyRTFilter

	o.MaxRecordAge = providers.ProvideValidity

	o.BucketSize = amino.DefaultBucketSize
	o.Concurrency = amino.DefaultConcurrency
	o.Resiliency = amino.DefaultResiliency
	o.LookupCheckConcurrency = 256

	// 注意:将其设置为OptProvReturnRatio * BucketSize的4倍是有意义的
	o.OptimisticProvideJobsPoolSize = 60

	return nil
}

// Validate 验证配置
// 返回值:
//   - error 错误信息
func (c *Config) Validate() error {
	// 仅当前缀匹配Amino DHT时才验证和强制执行配置
	if c.ProtocolPrefix != DefaultPrefix {
		return nil
	}
	if c.BucketSize != amino.DefaultBucketSize {
		return fmt.Errorf("协议前缀 %s 必须使用桶大小 %d", DefaultPrefix, amino.DefaultBucketSize)
	}
	if !c.EnableProviders {
		return fmt.Errorf("协议前缀 %s 必须启用提供者", DefaultPrefix)
	}
	if !c.EnableValues {
		return fmt.Errorf("协议前缀 %s 必须启用值", DefaultPrefix)
	}

	nsval, isNSVal := c.Validator.(record.NamespacedValidator)
	if !isNSVal {
		return fmt.Errorf("协议前缀 %s 必须使用命名空间验证器", DefaultPrefix)
	}

	if len(nsval) != 2 {
		return fmt.Errorf("协议前缀 %s 必须恰好有两个命名空间验证器 - /pk 和 /ipns", DefaultPrefix)
	}

	if pkVal, pkValFound := nsval["pk"]; !pkValFound {
		return fmt.Errorf("协议前缀 %s 必须支持 /pk 命名空间验证器", DefaultPrefix)
	} else if _, ok := pkVal.(record.PublicKeyValidator); !ok {
		return fmt.Errorf("协议前缀 %s 必须对 /pk 命名空间使用 record.PublicKeyValidator", DefaultPrefix)
	}

	if ipnsVal, ipnsValFound := nsval["ipns"]; !ipnsValFound {
		return fmt.Errorf("协议前缀 %s 必须支持 /ipns 命名空间验证器", DefaultPrefix)
	} else if _, ok := ipnsVal.(ipns.Validator); !ok {
		return fmt.Errorf("协议前缀 %s 必须对 /ipns 命名空间使用 ipns.Validator", DefaultPrefix)
	}
	return nil
}
