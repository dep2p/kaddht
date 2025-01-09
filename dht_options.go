package dht

import (
	"fmt"
	"testing"
	"time"

	"github.com/dep2p/kaddht/amino"
	dhtcfg "github.com/dep2p/kaddht/internal/config"
	"github.com/dep2p/kaddht/kbucket/peerdiversity"
	pb "github.com/dep2p/kaddht/pb"
	"github.com/dep2p/kaddht/providers"
	record "github.com/dep2p/kaddht/record"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/protocol"

	ds "github.com/ipfs/go-datastore"
	ma "github.com/multiformats/go-multiaddr"
)

// ModeOpt 描述 DHT 应该以什么模式运行
type ModeOpt = dhtcfg.ModeOpt

const (
	// ModeAuto 利用事件总线上发送的 EvtLocalReachabilityChanged 事件,根据网络条件动态地在客户端和服务器模式之间切换 DHT
	ModeAuto ModeOpt = iota
	// ModeClient 将 DHT 作为客户端运行,它不能响应传入的查询
	ModeClient
	// ModeServer 将 DHT 作为服务器运行,它既可以发送查询也可以响应查询
	ModeServer
	// ModeAutoServer 与 ModeAuto 的运行方式相同,但在可达性未知时作为服务器运行
	ModeAutoServer
)

// DefaultPrefix 是默认附加到所有 DHT 协议的应用程序特定前缀
const DefaultPrefix protocol.ID = amino.ProtocolPrefix

type Option = dhtcfg.Option

// ProviderStore 设置提供者存储管理器
// 参数:
//   - ps: providers.ProviderStore 提供者存储
//
// 返回值:
//   - Option 配置选项
func ProviderStore(ps providers.ProviderStore) Option {
	return func(c *dhtcfg.Config) error {
		c.ProviderStore = ps
		return nil
	}
}

// RoutingTableLatencyTolerance 设置路由表集群中对等点的最大可接受延迟
// 参数:
//   - latency: time.Duration 延迟时间
//
// 返回值:
//   - Option 配置选项
func RoutingTableLatencyTolerance(latency time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.LatencyTolerance = latency
		return nil
	}
}

// RoutingTableRefreshQueryTimeout 设置路由表刷新查询的超时时间
// 参数:
//   - timeout: time.Duration 超时时间
//
// 返回值:
//   - Option 配置选项
func RoutingTableRefreshQueryTimeout(timeout time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.RefreshQueryTimeout = timeout
		return nil
	}
}

// RoutingTableRefreshPeriod 设置刷新路由表中桶的周期。DHT 将通过以下方式每个周期刷新桶:
// 1. 首先搜索附近的对等点以确定我们应该尝试填充多少个桶
// 2. 然后在上次刷新期间未查询的每个桶中搜索随机键
//
// 参数:
//   - period: time.Duration 刷新周期
//
// 返回值:
//   - Option 配置选项
func RoutingTableRefreshPeriod(period time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.RefreshInterval = period
		return nil
	}
}

// Datastore 配置 DHT 使用指定的数据存储
// 默认使用内存中的(临时)映射
//
// 参数:
//   - ds: ds.Batching 数据存储
//
// 返回值:
//   - Option 配置选项
func Datastore(ds ds.Batching) Option {
	return func(c *dhtcfg.Config) error {
		c.Datastore = ds
		return nil
	}
}

// Mode 配置 DHT 运行的模式(Client、Server、Auto)
// 默认为 ModeAuto
//
// 参数:
//   - m: ModeOpt 运行模式
//
// 返回值:
//   - Option 配置选项
func Mode(m ModeOpt) Option {
	return func(c *dhtcfg.Config) error {
		c.Mode = m
		return nil
	}
}

// Validator 配置 DHT 使用指定的验证器
// 默认使用可以验证公钥(在"pk"命名空间下)和 IPNS 记录(在"ipns"命名空间下)的命名空间验证器
// 设置验证器意味着用户想要控制验证器,因此不会添加默认的公钥和 IPNS 验证器
//
// 参数:
//   - v: record.Validator 验证器
//
// 返回值:
//   - Option 配置选项
func Validator(v record.Validator) Option {
	return func(c *dhtcfg.Config) error {
		c.Validator = v
		c.ValidatorChanged = true
		return nil
	}
}

// NamespacedValidator 在命名空间 ns 下添加验证器
// 如果 DHT 没有使用 record.NamespacedValidator 作为其验证器,此选项将失败(默认使用一个,但可以使用 Validator 选项覆盖)
// 在不更改 Validator 的情况下添加命名空间验证器将导致在默认公钥和 IPNS 验证器之外添加新的验证器
// 除非先设置了新的 Validator,否则不能在此处覆盖"pk"和"ipns"命名空间
//
// 示例: 给定一个注册为 NamespacedValidator("ipns", myValidator) 的验证器,所有以 /ipns/ 开头的键的记录都将使用 myValidator 进行验证
//
// 参数:
//   - ns: string 命名空间
//   - v: record.Validator 验证器
//
// 返回值:
//   - Option 配置选项
func NamespacedValidator(ns string, v record.Validator) Option {
	return func(c *dhtcfg.Config) error {
		nsval, ok := c.Validator.(record.NamespacedValidator)
		if !ok {
			return fmt.Errorf("只能向 NamespacedValidator 添加命名空间验证器")
		}
		nsval[ns] = v
		return nil
	}
}

// ProtocolPrefix 设置要附加到所有 DHT 协议的应用程序特定前缀
// 例如,使用 /myapp/kad/1.0.0 而不是 /ipfs/kad/1.0.0。前缀应采用 /myapp 的形式
// 默认为 amino.ProtocolPrefix
//
// 参数:
//   - prefix: protocol.ID 协议前缀
//
// 返回值:
//   - Option 配置选项
func ProtocolPrefix(prefix protocol.ID) Option {
	return func(c *dhtcfg.Config) error {
		c.ProtocolPrefix = prefix
		return nil
	}
}

// ProtocolExtension 向 DHT 协议添加应用程序特定协议
// 例如,使用 /ipfs/lan/kad/1.0.0 而不是 /ipfs/kad/1.0.0。扩展应采用 /lan 的形式
//
// 参数:
//   - ext: protocol.ID 协议扩展
//
// 返回值:
//   - Option 配置选项
func ProtocolExtension(ext protocol.ID) Option {
	return func(c *dhtcfg.Config) error {
		c.ProtocolPrefix += ext
		return nil
	}
}

// V1ProtocolOverride 用另一个协议覆盖 /kad/1.0.0 使用的 protocolID
// 这是一个高级功能,只应用于处理尚未使用 /app/kad/1.0.0 形式的 protocolID 的遗留网络
// 此选项将覆盖并忽略 ProtocolPrefix 和 ProtocolExtension 选项
//
// 参数:
//   - proto: protocol.ID 协议ID
//
// 返回值:
//   - Option 配置选项
func V1ProtocolOverride(proto protocol.ID) Option {
	return func(c *dhtcfg.Config) error {
		c.V1ProtocolOverride = proto
		return nil
	}
}

// BucketSize 配置路由表的桶大小(Kademlia 论文中的 k)
// 默认值为 amino.DefaultBucketSize
//
// 参数:
//   - bucketSize: int 桶大小
//
// 返回值:
//   - Option 配置选项
func BucketSize(bucketSize int) Option {
	return func(c *dhtcfg.Config) error {
		c.BucketSize = bucketSize
		return nil
	}
}

// Concurrency 配置给定查询路径的并发请求数(Kademlia 论文中的 alpha)
// 默认值为 amino.DefaultConcurrency
//
// 参数:
//   - alpha: int 并发数
//
// 返回值:
//   - Option 配置选项
func Concurrency(alpha int) Option {
	return func(c *dhtcfg.Config) error {
		c.Concurrency = alpha
		return nil
	}
}

// Resiliency 配置给定查询路径完成所需的最接近目标的对等点数量
// 默认值为 amino.DefaultResiliency
//
// 参数:
//   - beta: int 弹性值
//
// 返回值:
//   - Option 配置选项
func Resiliency(beta int) Option {
	return func(c *dhtcfg.Config) error {
		c.Resiliency = beta
		return nil
	}
}

// LookupCheckConcurrency 配置在将新节点添加到路由表之前,可用于执行查找检查操作的最大 goroutine 数量
//
// 参数:
//   - n: int goroutine 数量
//
// 返回值:
//   - Option 配置选项
func LookupCheckConcurrency(n int) Option {
	return func(c *dhtcfg.Config) error {
		c.LookupCheckConcurrency = n
		return nil
	}
}

// MaxRecordAge 指定任何节点从接收记录("PutValue record")时起保留记录的最长时间
// 这不适用于记录可能包含的任何其他形式的有效性
// 例如,记录可能包含一个 EOL 为 2020 年(未来的一个很好的时间)的 ipns 条目
// 要使该记录继续存在,必须以比每个"MaxRecordAge"更频繁的频率重新广播
//
// 参数:
//   - maxAge: time.Duration 最大记录年龄
//
// 返回值:
//   - Option 配置选项
func MaxRecordAge(maxAge time.Duration) Option {
	return func(c *dhtcfg.Config) error {
		c.MaxRecordAge = maxAge
		return nil
	}
}

// DisableAutoRefresh 完全禁用 DHT 路由表上的"自动刷新"
// 这意味着我们既不会定期刷新路由表,也不会在路由表大小低于最小阈值时刷新
//
// 返回值:
//   - Option 配置选项
func DisableAutoRefresh() Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.AutoRefresh = false
		return nil
	}
}

// DisableProviders 禁用存储和检索提供者记录
// 默认启用
// 警告: 除非您使用分叉的 DHT(即私有网络和/或使用 Protocols 选项的不同 DHT 协议),否则不要更改此设置
//
// 返回值:
//   - Option 配置选项
func DisableProviders() Option {
	return func(c *dhtcfg.Config) error {
		c.EnableProviders = false
		return nil
	}
}

// DisableValues 禁用存储和检索值记录(包括公钥)
// 默认启用
// 警告: 除非您使用分叉的 DHT(即私有网络和/或使用 Protocols 选项的不同 DHT 协议),否则不要更改此设置
//
// 返回值:
//   - Option 配置选项
func DisableValues() Option {
	return func(c *dhtcfg.Config) error {
		c.EnableValues = false
		return nil
	}
}

// QueryFilter 设置一个函数,该函数批准在查询中可以拨号的对等点
//
// 参数:
//   - filter: QueryFilterFunc 查询过滤函数
//
// 返回值:
//   - Option 配置选项
func QueryFilter(filter QueryFilterFunc) Option {
	return func(c *dhtcfg.Config) error {
		c.QueryPeerFilter = filter
		return nil
	}
}

// RoutingTableFilter 设置一个函数,该函数批准哪些对等点可以添加到路由表中
// 主机应该已经至少与正在考虑的对等点建立了一个连接
//
// 参数:
//   - filter: RouteTableFilterFunc 路由表过滤函数
//
// 返回值:
//   - Option 配置选项
func RoutingTableFilter(filter RouteTableFilterFunc) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.PeerFilter = filter
		return nil
	}
}

// BootstrapPeers 配置引导节点,如果路由表变空,我们将连接这些节点以播种和刷新路由表
//
// 参数:
//   - bootstrappers: ...peer.AddrInfo 引导节点信息
//
// 返回值:
//   - Option 配置选项
func BootstrapPeers(bootstrappers ...peer.AddrInfo) Option {
	return func(c *dhtcfg.Config) error {
		c.BootstrapPeers = func() []peer.AddrInfo {
			return bootstrappers
		}
		return nil
	}
}

// BootstrapPeersFunc 配置返回引导节点的函数,如果路由表变空,我们将连接这些节点以播种和刷新路由表
//
// 参数:
//   - getBootstrapPeers: func() []peer.AddrInfo 获取引导节点的函数
//
// 返回值:
//   - Option 配置选项
func BootstrapPeersFunc(getBootstrapPeers func() []peer.AddrInfo) Option {
	return func(c *dhtcfg.Config) error {
		c.BootstrapPeers = getBootstrapPeers
		return nil
	}
}

// RoutingTablePeerDiversityFilter 配置用于构建路由表多样性过滤器的 PeerIPGroupFilter 实现
// 请参阅 peerdiversity.PeerIPGroupFilter 和 peerdiversity.Filter 的文档了解更多详情
//
// 参数:
//   - pg: peerdiversity.PeerIPGroupFilter 对等点IP组过滤器
//
// 返回值:
//   - Option 配置选项
func RoutingTablePeerDiversityFilter(pg peerdiversity.PeerIPGroupFilter) Option {
	return func(c *dhtcfg.Config) error {
		c.RoutingTable.DiversityFilter = pg
		return nil
	}
}

// disableFixLowPeersRoutine 禁用 DHT 中的"fixLowPeers"例程
// 仅用于测试
//
// 参数:
//   - t: *testing.T 测试对象
//
// 返回值:
//   - Option 配置选项
func disableFixLowPeersRoutine(t *testing.T) Option {
	return func(c *dhtcfg.Config) error {
		c.DisableFixLowPeers = true
		return nil
	}
}

// forceAddressUpdateProcessing 强制 DHT 处理主机地址的更改
// 即使禁用了 AutoRefresh,这也会发生
// 仅用于测试
//
// 参数:
//   - t: *testing.T 测试对象
//
// 返回值:
//   - Option 配置选项
func forceAddressUpdateProcessing(t *testing.T) Option {
	return func(c *dhtcfg.Config) error {
		c.TestAddressUpdateProcessing = true
		return nil
	}
}

// EnableOptimisticProvide 启用跳过提供过程最后跳数的优化
// 这通过使用网络大小估算器(使用查询的键空间密度)在最有可能找到最后一跳时乐观地发送 ADD_PROVIDER 请求来工作
// 它还将在返回后在后台异步运行一些 ADD_PROVIDER 请求
// 如果某些阈值数量的 RPC 已成功,这允许乐观地更早返回
// 后台/正在进行的查询数量可以通过 OptimisticProvideJobsPoolSize 选项配置
//
// 实验性: 这是一个实验性选项,可能在将来被删除。使用风险自负
//
// 返回值:
//   - Option 配置选项
func EnableOptimisticProvide() Option {
	return func(c *dhtcfg.Config) error {
		c.EnableOptimisticProvide = true
		return nil
	}
}

// OptimisticProvideJobsPoolSize 允许配置正在进行的 ADD_PROVIDER RPC 的异步性限制
// 将其设置为 optProvReturnRatio * BucketSize 的倍数是有意义的。查看 EnableOptimisticProvide 的描述了解更多详情
//
// 实验性: 这是一个实验性选项,可能在将来被删除。使用风险自负
//
// 参数:
//   - size: int 池大小
//
// 返回值:
//   - Option 配置选项
func OptimisticProvideJobsPoolSize(size int) Option {
	return func(c *dhtcfg.Config) error {
		c.OptimisticProvideJobsPoolSize = size
		return nil
	}
}

// AddressFilter 允许配置地址过滤函数
// 在将地址添加到 peerstore 之前运行此函数
// 它最适合用于避免添加 localhost / 本地地址
//
// 参数:
//   - f: func([]ma.Multiaddr) []ma.Multiaddr 地址过滤函数
//
// 返回值:
//   - Option 配置选项
func AddressFilter(f func([]ma.Multiaddr) []ma.Multiaddr) Option {
	return func(c *dhtcfg.Config) error {
		c.AddressFilter = f
		return nil
	}
}

// WithCustomMessageSender 配置 IpfsDHT 的 pb.MessageSender 使用 pb.MessageSender 的自定义实现
//
// 参数:
//   - messageSenderBuilder: func(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect 消息发送器构建函数
//
// 返回值:
//   - Option 配置选项
func WithCustomMessageSender(messageSenderBuilder func(h host.Host, protos []protocol.ID) pb.MessageSenderWithDisconnect) Option {
	return func(c *dhtcfg.Config) error {
		c.MsgSenderBuilder = messageSenderBuilder
		return nil
	}
}
