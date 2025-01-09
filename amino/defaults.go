// amino 包提供了 [Amino DHT] 的协议参数和建议默认值。
//
// [Amino DHT] 是 Kademlia 分布式哈希表(DHT)算法的一个实现,最初设计用于 IPFS(星际文件系统)网络。
// 本包定义了 Amino DHT 实现中使用的关键常量和协议标识符。
//
// [Amino DHT]: https://probelab.io/ipfs/amino/
package amino

import (
	"time"

	"github.com/dep2p/libp2p/core/protocol"
)

const (
	// ProtocolPrefix 是 Amono DHT 协议的基础前缀。
	ProtocolPrefix protocol.ID = "/ipfs"

	// ProtocolID 是 Amino DHT 的最新协议标识符。
	ProtocolID protocol.ID = "/ipfs/kad/1.0.0"

	// DefaultBucketSize 是 Amino DHT 的桶大小(Kademlia 论文中的 k 值)。
	// 它表示路由表中每个 k-bucket 可以存储的最大对等节点数量。
	DefaultBucketSize = 20

	// DefaultConcurrency 是 Amino DHT 中给定查询路径的建议并发请求数量(Kademlia 论文中的 alpha 值)。
	// 它决定了网络遍历过程中执行的并行查找数量。
	DefaultConcurrency = 10

	// DefaultResiliency 是 Amino DHT 中给定查询路径完成所需的最接近目标的对等节点响应数量。
	// 这通过要求多个确认来帮助确保可靠的结果。
	DefaultResiliency = 3

	// DefaultProvideValidity 是 Amino DHT 中提供者记录在需要刷新或删除之前应持续的默认时间。
	// 该值也称为提供者记录过期间隔。
	DefaultProvideValidity = 48 * time.Hour

	// DefaultProviderAddrTTL 是保留提供者对等节点多地址的 TTL。这些地址与提供者一起返回。
	// 过期后，返回的记录将需要额外的查找，以找到与返回的对等节点 ID 关联的多地址。
	DefaultProviderAddrTTL = 24 * time.Hour
)

var (
	// Protocols 是包含 Amino DHT 所有支持的协议 ID 的切片。
	// 目前它只包含主要的 ProtocolID，但定义为切片是为了允许未来可能的协议版本或变体。
	Protocols = []protocol.ID{ProtocolID}
)
