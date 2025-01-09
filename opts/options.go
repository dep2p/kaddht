// 已弃用: 选项现在定义在根包中。

package dhtopts

import (
	"time"

	dht "github.com/dep2p/kaddht"
	record "github.com/dep2p/kaddht/record"
	ds "github.com/ipfs/go-datastore"
)

type Option = dht.Option

// RoutingTableLatencyTolerance 设置路由表延迟容忍度
// 参数:
//   - latency: time.Duration 延迟时间
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.RoutingTableLatencyTolerance
func RoutingTableLatencyTolerance(latency time.Duration) dht.Option {
	return dht.RoutingTableLatencyTolerance(latency)
}

// RoutingTableRefreshQueryTimeout 设置路由表刷新查询超时时间
// 参数:
//   - timeout: time.Duration 超时时间
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.RoutingTableRefreshQueryTimeout
func RoutingTableRefreshQueryTimeout(timeout time.Duration) dht.Option {
	return dht.RoutingTableRefreshQueryTimeout(timeout)
}

// RoutingTableRefreshPeriod 设置路由表刷新周期
// 参数:
//   - period: time.Duration 刷新周期
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.RoutingTableRefreshPeriod
func RoutingTableRefreshPeriod(period time.Duration) dht.Option {
	return dht.RoutingTableRefreshPeriod(period)
}

// Datastore 设置数据存储
// 参数:
//   - ds: ds.Batching 数据存储接口
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.Datastore
func Datastore(ds ds.Batching) dht.Option { return dht.Datastore(ds) }

// Client 配置DHT是否以仅客户端模式运行
// 参数:
//   - only: bool 是否仅客户端模式
//
// 返回值:
//   - dht.Option DHT选项
//
// 默认为false(即ModeAuto)
// 已弃用: 使用 dht.Mode(ModeClient)
func Client(only bool) dht.Option {
	if only {
		return dht.Mode(dht.ModeClient)
	}
	return dht.Mode(dht.ModeAuto)
}

// Mode 设置DHT运行模式
// 参数:
//   - m: dht.ModeOpt 运行模式
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.Mode
func Mode(m dht.ModeOpt) dht.Option { return dht.Mode(m) }

// Validator 设置记录验证器
// 参数:
//   - v: record.Validator 记录验证器
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.Validator
func Validator(v record.Validator) dht.Option { return dht.Validator(v) }

// NamespacedValidator 设置命名空间记录验证器
// 参数:
//   - ns: string 命名空间
//   - v: record.Validator 记录验证器
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.NamespacedValidator
func NamespacedValidator(ns string, v record.Validator) dht.Option {
	return dht.NamespacedValidator(ns, v)
}

// BucketSize 设置桶大小
// 参数:
//   - bucketSize: int 桶大小
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.BucketSize
func BucketSize(bucketSize int) dht.Option { return dht.BucketSize(bucketSize) }

// MaxRecordAge 设置记录最大存活时间
// 参数:
//   - maxAge: time.Duration 最大存活时间
//
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.MaxRecordAge
func MaxRecordAge(maxAge time.Duration) dht.Option { return dht.MaxRecordAge(maxAge) }

// DisableAutoRefresh 禁用自动刷新
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.DisableAutoRefresh
func DisableAutoRefresh() dht.Option { return dht.DisableAutoRefresh() }

// DisableProviders 禁用提供者功能
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.DisableProviders
func DisableProviders() dht.Option { return dht.DisableProviders() }

// DisableValues 禁用值存储功能
// 返回值:
//   - dht.Option DHT选项
//
// 已弃用: 使用 dht.DisableValues
func DisableValues() dht.Option { return dht.DisableValues() }
