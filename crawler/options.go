package crawler

import (
	"time"

	"github.com/dep2p/kaddht/amino"
	"github.com/dep2p/libp2p/core/protocol"
)

// Option DHT爬虫选项类型
type Option func(*options) error

type options struct {
	protocols            []protocol.ID
	parallelism          int
	connectTimeout       time.Duration
	perMsgTimeout        time.Duration
	dialAddressExtendDur time.Duration
}

// defaults 默认的爬虫选项。此选项将自动添加到传递给爬虫构造函数的任何选项之前。
// 参数:
//   - o: *options 选项指针
//
// 返回值:
//   - error 错误信息
var defaults = func(o *options) error {
	o.protocols = amino.Protocols
	o.parallelism = 1000
	o.connectTimeout = time.Second * 5
	o.perMsgTimeout = time.Second * 5
	o.dialAddressExtendDur = time.Minute * 30

	return nil
}

// WithProtocols 定义爬虫用于与其他节点通信的有序协议集
// 参数:
//   - protocols: []protocol.ID 协议ID列表
//
// 返回值:
//   - Option 选项函数
func WithProtocols(protocols []protocol.ID) Option {
	return func(o *options) error {
		o.protocols = append([]protocol.ID{}, protocols...)
		return nil
	}
}

// WithParallelism 定义可以并行发出的查询数量
// 参数:
//   - parallelism: int 并行度
//
// 返回值:
//   - Option 选项函数
func WithParallelism(parallelism int) Option {
	return func(o *options) error {
		o.parallelism = parallelism
		return nil
	}
}

// WithMsgTimeout 定义单个DHT消息在被视为失败之前允许花费的时间
// 参数:
//   - timeout: time.Duration 超时时间
//
// 返回值:
//   - Option 选项函数
func WithMsgTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.perMsgTimeout = timeout
		return nil
	}
}

// WithConnectTimeout 定义对等连接超时的时间
// 参数:
//   - timeout: time.Duration 超时时间
//
// 返回值:
//   - Option 选项函数
func WithConnectTimeout(timeout time.Duration) Option {
	return func(o *options) error {
		o.connectTimeout = timeout
		return nil
	}
}

// WithDialAddrExtendDuration 设置对等存储中已拨号地址的TTL延长时间。
// 如果未设置，默认为30分钟。
// 参数:
//   - ext: time.Duration 延长时间
//
// 返回值:
//   - Option 选项函数
func WithDialAddrExtendDuration(ext time.Duration) Option {
	return func(o *options) error {
		o.dialAddressExtendDur = ext
		return nil
	}
}
