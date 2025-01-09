package fullrt

import (
	"fmt"
	"time"

	kaddht "github.com/dep2p/kaddht"
	"github.com/dep2p/kaddht/crawler"
	"github.com/dep2p/kaddht/providers"
)

// config DHT配置
type config struct {
	dhtOpts []kaddht.Option

	crawlInterval       time.Duration
	waitFrac            float64
	bulkSendParallelism int
	timeoutPerOp        time.Duration
	crawler             crawler.Crawler
	pmOpts              []providers.Option
}

// apply 应用配置选项
// 参数:
//   - opts: ...Option 配置选项列表
//
// 返回值:
//   - error 错误信息
func (cfg *config) apply(opts ...Option) error {
	for i, o := range opts {
		if err := o(cfg); err != nil {
			return fmt.Errorf("DHT配置选项 %d 失败: %w", i, err)
		}
	}
	return nil
}

// Option 配置选项函数类型
type Option func(opt *config) error

// DHTOption 设置DHT选项
// 参数:
//   - opts: ...kaddht.Option DHT选项列表
//
// 返回值:
//   - Option 配置选项
func DHTOption(opts ...kaddht.Option) Option {
	return func(c *config) error {
		c.dhtOpts = append(c.dhtOpts, opts...)
		return nil
	}
}

// WithCrawler 设置用于爬取DHT网络的爬虫
// 如果未指定,默认使用parallelism为200的crawler.DefaultCrawler
// 参数:
//   - c: crawler.Crawler 爬虫实例
//
// 返回值:
//   - Option 配置选项
func WithCrawler(c crawler.Crawler) Option {
	return func(opt *config) error {
		opt.crawler = c
		return nil
	}
}

// WithCrawlInterval 设置爬取DHT以刷新对等节点存储的时间间隔
// 如果未指定,默认为1小时
// 参数:
//   - i: time.Duration 时间间隔
//
// 返回值:
//   - Option 配置选项
func WithCrawlInterval(i time.Duration) Option {
	return func(opt *config) error {
		opt.crawlInterval = i
		return nil
	}
}

// WithSuccessWaitFraction 设置将操作视为成功前需要等待的对等节点比例,取值范围(0,1]
// 如果未指定,默认为30%
// 参数:
//   - f: float64 等待比例
//
// 返回值:
//   - Option 配置选项
func WithSuccessWaitFraction(f float64) Option {
	return func(opt *config) error {
		if f <= 0 || f > 1 {
			return fmt.Errorf("等待比例必须大于0且小于等于1; 当前值: %f", f)
		}
		opt.waitFrac = f
		return nil
	}
}

// WithBulkSendParallelism 设置向其他对等节点发送消息的最大并行度,必须大于等于1
// 如果未指定,默认为20
// 参数:
//   - b: int 并行度
//
// 返回值:
//   - Option 配置选项
func WithBulkSendParallelism(b int) Option {
	return func(opt *config) error {
		if b < 1 {
			return fmt.Errorf("批量发送并行度必须大于等于1; 当前值: %d", b)
		}
		opt.bulkSendParallelism = b
		return nil
	}
}

// WithTimeoutPerOperation 设置每个操作的超时时间,包括设置提供者和查询DHT
// 如果未指定,默认为5秒
// 参数:
//   - t: time.Duration 超时时间
//
// 返回值:
//   - Option 配置选项
func WithTimeoutPerOperation(t time.Duration) Option {
	return func(opt *config) error {
		opt.timeoutPerOp = t
		return nil
	}
}

// WithProviderManagerOptions 设置实例化providers.ProviderManager时使用的选项
// 参数:
//   - pmOpts: ...providers.Option 提供者管理器选项列表
//
// 返回值:
//   - Option 配置选项
func WithProviderManagerOptions(pmOpts ...providers.Option) Option {
	return func(opt *config) error {
		opt.pmOpts = pmOpts
		return nil
	}
}
