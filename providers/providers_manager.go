package providers

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/dep2p/kaddht/amino"
	"github.com/dep2p/kaddht/internal"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/peerstore"
	peerstoreImpl "github.com/dep2p/libp2p/p2p/host/peerstore"
	lru "github.com/hashicorp/golang-lru/simplelru"
	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/autobatch"
	dsq "github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-base32"
)

const (
	// ProvidersKeyPrefix 是存储在数据存储中所有提供者记录键的前缀/命名空间
	ProvidersKeyPrefix = "/providers/"

	// ProviderAddrTTL 是保留提供者节点多地址的TTL。这些地址会与提供者一起返回。
	// 过期后,返回的记录将需要额外查找,以找到与返回的节点ID关联的多地址。
	ProviderAddrTTL = amino.DefaultProviderAddrTTL
)

// ProvideValidity 是提供者记录在DHT上应该持续的默认时间
// 这个值也被称为提供者记录过期间隔
var ProvideValidity = amino.DefaultProvideValidity
var defaultCleanupInterval = time.Hour
var lruCacheSize = 256
var batchBufferSize = 256
var log = logging.Logger("providers")

// ProviderStore 表示一个将节点及其地址与键关联的存储
type ProviderStore interface {
	AddProvider(ctx context.Context, key []byte, prov peer.AddrInfo) error
	GetProviders(ctx context.Context, key []byte) ([]peer.AddrInfo, error)
	io.Closer
}

// ProviderManager 从数据存储中添加和获取提供者,并在中间进行缓存
type ProviderManager struct {
	self peer.ID
	// 所有非通道字段只能在run方法中访问
	cache  lru.LRUCache
	pstore peerstore.Peerstore
	dstore *autobatch.Datastore

	newprovs chan *addProv
	getprovs chan *getProv

	cleanupInterval time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

var _ ProviderStore = (*ProviderManager)(nil)

// Option 是设置提供者管理器选项的函数
type Option func(*ProviderManager) error

// applyOptions 应用提供者管理器选项
// 参数:
//   - opts: []Option 选项列表
//
// 返回值:
//   - error 错误信息
func (pm *ProviderManager) applyOptions(opts ...Option) error {
	for i, opt := range opts {
		if err := opt(pm); err != nil {
			return fmt.Errorf("提供者管理器选项 %d 失败: %s", i, err)
		}
	}
	return nil
}

// CleanupInterval 设置GC运行之间的时间间隔
// 默认为1小时
func CleanupInterval(d time.Duration) Option {
	return func(pm *ProviderManager) error {
		pm.cleanupInterval = d
		return nil
	}
}

// Cache 设置LRU缓存实现
// 默认为简单的LRU缓存
func Cache(c lru.LRUCache) Option {
	return func(pm *ProviderManager) error {
		pm.cache = c
		return nil
	}
}

type addProv struct {
	ctx context.Context
	key []byte
	val peer.ID
}

type getProv struct {
	ctx  context.Context
	key  []byte
	resp chan []peer.ID
}

// NewProviderManager 构造函数
// 参数:
//   - local: peer.ID 本地节点ID
//   - ps: peerstore.Peerstore 节点存储
//   - dstore: ds.Batching 数据存储
//   - opts: []Option 选项列表
//
// 返回值:
//   - *ProviderManager 提供者管理器实例
//   - error 错误信息
func NewProviderManager(local peer.ID, ps peerstore.Peerstore, dstore ds.Batching, opts ...Option) (*ProviderManager, error) {
	pm := new(ProviderManager)
	pm.self = local
	pm.getprovs = make(chan *getProv)
	pm.newprovs = make(chan *addProv)
	pm.pstore = ps
	pm.dstore = autobatch.NewAutoBatching(dstore, batchBufferSize)
	cache, err := lru.NewLRU(lruCacheSize, nil)
	if err != nil {
		return nil, err
	}
	pm.cache = cache
	pm.cleanupInterval = defaultCleanupInterval
	if err := pm.applyOptions(opts...); err != nil {
		return nil, err
	}
	pm.ctx, pm.cancel = context.WithCancel(context.Background())
	pm.run()
	return pm, nil
}

// run 运行提供者管理器的主循环
func (pm *ProviderManager) run() {
	pm.wg.Add(1)
	go func() {
		defer pm.wg.Done()

		var gcQuery dsq.Results
		gcTimer := time.NewTimer(pm.cleanupInterval)

		defer func() {
			gcTimer.Stop()
			if gcQuery != nil {
				// 不太关心这个失败
				_ = gcQuery.Close()
			}
			if err := pm.dstore.Flush(context.Background()); err != nil {
				log.Error("刷新数据存储失败: ", err)
			}
		}()

		var gcQueryRes <-chan dsq.Result
		var gcSkip map[string]struct{}
		var gcTime time.Time
		for {
			select {
			case np := <-pm.newprovs:
				err := pm.addProv(np.ctx, np.key, np.val)
				if err != nil {
					log.Error("添加新提供者错误: ", err)
					continue
				}
				if gcSkip != nil {
					// 我们有一个gc,告诉它跳过这个提供者,因为我们在GC开始后更新了它
					gcSkip[mkProvKeyFor(np.key, np.val)] = struct{}{}
				}
			case gp := <-pm.getprovs:
				provs, err := pm.getProvidersForKey(gp.ctx, gp.key)
				if err != nil && err != ds.ErrNotFound {
					log.Error("读取提供者错误: ", err)
				}

				// 设置容量,这样用户就不能追加了
				gp.resp <- provs[0:len(provs):len(provs)]
			case res, ok := <-gcQueryRes:
				if !ok {
					if err := gcQuery.Close(); err != nil {
						log.Error("关闭提供者GC查询失败: ", err)
					}
					gcTimer.Reset(pm.cleanupInterval)

					// 清理GC轮次
					gcQueryRes = nil
					gcSkip = nil
					gcQuery = nil
					continue
				}
				if res.Error != nil {
					log.Error("从GC查询得到错误: ", res.Error)
					continue
				}
				if _, ok := gcSkip[res.Key]; ok {
					// 自从开始GC轮次以来我们已经更新了这个记录,跳过它
					continue
				}

				// 检查过期时间
				t, err := readTimeValue(res.Value)
				switch {
				case err != nil:
					// 无法解析时间
					log.Error("从磁盘解析提供者记录: ", err)
					fallthrough
				case gcTime.Sub(t) > ProvideValidity:
					// 或已过期
					err = pm.dstore.Delete(pm.ctx, ds.RawKey(res.Key))
					if err != nil && err != ds.ErrNotFound {
						log.Error("从磁盘删除提供者记录失败: ", err)
					}
				}

			case gcTime = <-gcTimer.C:
				// 你知道缓存最棒的地方是什么吗?你可以丢弃它们
				//
				// 比GC快多了
				pm.cache.Purge()

				// 现在,开始对数据存储进行GC
				q, err := pm.dstore.Query(pm.ctx, dsq.Query{
					Prefix: ProvidersKeyPrefix,
				})
				if err != nil {
					log.Error("提供者记录GC查询失败: ", err)
					continue
				}
				gcQuery = q
				gcQueryRes = q.Next()
				gcSkip = make(map[string]struct{})
			case <-pm.ctx.Done():
				return
			}
		}
	}()
}

// Close 关闭提供者管理器
// 返回值:
//   - error 错误信息
func (pm *ProviderManager) Close() error {
	pm.cancel()
	pm.wg.Wait()
	return nil
}

// AddProvider 添加一个提供者
// 参数:
//   - ctx: context.Context 上下文
//   - k: []byte 键
//   - provInfo: peer.AddrInfo 提供者信息
//
// 返回值:
//   - error 错误信息
func (pm *ProviderManager) AddProvider(ctx context.Context, k []byte, provInfo peer.AddrInfo) error {
	ctx, span := internal.StartSpan(ctx, "ProviderManager.AddProvider")
	defer span.End()

	if provInfo.ID != pm.self { // 不添加自己的地址
		pm.pstore.AddAddrs(provInfo.ID, provInfo.Addrs, ProviderAddrTTL)
	}
	prov := &addProv{
		ctx: ctx,
		key: k,
		val: provInfo.ID,
	}
	select {
	case pm.newprovs <- prov:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// addProv 如果需要则更新缓存
// 参数:
//   - ctx: context.Context 上下文
//   - k: []byte 键
//   - p: peer.ID 提供者ID
//
// 返回值:
//   - error 错误信息
func (pm *ProviderManager) addProv(ctx context.Context, k []byte, p peer.ID) error {
	now := time.Now()
	if provs, ok := pm.cache.Get(string(k)); ok {
		provs.(*providerSet).setVal(p, now)
	} // 否则未缓存,只写入

	return writeProviderEntry(ctx, pm.dstore, k, p, now)
}

// writeProviderEntry 将提供者写入数据存储
// 参数:
//   - ctx: context.Context 上下文
//   - dstore: ds.Datastore 数据存储
//   - k: []byte 键
//   - p: peer.ID 提供者ID
//   - t: time.Time 时间戳
//
// 返回值:
//   - error 错误信息
func writeProviderEntry(ctx context.Context, dstore ds.Datastore, k []byte, p peer.ID, t time.Time) error {
	dsk := mkProvKeyFor(k, p)

	buf := make([]byte, 16)
	n := binary.PutVarint(buf, t.UnixNano())

	return dstore.Put(ctx, ds.NewKey(dsk), buf[:n])
}

// mkProvKeyFor 为键和提供者ID创建提供者键
// 参数:
//   - k: []byte 键
//   - p: peer.ID 提供者ID
//
// 返回值:
//   - string 提供者键
func mkProvKeyFor(k []byte, p peer.ID) string {
	return mkProvKey(k) + "/" + base32.RawStdEncoding.EncodeToString([]byte(p))
}

// mkProvKey 为键创建提供者键
// 参数:
//   - k: []byte 键
//
// 返回值:
//   - string 提供者键
func mkProvKey(k []byte) string {
	return ProvidersKeyPrefix + base32.RawStdEncoding.EncodeToString(k)
}

// GetProviders 返回给定键的提供者集合
// 此方法不会复制集合。不要修改它。
// 参数:
//   - ctx: context.Context 上下文
//   - k: []byte 键
//
// 返回值:
//   - []peer.AddrInfo 提供者信息列表
//   - error 错误信息
func (pm *ProviderManager) GetProviders(ctx context.Context, k []byte) ([]peer.AddrInfo, error) {
	ctx, span := internal.StartSpan(ctx, "ProviderManager.GetProviders")
	defer span.End()

	gp := &getProv{
		ctx:  ctx,
		key:  k,
		resp: make(chan []peer.ID, 1), // 缓冲以防止发送者阻塞
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case pm.getprovs <- gp:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case peers := <-gp.resp:
		return peerstoreImpl.PeerInfos(pm.pstore, peers), nil
	}
}

// getProvidersForKey 获取键的提供者列表
// 参数:
//   - ctx: context.Context 上下文
//   - k: []byte 键
//
// 返回值:
//   - []peer.ID 提供者ID列表
//   - error 错误信息
func (pm *ProviderManager) getProvidersForKey(ctx context.Context, k []byte) ([]peer.ID, error) {
	pset, err := pm.getProviderSetForKey(ctx, k)
	if err != nil {
		return nil, err
	}
	return pset.providers, nil
}

// getProviderSetForKey 如果缓存中已存在则返回ProviderSet,否则从数据存储加载
// 参数:
//   - ctx: context.Context 上下文
//   - k: []byte 键
//
// 返回值:
//   - *providerSet 提供者集合
//   - error 错误信息
func (pm *ProviderManager) getProviderSetForKey(ctx context.Context, k []byte) (*providerSet, error) {
	cached, ok := pm.cache.Get(string(k))
	if ok {
		return cached.(*providerSet), nil
	}

	pset, err := loadProviderSet(ctx, pm.dstore, k)
	if err != nil {
		return nil, err
	}

	if len(pset.providers) > 0 {
		pm.cache.Add(string(k), pset)
	}

	return pset, nil
}

// loadProviderSet 从数据存储加载ProviderSet
// 参数:
//   - ctx: context.Context 上下文
//   - dstore: ds.Datastore 数据存储
//   - k: []byte 键
//
// 返回值:
//   - *providerSet 提供者集合
//   - error 错误信息
func loadProviderSet(ctx context.Context, dstore ds.Datastore, k []byte) (*providerSet, error) {
	res, err := dstore.Query(ctx, dsq.Query{Prefix: mkProvKey(k)})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	now := time.Now()
	out := newProviderSet()
	for {
		e, ok := res.NextSync()
		if !ok {
			break
		}
		if e.Error != nil {
			log.Error("获取到一个错误: ", e.Error)
			continue
		}

		// 检查过期时间
		t, err := readTimeValue(e.Value)
		switch {
		case err != nil:
			// 无法解析时间
			log.Error("从磁盘解析提供者记录: ", err)
			fallthrough
		case now.Sub(t) > ProvideValidity:
			// 或已过期
			err = dstore.Delete(ctx, ds.RawKey(e.Key))
			if err != nil && err != ds.ErrNotFound {
				log.Error("从磁盘删除提供者记录失败: ", err)
			}
			continue
		}

		lix := strings.LastIndex(e.Key, "/")

		decstr, err := base32.RawStdEncoding.DecodeString(e.Key[lix+1:])
		if err != nil {
			log.Error("base32解码错误: ", err)
			err = dstore.Delete(ctx, ds.RawKey(e.Key))
			if err != nil && err != ds.ErrNotFound {
				log.Error("从磁盘删除提供者记录失败: ", err)
			}
			continue
		}

		pid := peer.ID(decstr)

		out.setVal(pid, t)
	}

	return out, nil
}

// readTimeValue 读取时间值
// 参数:
//   - data: []byte 时间数据
//
// 返回值:
//   - time.Time 时间
//   - error 错误信息
func readTimeValue(data []byte) (time.Time, error) {
	nsec, n := binary.Varint(data)
	if n <= 0 {
		return time.Time{}, fmt.Errorf("解析时间失败")
	}

	return time.Unix(0, nsec), nil
}
