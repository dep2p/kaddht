package dht

import (
	"context"
	"time"

	"github.com/dep2p/libp2p/core/peer"

	"github.com/multiformats/go-multiaddr"
)

// DefaultBootstrapPeers 是 libp2p 提供的一组公共 DHT 引导节点
var DefaultBootstrapPeers []multiaddr.Multiaddr

// minRTRefreshThreshold 是路由表中的最小对等节点数。如果我们低于此值并看到新的对等节点,我们将触发引导轮次
var minRTRefreshThreshold = 10

const (
	// periodicBootstrapInterval 定期引导间隔时间
	periodicBootstrapInterval = 2 * time.Minute
	// maxNBoostrappers 最大引导节点数
	maxNBoostrappers = 2
)

// init 初始化默认引导节点
func init() {
	for _, s := range []string{
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ", // mars.i.ipfs.io
	} {
		ma, err := multiaddr.NewMultiaddr(s)
		if err != nil {
			panic(err)
		}
		DefaultBootstrapPeers = append(DefaultBootstrapPeers, ma)
	}
}

// GetDefaultBootstrapPeerAddrInfos 返回默认引导节点的 peer.AddrInfos,以便我们可以通过将这些传递给 BootstrapPeers(...) 选项来初始化 DHT
// 返回值:
//   - []peer.AddrInfo 引导节点地址信息列表
func GetDefaultBootstrapPeerAddrInfos() []peer.AddrInfo {
	ds := make([]peer.AddrInfo, 0, len(DefaultBootstrapPeers))

	for i := range DefaultBootstrapPeers {
		info, err := peer.AddrInfoFromP2pAddr(DefaultBootstrapPeers[i])
		if err != nil {
			logger.Errorw("转换引导节点地址到对等节点地址信息失败", "address",
				DefaultBootstrapPeers[i].String(), err, "err")
			continue
		}
		ds = append(ds, *info)
	}
	return ds
}

// Bootstrap 告诉 DHT 进入满足 IpfsRouter 接口的引导状态
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - error 错误信息
func (dht *IpfsDHT) Bootstrap(ctx context.Context) (err error) {
	_, end := tracer.Bootstrap(dhtName, ctx)
	defer func() { end(err) }()

	dht.fixRTIfNeeded()
	dht.rtRefreshManager.RefreshNoWait()
	return nil
}

// RefreshRoutingTable 告诉 DHT 刷新其路由表
//
// 返回的通道将阻塞直到刷新完成,然后产生错误并关闭。该通道是带缓冲的,可以安全地忽略
// 返回值:
//   - <-chan error 错误通道
func (dht *IpfsDHT) RefreshRoutingTable() <-chan error {
	return dht.rtRefreshManager.Refresh(false)
}

// ForceRefresh 类似于 RefreshRoutingTable,但强制 DHT 刷新路由表中的所有桶,而不考虑它们上次刷新的时间
//
// 返回的通道将阻塞直到刷新完成,然后产生错误并关闭。该通道是带缓冲的,可以安全地忽略
// 返回值:
//   - <-chan error 错误通道
func (dht *IpfsDHT) ForceRefresh() <-chan error {
	return dht.rtRefreshManager.Refresh(true)
}
