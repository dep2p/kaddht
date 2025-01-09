package dht

import (
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"

	"github.com/google/gopacket/routing"
	netroute "github.com/libp2p/go-netroute"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	dhtcfg "github.com/dep2p/kaddht/internal/config"
)

// QueryFilterFunc 是在查询时考虑要拨号的对等节点时应用的过滤器
type QueryFilterFunc = dhtcfg.QueryFilterFunc

// RouteTableFilterFunc 是在考虑要保留在本地路由表中的连接时应用的过滤器
type RouteTableFilterFunc = dhtcfg.RouteTableFilterFunc

var publicCIDR6 = "2000::/3"
var public6 *net.IPNet

func init() {
	_, public6, _ = net.ParseCIDR(publicCIDR6)
}

// isPublicAddr 遵循 manet.IsPublicAddr 的逻辑,但对于 ipv6 使用更严格的"公共"定义:即"它是否在 2000::/3 中"?
// 参数:
//   - a: ma.Multiaddr 多地址
//
// 返回值:
//   - bool 是否为公共地址
func isPublicAddr(a ma.Multiaddr) bool {
	ip, err := manet.ToIP(a)
	if err != nil {
		return false
	}
	if ip.To4() != nil {
		return !inAddrRange(ip, manet.Private4) && !inAddrRange(ip, manet.Unroutable4)
	}

	return public6.Contains(ip)
}

// isPrivateAddr 遵循 manet.IsPrivateAddr 的逻辑,但对于 ipv6 使用更严格的"公共"定义
// 参数:
//   - a: ma.Multiaddr 多地址
//
// 返回值:
//   - bool 是否为私有地址
func isPrivateAddr(a ma.Multiaddr) bool {
	ip, err := manet.ToIP(a)
	if err != nil {
		return false
	}
	if ip.To4() != nil {
		return inAddrRange(ip, manet.Private4)
	}

	return !public6.Contains(ip) && !inAddrRange(ip, manet.Unroutable6)
}

// PublicQueryFilter 如果对等节点被认为是可公开访问的,则返回 true
// 参数:
//   - _: interface{} 接口
//   - ai: peer.AddrInfo 对等节点地址信息
//
// 返回值:
//   - bool 是否可公开访问
func PublicQueryFilter(_ interface{}, ai peer.AddrInfo) bool {
	if len(ai.Addrs) == 0 {
		return false
	}

	var hasPublicAddr bool
	for _, a := range ai.Addrs {
		if !isRelayAddr(a) && isPublicAddr(a) {
			hasPublicAddr = true
		}
	}
	return hasPublicAddr
}

type hasHost interface {
	Host() host.Host
}

var _ QueryFilterFunc = PublicQueryFilter

// PublicRoutingTableFilter 如果与该对等节点的连接表明它在公共网络上,则允许将该对等节点添加到路由表中
// 参数:
//   - dht: interface{} DHT 接口
//   - p: peer.ID 对等节点 ID
//
// 返回值:
//   - bool 是否允许添加到路由表
func PublicRoutingTableFilter(dht interface{}, p peer.ID) bool {
	d := dht.(hasHost)

	conns := d.Host().Network().ConnsToPeer(p)
	if len(conns) == 0 {
		return false
	}

	// 我们是否有此对等节点的公共地址?
	id := conns[0].RemotePeer()
	known := d.Host().Peerstore().PeerInfo(id)
	for _, a := range known.Addrs {
		if !isRelayAddr(a) && isPublicAddr(a) {
			return true
		}
	}

	return false
}

var _ RouteTableFilterFunc = PublicRoutingTableFilter

// PrivateQueryFilter 目前不限制我们愿意从本地 DHT 查询的对等节点
// 参数:
//   - _: interface{} 接口
//   - ai: peer.AddrInfo 对等节点地址信息
//
// 返回值:
//   - bool 是否允许查询
func PrivateQueryFilter(_ interface{}, ai peer.AddrInfo) bool {
	return len(ai.Addrs) > 0
}

var _ QueryFilterFunc = PrivateQueryFilter

// 我们经常调用这个,但路由在技术上可以在运行时更改
// 缓存 2 分钟
const routerCacheTime = 2 * time.Minute

var routerCache struct {
	sync.RWMutex
	router  routing.Router
	expires time.Time
}

// getCachedRouter 获取缓存的路由器
// 返回值:
//   - routing.Router 路由器
func getCachedRouter() routing.Router {
	routerCache.RLock()
	router := routerCache.router
	expires := routerCache.expires
	routerCache.RUnlock()

	if time.Now().Before(expires) {
		return router
	}

	routerCache.Lock()
	defer routerCache.Unlock()

	now := time.Now()
	if now.Before(routerCache.expires) {
		return router
	}
	routerCache.router, _ = netroute.New()
	routerCache.expires = now.Add(routerCacheTime)
	return router
}

// PrivateRoutingTableFilter 如果与该对等节点的连接表明它在私有网络上,则允许将该对等节点添加到路由表中
// 参数:
//   - dht: interface{} DHT 接口
//   - p: peer.ID 对等节点 ID
//
// 返回值:
//   - bool 是否允许添加到路由表
func PrivateRoutingTableFilter(dht interface{}, p peer.ID) bool {
	d := dht.(hasHost)
	conns := d.Host().Network().ConnsToPeer(p)
	return privRTFilter(d, conns)
}

// privRTFilter 私有路由表过滤器
// 参数:
//   - dht: interface{} DHT 接口
//   - conns: []network.Conn 连接列表
//
// 返回值:
//   - bool 是否允许添加到路由表
func privRTFilter(dht interface{}, conns []network.Conn) bool {
	d := dht.(hasHost)
	h := d.Host()

	router := getCachedRouter()
	myAdvertisedIPs := make([]net.IP, 0)
	for _, a := range h.Addrs() {
		if isPublicAddr(a) && !isRelayAddr(a) {
			ip, err := manet.ToIP(a)
			if err != nil {
				continue
			}
			myAdvertisedIPs = append(myAdvertisedIPs, ip)
		}
	}

	for _, c := range conns {
		ra := c.RemoteMultiaddr()
		if isPrivateAddr(ra) && !isRelayAddr(ra) {
			return true
		}

		if isPublicAddr(ra) {
			ip, err := manet.ToIP(ra)
			if err != nil {
				continue
			}

			// 如果 IP 与本地主机的公共广播 IP 之一相同 - 则认为它是本地的
			for _, i := range myAdvertisedIPs {
				if i.Equal(ip) {
					return true
				}
				if ip.To4() == nil {
					if i.To4() == nil && isEUI(ip) && sameV6Net(i, ip) {
						return true
					}
				}
			}

			// 如果没有网关 - OS 路由表中的直接主机 - 则认为它是本地的
			// 这特别适用于 ipv6 网络,其中地址可能都是公共的,但节点知道彼此之间的直接链接
			if router != nil {
				_, gw, _, err := router.Route(ip)
				if gw == nil && err == nil {
					return true
				}
			}
		}
	}

	return false
}

var _ RouteTableFilterFunc = PrivateRoutingTableFilter

// isEUI 检查是否为 EUI 地址
// 参数:
//   - ip: net.IP IP 地址
//
// 返回值:
//   - bool 是否为 EUI 地址
func isEUI(ip net.IP) bool {
	// 根据 rfc 2373
	return len(ip) == net.IPv6len && ip[11] == 0xff && ip[12] == 0xfe
}

// sameV6Net 检查两个 IPv6 地址是否在同一网段
// 参数:
//   - a: net.IP 第一个 IP 地址
//   - b: net.IP 第二个 IP 地址
//
// 返回值:
//   - bool 是否在同一网段
func sameV6Net(a, b net.IP) bool {
	//lint:ignore SA1021 我们在这里只比较 IP 地址的部分内容
	return len(a) == net.IPv6len && len(b) == net.IPv6len && bytes.Equal(a[0:8], b[0:8]) //nolint
}

// isRelayAddr 检查是否为中继地址
// 参数:
//   - a: ma.Multiaddr 多地址
//
// 返回值:
//   - bool 是否为中继地址
func isRelayAddr(a ma.Multiaddr) bool {
	found := false
	ma.ForEach(a, func(c ma.Component) bool {
		found = c.Protocol().Code == ma.P_CIRCUIT
		return !found
	})
	return found
}

// inAddrRange 检查 IP 是否在给定的 IP 网段范围内
// 参数:
//   - ip: net.IP IP 地址
//   - ipnets: []*net.IPNet IP 网段列表
//
// 返回值:
//   - bool 是否在范围内
func inAddrRange(ip net.IP, ipnets []*net.IPNet) bool {
	for _, ipnet := range ipnets {
		if ipnet.Contains(ip) {
			return true
		}
	}

	return false
}

// hasValidConnectedness 检查主机与给定 ID 的连接状态是否有效
// 参数:
//   - host: host.Host 主机
//   - id: peer.ID 对等节点 ID
//
// 返回值:
//   - bool 连接状态是否有效
func hasValidConnectedness(host host.Host, id peer.ID) bool {
	connectedness := host.Network().Connectedness(id)
	return connectedness == network.Connected || connectedness == network.Limited
}
