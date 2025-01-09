package dht_pb

import (
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"

	logging "github.com/ipfs/go-log/v2"
	ma "github.com/multiformats/go-multiaddr"
)

var log = logging.Logger("dht.pb")

// PeerRoutingInfo 对等节点路由信息
type PeerRoutingInfo struct {
	peer.AddrInfo
	network.Connectedness
}

// NewMessage 构造一个新的DHT消息
// 参数:
//   - typ: Message_MessageType 消息类型
//   - key: []byte 键值
//   - level: int 集群层级
//
// 返回值:
//   - *Message DHT消息对象
func NewMessage(typ Message_MessageType, key []byte, level int) *Message {
	m := &Message{
		Type: typ,
		Key:  key,
	}
	m.SetClusterLevel(level)
	return m
}

// peerRoutingInfoToPBPeer 将PeerRoutingInfo转换为Message_Peer
// 参数:
//   - p: PeerRoutingInfo 对等节点路由信息
//
// 返回值:
//   - Message_Peer 转换后的消息对等节点
func peerRoutingInfoToPBPeer(p PeerRoutingInfo) Message_Peer {
	var pbp Message_Peer

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // 使用Bytes而不是String,已压缩
	}
	pbp.Id = byteString(p.ID)
	pbp.Connection = ConnectionType(p.Connectedness)
	return pbp
}

// peerInfoToPBPeer 将peer.AddrInfo转换为Message_Peer
// 参数:
//   - p: peer.AddrInfo 对等节点地址信息
//
// 返回值:
//   - Message_Peer 转换后的消息对等节点
func peerInfoToPBPeer(p peer.AddrInfo) Message_Peer {
	var pbp Message_Peer

	pbp.Addrs = make([][]byte, len(p.Addrs))
	for i, maddr := range p.Addrs {
		pbp.Addrs[i] = maddr.Bytes() // 使用Bytes而不是String,已压缩
	}
	pbp.Id = byteString(p.ID)
	return pbp
}

// PBPeerToPeerInfo 将Message_Peer转换为peer.AddrInfo
// 参数:
//   - pbp: Message_Peer 消息对等节点
//
// 返回值:
//   - peer.AddrInfo 对等节点地址信息
func PBPeerToPeerInfo(pbp Message_Peer) peer.AddrInfo {
	return peer.AddrInfo{
		ID:    peer.ID(pbp.Id),
		Addrs: pbp.Addresses(),
	}
}

// RawPeerInfosToPBPeers 将对等节点切片转换为Message_Peer切片
// 参数:
//   - peers: []peer.AddrInfo 对等节点地址信息切片
//
// 返回值:
//   - []Message_Peer 转换后的消息对等节点切片
func RawPeerInfosToPBPeers(peers []peer.AddrInfo) []Message_Peer {
	pbpeers := make([]Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerInfoToPBPeer(p)
	}
	return pbpeers
}

// PeerInfosToPBPeers 将对等节点切片转换为Message_Peer切片,并设置连接状态
// 参数:
//   - n: network.Network 网络对象
//   - peers: []peer.AddrInfo 对等节点地址信息切片
//
// 返回值:
//   - []Message_Peer 转换后的消息对等节点切片
func PeerInfosToPBPeers(n network.Network, peers []peer.AddrInfo) []Message_Peer {
	pbps := RawPeerInfosToPBPeers(peers)
	for i := range pbps {
		c := ConnectionType(n.Connectedness(peers[i].ID))
		pbps[i].Connection = c
	}
	return pbps
}

// PeerRoutingInfosToPBPeers 将PeerRoutingInfo切片转换为Message_Peer切片
// 参数:
//   - peers: []PeerRoutingInfo 对等节点路由信息切片
//
// 返回值:
//   - []Message_Peer 转换后的消息对等节点切片
func PeerRoutingInfosToPBPeers(peers []PeerRoutingInfo) []Message_Peer {
	pbpeers := make([]Message_Peer, len(peers))
	for i, p := range peers {
		pbpeers[i] = peerRoutingInfoToPBPeer(p)
	}
	return pbpeers
}

// PBPeersToPeerInfos 将Message_Peer切片转换为peer.AddrInfo切片
// 参数:
//   - pbps: []Message_Peer 消息对等节点切片
//
// 返回值:
//   - []*peer.AddrInfo 对等节点地址信息切片,无效地址将被静默忽略
func PBPeersToPeerInfos(pbps []Message_Peer) []*peer.AddrInfo {
	peers := make([]*peer.AddrInfo, 0, len(pbps))
	for _, pbp := range pbps {
		ai := PBPeerToPeerInfo(pbp)
		peers = append(peers, &ai)
	}
	return peers
}

// Addresses 返回与Message_Peer条目关联的多地址
// 参数:
//   - m: *Message_Peer 消息对等节点
//
// 返回值:
//   - []ma.Multiaddr 多地址切片
func (m *Message_Peer) Addresses() []ma.Multiaddr {
	if m == nil {
		return nil
	}

	maddrs := make([]ma.Multiaddr, 0, len(m.Addrs))
	for _, addr := range m.Addrs {
		maddr, err := ma.NewMultiaddrBytes(addr)
		if err != nil {
			log.Debugw("解码对等节点多地址时出错", "peer", peer.ID(m.Id), "error", err)
			continue
		}

		maddrs = append(maddrs, maddr)
	}
	return maddrs
}

// GetClusterLevel 获取并调整消息的集群层级
// 需要+/-1调整以区分有效的第一层级(1)和protobuf默认的"无值"行为(0)
// 参数:
//   - m: *Message 消息对象
//
// 返回值:
//   - int 集群层级
func (m *Message) GetClusterLevel() int {
	level := m.GetClusterLevelRaw() - 1
	if level < 0 {
		return 0
	}
	return int(level)
}

// SetClusterLevel 调整并设置消息的集群层级
// 需要+/-1调整以区分有效的第一层级(1)和protobuf默认的"无值"行为(0)
// 参数:
//   - m: *Message 消息对象
//   - level: int 集群层级
func (m *Message) SetClusterLevel(level int) {
	lvl := int32(level)
	m.ClusterLevelRaw = lvl + 1
}

// ConnectionType 返回与network.Connectedness关联的Message_ConnectionType
// 参数:
//   - c: network.Connectedness 连接状态
//
// 返回值:
//   - Message_ConnectionType 消息连接类型
func ConnectionType(c network.Connectedness) Message_ConnectionType {
	switch c {
	default:
		return Message_NOT_CONNECTED
	case network.NotConnected:
		return Message_NOT_CONNECTED
	case network.Connected:
		return Message_CONNECTED
	}
}

// Connectedness 返回与Message_ConnectionType关联的network.Connectedness
// 参数:
//   - c: Message_ConnectionType 消息连接类型
//
// 返回值:
//   - network.Connectedness 连接状态
func Connectedness(c Message_ConnectionType) network.Connectedness {
	switch c {
	default:
		return network.NotConnected
	case Message_NOT_CONNECTED:
		return network.NotConnected
	case Message_CONNECTED:
		return network.Connected
	}
}
