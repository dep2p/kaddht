package kbucket

import (
	"errors"

	"github.com/minio/sha256-simd"

	ks "github.com/dep2p/kaddht/kbucket/keyspace"
	"github.com/dep2p/libp2p/core/peer"

	u "github.com/ipfs/boxo/util"
)

// ErrLookupFailure 当路由表查询没有返回结果时返回此错误。这不是预期的行为
var ErrLookupFailure = errors.New("failed to find any peer in table")

// ID 用于IpfsDHT,在XOR密钥空间中
//
// dht.ID类型表示其内容已从peer.ID或util.Key进行哈希处理。这统一了密钥空间
type ID []byte

// less 比较两个ID的大小
// 参数:
//   - other: ID 要比较的另一个ID
//
// 返回值:
//   - bool 如果当前ID小于other则返回true
func (id ID) less(other ID) bool {
	a := ks.Key{Space: ks.XORKeySpace, Bytes: id}
	b := ks.Key{Space: ks.XORKeySpace, Bytes: other}
	return a.Less(b)
}

// xor 计算两个ID的异或值
// 参数:
//   - a: ID 第一个ID
//   - b: ID 第二个ID
//
// 返回值:
//   - ID 异或结果
func xor(a, b ID) ID {
	return ID(u.XOR(a, b))
}

// CommonPrefixLen 计算两个ID的公共前缀长度
// 参数:
//   - a: ID 第一个ID
//   - b: ID 第二个ID
//
// 返回值:
//   - int 公共前缀的长度
func CommonPrefixLen(a, b ID) int {
	return ks.ZeroPrefixLen(u.XOR(a, b))
}

// ConvertPeerID 通过对Peer ID(Multihash)进行哈希来创建DHT ID
// 参数:
//   - id: peer.ID 要转换的Peer ID
//
// 返回值:
//   - ID 生成的DHT ID
func ConvertPeerID(id peer.ID) ID {
	hash := sha256.Sum256([]byte(id))
	return hash[:]
}

// ConvertKey 通过对本地key(字符串)进行哈希来创建DHT ID
// 参数:
//   - id: string 要转换的key字符串
//
// 返回值:
//   - ID 生成的DHT ID
func ConvertKey(id string) ID {
	hash := sha256.Sum256([]byte(id))
	return hash[:]
}

// Closer 判断节点a是否比节点b更接近目标key
// 参数:
//   - a: peer.ID 第一个节点ID
//   - b: peer.ID 第二个节点ID
//   - key: string 目标key
//
// 返回值:
//   - bool 如果a比b更接近key则返回true
func Closer(a, b peer.ID, key string) bool {
	aid := ConvertPeerID(a)
	bid := ConvertPeerID(b)
	tgt := ConvertKey(key)
	adist := xor(aid, tgt)
	bdist := xor(bid, tgt)

	return adist.less(bdist)
}
