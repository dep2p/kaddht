package ipns

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/dep2p/libp2p/core/peer"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/go-cid"
	mb "github.com/multiformats/go-multibase"
	mc "github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
)

const (
	// NamespacePrefix 是 IPNS 命名空间的前缀
	NamespacePrefix = "/ipns/"
)

// Name 表示根据 IPNS 名称规范序列化的公钥的多重哈希值
//
// 参考:
// - 多重哈希: https://multiformats.io/multihash/
// - IPNS 名称: https://specs.ipfs.tech/ipns/ipns-record/#ipns-name
type Name struct {
	multihash string // 不带多重基编码的二进制多重哈希值
}

// NameFromString 从给定的字符串表示形式创建 IPNS 名称
//
// 参数:
//   - str: string IPNS 名称的字符串表示
//
// 返回值:
//   - Name IPNS 名称对象
//   - error 错误信息
func NameFromString(str string) (Name, error) {
	str = strings.TrimPrefix(str, NamespacePrefix)
	pid, err := peer.Decode(str)
	if err != nil {
		return Name{}, err
	}
	return NameFromPeer(pid), nil
}

// NameFromRoutingKey 从给定的路由键表示形式创建 IPNS 名称。更多信息请参见 Name.RoutingKey
//
// 参数:
//   - data: []byte 路由键数据
//
// 返回值:
//   - Name IPNS 名称对象
//   - error 错误信息
func NameFromRoutingKey(data []byte) (Name, error) {
	if !bytes.HasPrefix(data, []byte(NamespacePrefix)) {
		return Name{}, ErrInvalidName
	}

	data = bytes.TrimPrefix(data, []byte(NamespacePrefix))
	pid, err := peer.IDFromBytes(data)
	if err != nil {
		return Name{}, err
	}
	return NameFromPeer(pid), nil
}

// NameFromPeer 从给定的对等节点 ID 创建 IPNS 名称
//
// 参数:
//   - pid: peer.ID 对等节点 ID
//
// 返回值:
//   - Name IPNS 名称对象
func NameFromPeer(pid peer.ID) Name {
	return Name{multihash: string(pid)}
}

// NameFromCid 从给定的 CID 创建 IPNS 名称
//
// 参数:
//   - c: cid.Cid 内容标识符
//
// 返回值:
//   - Name IPNS 名称对象
//   - error 错误信息
func NameFromCid(c cid.Cid) (Name, error) {
	code := mc.Code(c.Type())
	if code != mc.Libp2pKey {
		return Name{}, fmt.Errorf("CID 编解码器 %q 不允许用于 IPNS 名称，请使用 %q", code, mc.Libp2pKey)
	}
	return Name{multihash: string(c.Hash())}, nil
}

// RoutingKey 返回给定 IPNS 名称的二进制路由键。注意，此函数仅用于路由目的。
// 此函数的输出是二进制的，不可读。如需人类可读的字符串，请参见 Name.Key。
//
// 返回值:
//   - []byte 路由键数据
func (n Name) RoutingKey() []byte {
	var buffer bytes.Buffer
	buffer.WriteString(NamespacePrefix)
	buffer.WriteString(n.multihash) // 注意：我们附加原始多重哈希字节（无多重基编码）
	return buffer.Bytes()
}

// Cid 将 IPNS 名称编码为公钥的 CID
// 如果 IPNS 名称无效（例如为空），将返回未定义的 CID
//
// 返回值:
//   - cid.Cid 内容标识符
func (n Name) Cid() cid.Cid {
	m, err := mh.Cast([]byte(n.multihash))
	if err != nil {
		return cid.Undef
	}
	return cid.NewCidV1(cid.Libp2pKey, m)
}

// Peer 将 IPNS 名称转换为对等节点 ID
//
// 返回值:
//   - peer.ID 对等节点 ID
func (n Name) Peer() peer.ID {
	return peer.ID(n.multihash)
}

// String 返回人类可读的 IPNS 名称，使用 libp2p-key 多重编解码器 (0x72) 和不区分大小写的 Base36 编码为 CIDv1
//
// 返回值:
//   - string IPNS 名称的字符串表示
func (n Name) String() string {
	name, err := n.Cid().StringOfBase(mb.Base36)
	if err != nil {
		panic(fmt.Errorf("cid.StringOfBase 使用了错误的参数: %w", err))
	}
	return name
}

// UnmarshalJSON 实现 json.Unmarshaler 接口。IPNS 名称将通过 NameFromString 从字符串解析
//
// 参数:
//   - b: []byte JSON 数据
//
// 返回值:
//   - error 错误信息
func (n *Name) UnmarshalJSON(b []byte) error {
	var str string
	err := json.Unmarshal(b, &str)
	if err != nil {
		return err
	}

	v, err := NameFromString(str)
	if err != nil {
		return err
	}

	*n = v
	return nil
}

// MarshalJSON 实现 json.Marshaler 接口
// IPNS 名称将使用 Name.String 序列化为字符串
//
// 返回值:
//   - []byte JSON 数据
//   - error 错误信息
func (n Name) MarshalJSON() ([]byte, error) {
	return json.Marshal(n.String())
}

// Equal 返回两个记录是否相等
//
// 参数:
//   - other: Name 另一个 IPNS 名称
//
// 返回值:
//   - bool 是否相等
func (n Name) Equal(other Name) bool {
	return n.multihash == other.multihash
}

// AsPath 将 IPNS 名称转换为以 path.IPNSNamespace 为前缀的路径
//
// 返回值:
//   - path.Path IPNS 路径
func (n Name) AsPath() path.Path {
	p, err := path.NewPathFromSegments(path.IPNSNamespace, n.String())
	if err != nil {
		panic(fmt.Errorf("path.NewPathFromSegments 使用了无效的参数: %w", err))
	}
	return p
}
