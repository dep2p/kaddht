//go:generate protoc -I=pb --go_out=pb pb/record.proto
package ipns

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	ipns_pb "github.com/dep2p/kaddht/ipns/pb"
	ic "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/util"
	logging "github.com/ipfs/go-log/v2"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"go.uber.org/multierr"
	"google.golang.org/protobuf/proto"
)

var log = logging.Logger("ipns")

type ValidityType int64

// ValidityEOL 表示"此记录在{Validity}之前有效"。这是目前唯一支持的有效性类型。
const ValidityEOL ValidityType = 0

// Record 表示一个 IPNS 记录。
//
// [IPNS Record]: https://specs.ipfs.tech/ipns/ipns-record/
type Record struct {
	pb   *ipns_pb.IpnsRecord
	node datamodel.Node
}

// UnmarshalRecord 将 Protobuf 序列化的 IPNS 记录解析为可用的 Record 结构体。
// 请注意,此函数不执行记录的完整验证。要进行验证请使用 Validate。
//
// 参数:
//   - data: []byte 序列化的记录数据
//
// 返回值:
//   - *Record: 解析后的记录
//   - error: 错误信息
func UnmarshalRecord(data []byte) (*Record, error) {
	if len(data) > MaxRecordSize {
		return nil, ErrRecordSize
	}

	var pb ipns_pb.IpnsRecord
	err := proto.Unmarshal(data, &pb)
	if err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}

	record := &Record{
		pb: &pb,
	}

	// 确保记录包含 DAG-CBOR 数据,因为我们需要它
	if len(pb.GetData()) == 0 {
		return nil, multierr.Combine(ErrInvalidRecord, ErrDataMissing)
	}

	// 解码 CBOR 数据
	builder := basicnode.Prototype__Map{}.NewBuilder()
	if err := dagcbor.Decode(builder, bytes.NewReader(pb.GetData())); err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}
	record.node = builder.Build()

	return record, nil
}

// MarshalRecord 将给定的 IPNS 记录编码为 Protobuf 序列化格式。
//
// 参数:
//   - rec: *Record 要序列化的记录
//
// 返回值:
//   - []byte: 序列化后的数据
//   - error: 错误信息
func MarshalRecord(rec *Record) ([]byte, error) {
	return proto.Marshal(rec.pb)
}

// Value 返回此 IPNS 记录中嵌入的 path.Path。
// 如果路径无效,将返回 ErrInvalidPath。
//
// 返回值:
//   - path.Path: 记录中的路径
//   - error: 错误信息
func (rec *Record) Value() (path.Path, error) {
	value, err := rec.getBytesValue(cborValueKey)
	if err != nil {
		return nil, err
	}

	p, err := path.NewPath(string(value))
	if err != nil {
		return nil, multierr.Combine(ErrInvalidPath, err)
	}

	return p, nil
}

// ValidityType 返回记录的有效性类型。
//
// 返回值:
//   - ValidityType: 有效性类型
//   - error: 错误信息
func (rec *Record) ValidityType() (ValidityType, error) {
	value, err := rec.getIntValue(cborValidityTypeKey)
	if err != nil {
		return -1, err
	}

	return ValidityType(value), nil
}

// Validity 返回 IPNS 记录的有效期。如果记录的有效性类型不是 EOL,此函数返回 ErrUnrecognizedValidity。
// 否则,如果无法解析 EOL,则返回错误。
//
// 返回值:
//   - time.Time: 有效期时间
//   - error: 错误信息
func (rec *Record) Validity() (time.Time, error) {
	validityType, err := rec.ValidityType()
	if err != nil {
		return time.Time{}, err
	}

	switch validityType {
	case ValidityEOL:
		value, err := rec.getBytesValue(cborValidityKey)
		if err != nil {
			return time.Time{}, err
		}

		v, err := util.ParseRFC3339(string(value))
		if err != nil {
			return time.Time{}, multierr.Combine(ErrInvalidValidity, err)
		}
		return v, nil
	default:
		return time.Time{}, ErrUnrecognizedValidity
	}
}

// Sequence 返回记录的序列号。
//
// 返回值:
//   - uint64: 序列号
//   - error: 错误信息
func (rec *Record) Sequence() (uint64, error) {
	value, err := rec.getIntValue(cborSequenceKey)
	if err != nil {
		return 0, err
	}

	return uint64(value), nil
}

// TTL 返回记录的生存时间。
//
// 返回值:
//   - time.Duration: 生存时间
//   - error: 错误信息
func (rec *Record) TTL() (time.Duration, error) {
	value, err := rec.getIntValue(cborTTLKey)
	if err != nil {
		return 0, err
	}

	return time.Duration(value), nil
}

// PubKey 返回记录的公钥。
//
// 返回值:
//   - ic.PubKey: 公钥
//   - error: 错误信息
func (rec *Record) PubKey() (ic.PubKey, error) {
	if pk := rec.pb.GetPubKey(); len(pk) != 0 {
		return ic.UnmarshalPublicKey(pk)
	}

	return nil, ErrPublicKeyNotFound
}

// getBytesValue 获取指定键的字节值。
//
// 参数:
//   - key: string 键名
//
// 返回值:
//   - []byte: 字节值
//   - error: 错误信息
func (rec *Record) getBytesValue(key string) ([]byte, error) {
	node, err := rec.node.LookupByString(key)
	if err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}

	value, err := node.AsBytes()
	if err != nil {
		return nil, multierr.Combine(ErrInvalidRecord, err)
	}

	return value, nil
}

// getIntValue 获取指定键的整数值。
//
// 参数:
//   - key: string 键名
//
// 返回值:
//   - int64: 整数值
//   - error: 错误信息
func (rec *Record) getIntValue(key string) (int64, error) {
	node, err := rec.node.LookupByString(key)
	if err != nil {
		return -1, multierr.Combine(ErrInvalidRecord, err)
	}

	value, err := node.AsInt()
	if err != nil {
		return -1, multierr.Combine(ErrInvalidRecord, err)
	}

	return value, nil
}

const (
	cborValidityKey     = "Validity"
	cborValidityTypeKey = "ValidityType"
	cborValueKey        = "Value"
	cborSequenceKey     = "Sequence"
	cborTTLKey          = "TTL"
)

type options struct {
	v1Compatibility bool
	embedPublicKey  *bool
}

type Option func(*options)

// WithV1Compatibility 设置是否兼容 V1 版本。
//
// 参数:
//   - compatible: bool 是否兼容
//
// 返回值:
//   - Option: 选项函数
func WithV1Compatibility(compatible bool) Option {
	return func(o *options) {
		o.v1Compatibility = compatible
	}
}

// WithPublicKey 设置是否嵌入公钥。
//
// 参数:
//   - embedded: bool 是否嵌入
//
// 返回值:
//   - Option: 选项函数
func WithPublicKey(embedded bool) Option {
	return func(o *options) {
		o.embedPublicKey = &embedded
	}
}

// processOptions 处理选项。
//
// 参数:
//   - opts: ...Option 选项列表
//
// 返回值:
//   - *options: 处理后的选项
func processOptions(opts ...Option) *options {
	options := &options{
		// TODO: 在 IPIP-XXXX 随 Kubo 和 Helia 发布至少 6 个月后,默认生成 V2 记录
		v1Compatibility: true,
	}

	for _, opt := range opts {
		opt(options)
	}
	return options
}

// NewRecord 创建一个新的 IPNS 记录并使用给定的私钥签名。
// 默认情况下,我们会为那些对等点 ID 不编码公钥的密钥类型(如 RSA 和 ECDSA)嵌入公钥。这可以通过 WithPublicKey 选项更改。
// 此外,默认情况下会创建兼容 V1 的记录。
//
// 参数:
//   - sk: ic.PrivKey 私钥
//   - value: path.Path 路径值
//   - seq: uint64 序列号
//   - eol: time.Time 过期时间
//   - ttl: time.Duration 生存时间
//   - opts: ...Option 选项列表
//
// 返回值:
//   - *Record: 创建的记录
//   - error: 错误信息
func NewRecord(sk ic.PrivKey, value path.Path, seq uint64, eol time.Time, ttl time.Duration, opts ...Option) (*Record, error) {
	options := processOptions(opts...)

	node, err := createNode(value, seq, eol, ttl)
	if err != nil {
		return nil, err
	}

	cborData, err := nodeToCBOR(node)
	if err != nil {
		return nil, err
	}

	sig2Data, err := recordDataForSignatureV2(cborData)
	if err != nil {
		return nil, err
	}

	sig2, err := sk.Sign(sig2Data)
	if err != nil {
		return nil, err
	}

	pb := ipns_pb.IpnsRecord{
		Data:        cborData,
		SignatureV2: sig2,
	}

	if options.v1Compatibility {
		pb.Value = []byte(value.String())
		typ := ipns_pb.IpnsRecord_EOL
		pb.ValidityType = &typ
		pb.Sequence = &seq
		pb.Validity = []byte(util.FormatRFC3339(eol))
		ttlNs := uint64(ttl.Nanoseconds())
		pb.Ttl = proto.Uint64(ttlNs)

		// 目前我们仍然创建 V1 签名。这些已被弃用,在验证期间不再使用(Validate 函数需要 SignatureV2),但在此设置它允许旧节点(例如 go-ipfs < v0.9.0)仍然可以解析由现代节点发布的 IPNS
		sig1, err := sk.Sign(recordDataForSignatureV1(&pb))
		if err != nil {
			return nil, fmt.Errorf("%w: could not compute signature data", err)
		}
		pb.SignatureV1 = sig1
	}

	embedPublicKey := false
	if options.embedPublicKey == nil {
		embedPublicKey, err = needToEmbedPublicKey(sk.GetPublic())
		if err != nil {
			return nil, err
		}
	} else {
		embedPublicKey = *options.embedPublicKey
	}

	if embedPublicKey {
		pkBytes, err := ic.MarshalPublicKey(sk.GetPublic())
		if err != nil {
			return nil, err
		}
		pb.PubKey = pkBytes
	}

	return &Record{
		pb:   &pb,
		node: node,
	}, nil
}

// createNode 创建一个新的 IPLD 节点。
//
// 参数:
//   - value: path.Path 路径值
//   - seq: uint64 序列号
//   - eol: time.Time 过期时间
//   - ttl: time.Duration 生存时间
//
// 返回值:
//   - datamodel.Node: 创建的节点
//   - error: 错误信息
func createNode(value path.Path, seq uint64, eol time.Time, ttl time.Duration) (datamodel.Node, error) {
	m := make(map[string]ipld.Node)
	var keys []string

	m[cborValueKey] = basicnode.NewBytes([]byte(value.String()))
	keys = append(keys, cborValueKey)

	m[cborValidityKey] = basicnode.NewBytes([]byte(util.FormatRFC3339(eol)))
	keys = append(keys, cborValidityKey)

	m[cborValidityTypeKey] = basicnode.NewInt(int64(ValidityEOL))
	keys = append(keys, cborValidityTypeKey)

	m[cborSequenceKey] = basicnode.NewInt(int64(seq))
	keys = append(keys, cborSequenceKey)

	m[cborTTLKey] = basicnode.NewInt(int64(ttl))
	keys = append(keys, cborTTLKey)

	sort.Slice(keys, func(i, j int) bool {
		li, lj := len(keys[i]), len(keys[j])
		if li == lj {
			return keys[i] < keys[j]
		}
		return li < lj
	})

	newNd := basicnode.Prototype__Map{}.NewBuilder()
	ma, err := newNd.BeginMap(int64(len(keys)))
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		if err := ma.AssembleKey().AssignString(k); err != nil {
			return nil, err
		}
		if err := ma.AssembleValue().AssignNode(m[k]); err != nil {
			return nil, err
		}
	}

	if err := ma.Finish(); err != nil {
		return nil, err
	}

	return newNd.Build(), nil
}

// nodeToCBOR 将节点编码为 CBOR 格式。
//
// 参数:
//   - node: datamodel.Node 要编码的节点
//
// 返回值:
//   - []byte: 编码后的数据
//   - error: 错误信息
func nodeToCBOR(node datamodel.Node) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := dagcbor.Encode(node, buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// recordDataForSignatureV1 生成用于 V1 签名的数据。
//
// 参数:
//   - e: *ipns_pb.IpnsRecord 记录
//
// 返回值:
//   - []byte: 签名数据
func recordDataForSignatureV1(e *ipns_pb.IpnsRecord) []byte {
	return bytes.Join([][]byte{
		e.Value,
		e.Validity,
		[]byte(fmt.Sprint(e.GetValidityType())),
	},
		[]byte{})
}

// recordDataForSignatureV2 生成用于 V2 签名的数据。
//
// 参数:
//   - data: []byte 记录数据
//
// 返回值:
//   - []byte: 签名数据
//   - error: 错误信息
func recordDataForSignatureV2(data []byte) ([]byte, error) {
	dataForSig := []byte("ipns-signature:")
	dataForSig = append(dataForSig, data...)
	return dataForSig, nil
}

// needToEmbedPublicKey 检查是否需要嵌入公钥。
//
// 参数:
//   - pk: ic.PubKey 公钥
//
// 返回值:
//   - bool: 是否需要嵌入
//   - error: 错误信息
func needToEmbedPublicKey(pk ic.PubKey) (bool, error) {
	// 首先尝试从公钥中提取对等点 ID
	pid, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return false, fmt.Errorf("无法将公钥转换为对等点 ID: %w", err)
	}

	_, err = pid.ExtractPublicKey()
	if err == nil {
		// 可以提取,因此无需嵌入公钥
		return false, nil
	}

	if errors.Is(err, peer.ErrNoPublicKey) {
		return true, nil
	}

	return false, fmt.Errorf("无法从公钥提取 ID: %w", err)
}

// compare 比较两个 IPNS 记录。它返回:
//   - -1 如果 a 比 b 旧
//   - 0 如果 a 和 b 无法排序(这并不意味着它们相等)
//   - +1 如果 a 比 b 新
//
// 此函数不验证记录。调用者负责使用 Validate 确保记录有效。
//
// 参数:
//   - a: *Record 第一个记录
//   - b: *Record 第二个记录
//
// 返回值:
//   - int: 比较结果
//   - error: 错误信息
func compare(a, b *Record) (int, error) {
	aHasV2Sig := a.pb.GetSignatureV2() != nil
	bHasV2Sig := b.pb.GetSignatureV2() != nil

	// 具有较新的签名版本比具有较旧的签名版本更好
	if aHasV2Sig && !bHasV2Sig {
		return 1, nil
	} else if !aHasV2Sig && bHasV2Sig {
		return -1, nil
	}

	as, err := a.Sequence()
	if err != nil {
		return 0, err
	}

	bs, err := b.Sequence()
	if err != nil {
		return 0, err
	}

	if as > bs {
		return 1, nil
	} else if as < bs {
		return -1, nil
	}

	at, err := a.Validity()
	if err != nil {
		return 0, err
	}

	bt, err := b.Validity()
	if err != nil {
		return 0, err
	}

	if at.After(bt) {
		return 1, nil
	} else if bt.After(at) {
		return -1, nil
	}

	return 0, nil
}

// ExtractPublicKey 从 IPNS 记录中提取与给定名称匹配的公钥(如果可能)。
//
// 参数:
//   - rec: *Record IPNS 记录
//   - name: Name 名称
//
// 返回值:
//   - ic.PubKey: 提取的公钥
//   - error: 错误信息
func ExtractPublicKey(rec *Record, name Name) (ic.PubKey, error) {
	if pk, err := rec.PubKey(); err == nil {
		expPid, err := peer.IDFromPublicKey(pk)
		if err != nil {
			return nil, multierr.Combine(ErrInvalidPublicKey, err)
		}

		if !name.Equal(NameFromPeer(expPid)) {
			return nil, ErrPublicKeyMismatch
		}

		return pk, nil
	} else if !errors.Is(err, ErrPublicKeyNotFound) {
		return nil, multierr.Combine(ErrInvalidPublicKey, err)
	} else {
		return name.Peer().ExtractPublicKey()
	}
}
