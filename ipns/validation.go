package ipns

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	ipns_pb "github.com/dep2p/kaddht/ipns/pb"
	record "github.com/dep2p/kaddht/record"
	ic "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/peerstore"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"google.golang.org/protobuf/proto"
)

// ValidateWithName 使用给定的 Name 验证 IPNS Record
// 参数:
//   - rec: *Record IPNS记录
//   - name: Name IPNS名称
//
// 返回值:
//   - error 错误信息
func ValidateWithName(rec *Record, name Name) error {
	pk, err := ExtractPublicKey(rec, name)
	if err != nil {
		return err
	}

	return Validate(rec, pk)
}

// Validate 根据给定的公钥验证 IPNS Record,遵循 Record 验证规范
// 参数:
//   - rec: *Record IPNS记录
//   - pk: ic.PubKey 公钥
//
// 返回值:
//   - error 错误信息
func Validate(rec *Record, pk ic.PubKey) error {
	// (1) 确保大小不超过最大记录大小
	if proto.Size(rec.pb) > MaxRecordSize {
		return ErrRecordSize
	}

	// (2) 确保 SignatureV2 和 Data 存在且不为空
	if len(rec.pb.GetSignatureV2()) == 0 {
		return ErrSignature
	}
	if len(rec.pb.GetData()) == 0 {
		return ErrInvalidRecord
	}

	// (3) 提取公钥 - 不需要。通过 ValidateWithName 完成

	// (4) 获取反序列化的 DAG-CBOR 文档数据
	sig2Data, err := recordDataForSignatureV2(rec.pb.GetData())
	if err != nil {
		return fmt.Errorf("无法计算签名数据: %w", err)
	}

	// (6) 验证签名
	if ok, err := pk.Verify(sig2Data, rec.pb.GetSignatureV2()); err != nil || !ok {
		return ErrSignature
	}

	// (5) 如果存在非 CBOR Value 或 SignatureV1,确保 CBOR 数据与 Protobuf 匹配
	if len(rec.pb.GetSignatureV1()) != 0 || len(rec.pb.GetValue()) != 0 {
		if err := validateCborDataMatchesPbData(rec.pb); err != nil {
			return err
		}
	}

	// 检查有效期
	eol, err := rec.Validity()
	if err != nil {
		return err
	}

	if time.Now().After(eol) {
		return ErrExpiredRecord
	}

	return nil
}

// TODO: 这个函数的大部分内容可能可以用代码生成替代
// validateCborDataMatchesPbData 验证 CBOR 数据是否与 Protobuf 数据匹配
// 参数:
//   - entry: *ipns_pb.IpnsRecord IPNS记录
//
// 返回值:
//   - error 错误信息
func validateCborDataMatchesPbData(entry *ipns_pb.IpnsRecord) error {
	if len(entry.GetData()) == 0 {
		return errors.New("记录数据缺失")
	}

	ndbuilder := basicnode.Prototype__Map{}.NewBuilder()
	if err := dagcbor.Decode(ndbuilder, bytes.NewReader(entry.GetData())); err != nil {
		return err
	}

	fullNd := ndbuilder.Build()
	nd, err := fullNd.LookupByString(cborValueKey)
	if err != nil {
		return err
	}
	ndBytes, err := nd.AsBytes()
	if err != nil {
		return err
	}
	if !bytes.Equal(entry.GetValue(), ndBytes) {
		return fmt.Errorf("字段 \"%v\" 在 protobuf 和 CBOR 之间不匹配", cborValueKey)
	}

	nd, err = fullNd.LookupByString(cborValidityKey)
	if err != nil {
		return err
	}
	ndBytes, err = nd.AsBytes()
	if err != nil {
		return err
	}
	if !bytes.Equal(entry.GetValidity(), ndBytes) {
		return fmt.Errorf("字段 \"%v\" 在 protobuf 和 CBOR 之间不匹配", cborValidityKey)
	}

	nd, err = fullNd.LookupByString(cborValidityTypeKey)
	if err != nil {
		return err
	}
	ndInt, err := nd.AsInt()
	if err != nil {
		return err
	}
	if int64(entry.GetValidityType()) != ndInt {
		return fmt.Errorf("字段 \"%v\" 在 protobuf 和 CBOR 之间不匹配", cborValidityTypeKey)
	}

	nd, err = fullNd.LookupByString(cborSequenceKey)
	if err != nil {
		return err
	}
	ndInt, err = nd.AsInt()
	if err != nil {
		return err
	}

	if entry.GetSequence() != uint64(ndInt) {
		return fmt.Errorf("字段 \"%v\" 在 protobuf 和 CBOR 之间不匹配", cborSequenceKey)
	}

	nd, err = fullNd.LookupByString("TTL")
	if err != nil {
		return err
	}
	ndInt, err = nd.AsInt()
	if err != nil {
		return err
	}
	if entry.GetTtl() != uint64(ndInt) {
		return fmt.Errorf("字段 \"%v\" 在 protobuf 和 CBOR 之间不匹配", cborTTLKey)
	}

	return nil
}

var _ record.Validator = Validator{}

// Validator 是一个满足 Libp2p record.Validator 接口的 IPNS 记录验证器
type Validator struct {
	// KeyBook 如果非空,用于查找验证 IPNS 记录的密钥
	KeyBook peerstore.KeyBook
}

// Validate 验证 IPNS 记录
// 参数:
//   - key: string 路由键
//   - value: []byte 记录值
//
// 返回值:
//   - error 错误信息
func (v Validator) Validate(key string, value []byte) error {
	name, err := NameFromRoutingKey([]byte(key))
	if err != nil {
		log.Debugf("无法将 ipns 路由键 %q 解析为名称", key)
		return ErrInvalidName
	}

	r, err := UnmarshalRecord(value)
	if err != nil {
		return err
	}

	pk, err := v.getPublicKey(r, name)
	if err != nil {
		return err
	}

	return Validate(r, pk)
}

// getPublicKey 获取公钥
// 参数:
//   - r: *Record IPNS记录
//   - name: Name IPNS名称
//
// 返回值:
//   - ic.PubKey 公钥
//   - error 错误信息
func (v Validator) getPublicKey(r *Record, name Name) (ic.PubKey, error) {
	switch pk, err := ExtractPublicKey(r, name); err {
	case peer.ErrNoPublicKey:
	case nil:
		return pk, nil
	default:
		return nil, err
	}

	if v.KeyBook == nil {
		log.Debugf("在 IPNS 记录中未找到哈希为 %q 的公钥,且未提供对等存储", name.Peer())
		return nil, ErrPublicKeyNotFound
	}

	pk := v.KeyBook.PubKey(name.Peer())
	if pk == nil {
		log.Debugf("在对等存储中未找到哈希为 %q 的公钥", name.Peer())
		return nil, ErrPublicKeyNotFound
	}

	return pk, nil
}

// Select 通过检查序列号最高和有效期最新来选择最佳记录
// 如果任何记录解析失败,此函数将返回错误
// 此函数不验证记录。调用者负责使用 Validate 确保记录有效
// 参数:
//   - k: string 键
//   - vals: [][]byte 记录值列表
//
// 返回值:
//   - int 选中的记录索引
//   - error 错误信息
func (v Validator) Select(k string, vals [][]byte) (int, error) {
	var recs []*Record
	for _, v := range vals {
		r, err := UnmarshalRecord(v)
		if err != nil {
			return -1, err
		}
		recs = append(recs, r)
	}

	return selectRecord(recs, vals)
}

// selectRecord 从记录列表中选择最佳记录
// 参数:
//   - recs: []*Record 记录列表
//   - vals: [][]byte 记录值列表
//
// 返回值:
//   - int 选中的记录索引
//   - error 错误信息
func selectRecord(recs []*Record, vals [][]byte) (int, error) {
	switch len(recs) {
	case 0:
		return -1, errors.New("给定集合中没有可用记录")
	case 1:
		return 0, nil
	}

	var i int
	for j := 1; j < len(recs); j++ {
		cmp, err := compare(recs[i], recs[j])
		if err != nil {
			return -1, err
		}
		if cmp == 0 {
			cmp = bytes.Compare(vals[i], vals[j])
		}
		if cmp < 0 {
			i = j
		}
	}

	return i, nil
}
