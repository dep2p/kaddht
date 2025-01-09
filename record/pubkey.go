package record

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
)

// PublicKeyValidator 公钥验证器,用于验证公钥
type PublicKeyValidator struct{}

// Validate 实现 Validator 接口
// 参数:
//   - key: string 键名
//   - value: []byte 值
//
// 返回值:
//   - error 错误信息
//
// 验证传入的记录值是否与传入的键匹配的公钥
func (pkv PublicKeyValidator) Validate(key string, value []byte) error {
	ns, key, err := SplitKey(key)
	if err != nil {
		return err
	}
	if ns != "pk" {
		return errors.New("命名空间不是'pk'")
	}

	keyhash := []byte(key)
	if _, err := mh.Cast(keyhash); err != nil {
		return fmt.Errorf("键不包含有效的多重哈希: %s", err)
	}

	pk, err := crypto.UnmarshalPublicKey(value)
	if err != nil {
		return err
	}
	id, err := peer.IDFromPublicKey(pk)
	if err != nil {
		return err
	}
	if !bytes.Equal(keyhash, []byte(id)) {
		return errors.New("公钥与存储键不匹配")
	}
	return nil
}

// Select 实现 Validator 接口
// 参数:
//   - k: string 键名
//   - vals: [][]byte 值列表
//
// 返回值:
//   - int 选择的索引
//   - error 错误信息
//
// 始终返回0,因为所有公钥都是等效有效的
func (pkv PublicKeyValidator) Select(k string, vals [][]byte) (int, error) {
	return 0, nil
}

var _ Validator = PublicKeyValidator{}
