package record

import (
	"errors"
	"fmt"
)

// ErrInvalidRecordType 当DHT记录的键前缀在DHT的验证器映射中未找到时返回此错误
var ErrInvalidRecordType = errors.New("invalid record keytype")

// ErrBetterRecord 当子系统因找到更好的记录而失败时返回此错误
type ErrBetterRecord struct {
	// Key 与记录关联的键
	Key string
	// Value 根据记录验证器找到的最佳值
	Value []byte
}

// Error 返回错误信息
// 返回值:
//   - string 格式化的错误信息
func (e *ErrBetterRecord) Error() string {
	return fmt.Sprintf("found better value for %q", e.Key)
}

// Validator 记录验证器接口
type Validator interface {
	// Validate 验证给定的记录
	// 参数:
	//   - key: string 记录的键
	//   - value: []byte 记录的值
	//
	// 返回值:
	//   - error 如果记录无效(如过期、签名错误等)则返回错误
	Validate(key string, value []byte) error

	// Select 从记录集中选择最佳记录(如最新的)
	// 参数:
	//   - key: string 记录的键
	//   - values: [][]byte 记录值的集合
	//
	// 返回值:
	//   - int 选中记录的索引
	//   - error 选择过程中的错误
	//
	// 注意: Select做出的决定应该是稳定的
	Select(key string, values [][]byte) (int, error)
}

// NamespacedValidator 按命名空间委派给子验证器的验证器
type NamespacedValidator map[string]Validator

// ValidatorByKey 查找负责验证给定键的验证器
// 参数:
//   - key: string 需要验证的键
//
// 返回值:
//   - Validator 对应的验证器
func (v NamespacedValidator) ValidatorByKey(key string) Validator {
	ns, _, err := SplitKey(key)
	if err != nil {
		return nil
	}
	return v[ns]
}

// Validate 实现Validator接口
// 参数:
//   - key: string 记录的键
//   - value: []byte 记录的值
//
// 返回值:
//   - error 验证过程中的错误
func (v NamespacedValidator) Validate(key string, value []byte) error {
	vi := v.ValidatorByKey(key)
	if vi == nil {
		return ErrInvalidRecordType
	}
	return vi.Validate(key, value)
}

// Select 实现Validator接口
// 参数:
//   - key: string 记录的键
//   - values: [][]byte 记录值的集合
//
// 返回值:
//   - int 选中记录的索引
//   - error 选择过程中的错误
func (v NamespacedValidator) Select(key string, values [][]byte) (int, error) {
	if len(values) == 0 {
		return 0, errors.New("无法从空值中进行选择")
	}
	vi := v.ValidatorByKey(key)
	if vi == nil {
		return 0, ErrInvalidRecordType
	}
	return vi.Select(key, values)
}

var _ Validator = NamespacedValidator{}
