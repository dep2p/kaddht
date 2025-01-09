package keyspace

import (
	"sort"

	"math/big"
)

// Key 表示 KeySpace 中的标识符。它持有对关联 KeySpace 的引用,以及对原始标识符和新的 KeySpace 字节的引用。
type Key struct {

	// Space 是此 Key 关联的 KeySpace
	Space KeySpace

	// Original 是标识符的原始值
	Original []byte

	// Bytes 是标识符在 KeySpace 中的新值
	Bytes []byte
}

// Equal 判断此 key 是否等于另一个 key
// 参数:
//   - k2: Key 要比较的另一个 key
//
// 返回值:
//   - bool 是否相等
func (k1 Key) Equal(k2 Key) bool {
	if k1.Space != k2.Space {
		panic("k1 和 k2 不在同一个 key space")
	}
	return k1.Space.Equal(k1, k2)
}

// Less 判断此 key 是否小于另一个 key
// 参数:
//   - k2: Key 要比较的另一个 key
//
// 返回值:
//   - bool 是否小于
func (k1 Key) Less(k2 Key) bool {
	if k1.Space != k2.Space {
		panic("k1 和 k2 不在同一个 key space")
	}
	return k1.Space.Less(k1, k2)
}

// Distance 计算此 key 到另一个 key 的距离
// 参数:
//   - k2: Key 目标 key
//
// 返回值:
//   - *big.Int 距离值
func (k1 Key) Distance(k2 Key) *big.Int {
	if k1.Space != k2.Space {
		panic("k1 和 k2 不在同一个 key space")
	}
	return k1.Space.Distance(k1, k2)
}

// KeySpace 是用于对标识符进行数学运算的对象。每个 keyspace 都有自己的属性和规则。参见 XorKeySpace。
type KeySpace interface {

	// Key 将标识符转换为此空间中的 Key
	Key([]byte) Key

	// Equal 判断在此 key space 中两个 key 是否相等
	Equal(Key, Key) bool

	// Distance 返回此 key space 中的距离度量
	Distance(Key, Key) *big.Int

	// Less 判断第一个 key 是否小于第二个
	Less(Key, Key) bool
}

// byDistanceToCenter 是一个用于按照到中心的距离对 Key 进行排序的类型
type byDistanceToCenter struct {
	Center Key
	Keys   []Key
}

func (s byDistanceToCenter) Len() int {
	return len(s.Keys)
}

func (s byDistanceToCenter) Swap(i, j int) {
	s.Keys[i], s.Keys[j] = s.Keys[j], s.Keys[i]
}

func (s byDistanceToCenter) Less(i, j int) bool {
	a := s.Center.Distance(s.Keys[i])
	b := s.Center.Distance(s.Keys[j])
	return a.Cmp(b) == -1
}

// SortByDistance 接收一个 KeySpace、一个中心 Key 和一个待排序的 Key 列表。
// 它返回一个新列表,其中待排序的 Key 按照到中心 Key 的距离进行排序。
// 参数:
//   - sp: KeySpace key空间
//   - center: Key 中心key
//   - toSort: []Key 待排序的key列表
//
// 返回值:
//   - []Key 排序后的key列表
func SortByDistance(sp KeySpace, center Key, toSort []Key) []Key {
	toSortCopy := make([]Key, len(toSort))
	copy(toSortCopy, toSort)
	bdtc := &byDistanceToCenter{
		Center: center,
		Keys:   toSortCopy, // 复制
	}
	sort.Sort(bdtc)
	return bdtc.Keys
}
