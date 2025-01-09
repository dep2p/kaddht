package trie

import (
	"encoding/json"

	"github.com/dep2p/kaddht/xor/key"
)

// Trie 是一个用于等长位向量的字典树,仅在叶子节点存储值
// Trie节点的不变性:
// (1) 两个分支要么都为nil,要么都不为nil
// (2) 如果分支不为nil,则key必须为nil
// (3) 如果两个分支都是叶子节点,则它们都必须非空(有key值)
type Trie struct {
	Branch [2]*Trie
	Key    key.Key
}

// New 创建一个新的Trie
// 返回值:
//   - *Trie 新创建的Trie
func New() *Trie {
	return &Trie{}
}

// FromKeys 从key切片创建Trie
// 参数:
//   - k: []key.Key key切片
//
// 返回值:
//   - *Trie 创建的Trie
func FromKeys(k []key.Key) *Trie {
	t := New()
	for _, k := range k {
		t.Add(k)
	}
	return t
}

// FromKeysAtDepth 在指定深度从key切片创建Trie
// 参数:
//   - depth: int 深度
//   - k: []key.Key key切片
//
// 返回值:
//   - *Trie 创建的Trie
func FromKeysAtDepth(depth int, k []key.Key) *Trie {
	t := New()
	for _, k := range k {
		t.AddAtDepth(depth, k)
	}
	return t
}

// String 返回Trie的JSON字符串表示
// 返回值:
//   - string JSON字符串
func (trie *Trie) String() string {
	b, _ := json.Marshal(trie)
	return string(b)
}

// Depth 返回Trie的深度
// 返回值:
//   - int Trie的深度
func (trie *Trie) Depth() int {
	return trie.DepthAtDepth(0)
}

// DepthAtDepth 返回从指定深度开始的Trie深度
// 参数:
//   - depth: int 起始深度
//
// 返回值:
//   - int Trie的深度
func (trie *Trie) DepthAtDepth(depth int) int {
	if trie.Branch[0] == nil && trie.Branch[1] == nil {
		return depth
	} else {
		return max(trie.Branch[0].DepthAtDepth(depth+1), trie.Branch[1].DepthAtDepth(depth+1))
	}
}

// max 返回两个整数中的较大值
// 参数:
//   - x: int 第一个整数
//   - y: int 第二个整数
//
// 返回值:
//   - int 较大值
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// Size 返回Trie中添加的key数量
// 即返回Trie中非空叶子节点的数量
// 返回值:
//   - int key的数量
func (trie *Trie) Size() int {
	return trie.SizeAtDepth(0)
}

// SizeAtDepth 返回从指定深度开始的Trie中key的数量
// 参数:
//   - depth: int 起始深度
//
// 返回值:
//   - int key的数量
func (trie *Trie) SizeAtDepth(depth int) int {
	if trie.Branch[0] == nil && trie.Branch[1] == nil {
		if trie.IsEmpty() {
			return 0
		} else {
			return 1
		}
	} else {
		return trie.Branch[0].SizeAtDepth(depth+1) + trie.Branch[1].SizeAtDepth(depth+1)
	}
}

// IsEmpty 判断Trie节点是否为空
// 返回值:
//   - bool 是否为空
func (trie *Trie) IsEmpty() bool {
	return trie.Key == nil
}

// IsLeaf 判断是否为叶子节点
// 返回值:
//   - bool 是否为叶子节点
func (trie *Trie) IsLeaf() bool {
	return trie.Branch[0] == nil && trie.Branch[1] == nil
}

// IsEmptyLeaf 判断是否为空叶子节点
// 返回值:
//   - bool 是否为空叶子节点
func (trie *Trie) IsEmptyLeaf() bool {
	return trie.IsEmpty() && trie.IsLeaf()
}

// IsNonEmptyLeaf 判断是否为非空叶子节点
// 返回值:
//   - bool 是否为非空叶子节点
func (trie *Trie) IsNonEmptyLeaf() bool {
	return !trie.IsEmpty() && trie.IsLeaf()
}

// Copy 复制Trie
// 返回值:
//   - *Trie 复制的Trie
func (trie *Trie) Copy() *Trie {
	if trie.IsLeaf() {
		return &Trie{Key: trie.Key}
	}

	return &Trie{Branch: [2]*Trie{
		trie.Branch[0].Copy(),
		trie.Branch[1].Copy(),
	}}
}

// shrink 收缩Trie
// 当子节点为空叶子节点时,将其合并到父节点
func (trie *Trie) shrink() {
	b0, b1 := trie.Branch[0], trie.Branch[1]
	switch {
	case b0.IsEmptyLeaf() && b1.IsEmptyLeaf():
		trie.Branch[0], trie.Branch[1] = nil, nil
	case b0.IsEmptyLeaf() && b1.IsNonEmptyLeaf():
		trie.Key = b1.Key
		trie.Branch[0], trie.Branch[1] = nil, nil
	case b0.IsNonEmptyLeaf() && b1.IsEmptyLeaf():
		trie.Key = b0.Key
		trie.Branch[0], trie.Branch[1] = nil, nil
	}
}
