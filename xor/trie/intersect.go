package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// IntersectKeySlices 计算两个key切片的交集
// 参数:
//   - p: []key.Key 第一个key切片
//   - q: []key.Key 第二个key切片
//
// 返回值:
//   - []key.Key 交集结果
func IntersectKeySlices(p, q []key.Key) []key.Key {
	hat := []key.Key{}
	for _, p := range p {
		if keyIsIn(p, q) && !keyIsIn(p, hat) {
			hat = append(hat, p)
		}
	}
	return hat
}

// keyIsIn 检查key是否在切片中存在
// 参数:
//   - q: key.Key 待检查的key
//   - s: []key.Key key切片
//
// 返回值:
//   - bool 是否存在
func keyIsIn(q key.Key, s []key.Key) bool {
	for _, s := range s {
		if key.Equal(q, s) {
			return true
		}
	}
	return false
}

// Intersect 计算两个Trie的key交集
// 参数:
//   - p: *Trie 第一个Trie
//   - q: *Trie 第二个Trie
//
// 返回值:
//   - *Trie 交集结果Trie，永不为nil
func Intersect(p, q *Trie) *Trie {
	return IntersectAtDepth(0, p, q)
}

// IntersectAtDepth 在指定深度计算两个Trie的交集
// 参数:
//   - depth: int 当前深度
//   - p: *Trie 第一个Trie
//   - q: *Trie 第二个Trie
//
// 返回值:
//   - *Trie 交集结果Trie
func IntersectAtDepth(depth int, p, q *Trie) *Trie {
	switch {
	case p.IsLeaf() && q.IsLeaf():
		if p.IsEmpty() || q.IsEmpty() {
			return &Trie{} // 空集
		} else {
			if key.Equal(p.Key, q.Key) {
				return &Trie{Key: p.Key} // 单元素集
			} else {
				return &Trie{} // 空集
			}
		}
	case p.IsLeaf() && !q.IsLeaf():
		if p.IsEmpty() {
			return &Trie{} // 空集
		} else {
			if _, found := q.FindAtDepth(depth, p.Key); found {
				return &Trie{Key: p.Key}
			} else {
				return &Trie{} // 空集
			}
		}
	case !p.IsLeaf() && q.IsLeaf():
		return IntersectAtDepth(depth, q, p)
	case !p.IsLeaf() && !q.IsLeaf():
		disjointUnion := &Trie{
			Branch: [2]*Trie{
				IntersectAtDepth(depth+1, p.Branch[0], q.Branch[0]),
				IntersectAtDepth(depth+1, p.Branch[1], q.Branch[1]),
			},
		}
		disjointUnion.shrink()
		return disjointUnion
	}
	panic("不可达") // unreachable
}
