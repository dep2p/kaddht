package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// UnionKeySlices 合并两个Key切片
// 参数:
//   - left: []key.Key 第一个Key切片
//   - right: []key.Key 第二个Key切片
//
// 返回值:
//   - []key.Key 合并后的Key切片
func UnionKeySlices(left, right []key.Key) []key.Key {
	result := append([]key.Key{}, left...)
	for _, item := range right {
		if !keyIsIn(item, result) {
			result = append(result, item)
		}
	}
	return result
}

// Union 合并两个Trie树
// 参数:
//   - left: *Trie 第一个Trie树
//   - right: *Trie 第二个Trie树
//
// 返回值:
//   - *Trie 合并后的Trie树
func Union(left, right *Trie) *Trie {
	return UnionAtDepth(0, left, right)
}

// UnionAtDepth 在指定深度合并两个Trie树
// 参数:
//   - depth: int 当前深度
//   - left: *Trie 第一个Trie树
//   - right: *Trie 第二个Trie树
//
// 返回值:
//   - *Trie 合并后的Trie树
func UnionAtDepth(depth int, left, right *Trie) *Trie {
	switch {
	case left.IsLeaf() && right.IsLeaf():
		switch {
		case left.IsEmpty() && right.IsEmpty():
			return &Trie{}
		case !left.IsEmpty() && right.IsEmpty():
			return &Trie{Key: left.Key}
		case left.IsEmpty() && !right.IsEmpty():
			return &Trie{Key: right.Key}
		case !left.IsEmpty() && !right.IsEmpty():
			u := &Trie{}
			u.AddAtDepth(depth, left.Key)
			u.AddAtDepth(depth, right.Key)
			return u
		}
	case !left.IsLeaf() && right.IsLeaf():
		return unionTrieAndLeaf(depth, left, right)
	case left.IsLeaf() && !right.IsLeaf():
		return unionTrieAndLeaf(depth, right, left)
	case !left.IsLeaf() && !right.IsLeaf():
		return &Trie{Branch: [2]*Trie{
			UnionAtDepth(depth+1, left.Branch[0], right.Branch[0]),
			UnionAtDepth(depth+1, left.Branch[1], right.Branch[1]),
		}}
	}
	panic("无法到达的代码")
}

// unionTrieAndLeaf 合并一个Trie树和一个叶子节点
// 参数:
//   - depth: int 当前深度
//   - trie: *Trie Trie树
//   - leaf: *Trie 叶子节点
//
// 返回值:
//   - *Trie 合并后的Trie树
func unionTrieAndLeaf(depth int, trie, leaf *Trie) *Trie {
	if leaf.IsEmpty() {
		return trie.Copy()
	} else {
		dir := leaf.Key.BitAt(depth)
		copy := &Trie{}
		copy.Branch[dir] = UnionAtDepth(depth+1, trie.Branch[dir], leaf)
		copy.Branch[1-dir] = trie.Branch[1-dir].Copy()
		return copy
	}
}
