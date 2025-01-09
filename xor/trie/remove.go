package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// Remove 从Trie中删除key
// TODO: 实现一个不可变版本的Remove
// 参数:
//   - q: key.Key 要删除的key
//
// 返回值:
//   - removedDepth: int 删除操作到达的深度
//   - removed: bool 是否成功删除
func (trie *Trie) Remove(q key.Key) (removedDepth int, removed bool) {
	return trie.RemoveAtDepth(0, q)
}

// RemoveAtDepth 在指定深度删除key
// 参数:
//   - depth: int 当前深度
//   - q: key.Key 要删除的key
//
// 返回值:
//   - reachedDepth: int 到达的深度
//   - removed: bool 是否成功删除
func (trie *Trie) RemoveAtDepth(depth int, q key.Key) (reachedDepth int, removed bool) {
	switch {
	case trie.IsEmptyLeaf():
		return depth, false
	case trie.IsNonEmptyLeaf():
		trie.Key = nil
		return depth, true
	default:
		if d, removed := trie.Branch[q.BitAt(depth)].RemoveAtDepth(depth+1, q); removed {
			trie.shrink()
			return d, true
		} else {
			return d, false
		}
	}
}

// Remove 从Trie中删除key并返回新的Trie
// 参数:
//   - trie: *Trie 原始Trie
//   - q: key.Key 要删除的key
//
// 返回值:
//   - *Trie 删除key后的新Trie
func Remove(trie *Trie, q key.Key) *Trie {
	return RemoveAtDepth(0, trie, q)
}

// RemoveAtDepth 在指定深度删除key并返回新的Trie
// 参数:
//   - depth: int 当前深度
//   - trie: *Trie 原始Trie
//   - q: key.Key 要删除的key
//
// 返回值:
//   - *Trie 删除key后的新Trie
func RemoveAtDepth(depth int, trie *Trie, q key.Key) *Trie {
	switch {
	case trie.IsEmptyLeaf():
		return trie
	case trie.IsNonEmptyLeaf() && !key.Equal(trie.Key, q):
		return trie
	case trie.IsNonEmptyLeaf() && key.Equal(trie.Key, q):
		return &Trie{}
	default:
		dir := q.BitAt(depth)
		afterDelete := RemoveAtDepth(depth+1, trie.Branch[dir], q)
		if afterDelete == trie.Branch[dir] {
			return trie
		}
		copy := &Trie{}
		copy.Branch[dir] = afterDelete
		copy.Branch[1-dir] = trie.Branch[1-dir]
		copy.shrink()
		return copy
	}
}
