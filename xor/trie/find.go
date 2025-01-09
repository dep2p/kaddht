package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// Find 在trie中查找key
// 参数:
//   - q: key.Key 要查找的key
//
// 返回值:
//   - reachedDepth: int 到达叶子节点的深度,无论是否找到key
//   - found: bool 是否找到key
func (trie *Trie) Find(q key.Key) (reachedDepth int, found bool) {
	return trie.FindAtDepth(0, q)
}

// FindAtDepth 在指定深度查找key
// 参数:
//   - depth: int 深度
//   - q: key.Key 要查找的key
//
// 返回值:
//   - reachedDepth: int 到达叶子节点的深度
//   - found: bool 是否找到key
func (trie *Trie) FindAtDepth(depth int, q key.Key) (reachedDepth int, found bool) {
	switch {
	case trie.IsEmptyLeaf():
		return depth, false
	case trie.IsNonEmptyLeaf():
		return depth, key.Equal(trie.Key, q)
	default:
		return trie.Branch[q.BitAt(depth)].FindAtDepth(depth+1, q)
	}
}
