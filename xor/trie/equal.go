package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// Equal 比较两个Trie是否相等
// 参数:
//   - p: *Trie 第一个Trie
//   - q: *Trie 第二个Trie
//
// 返回值:
//   - bool 是否相等
func Equal(p, q *Trie) bool {
	switch {
	case p.IsLeaf() && q.IsLeaf():
		return key.Equal(p.Key, q.Key)
	case !p.IsLeaf() && !q.IsLeaf():
		return Equal(p.Branch[0], q.Branch[0]) && Equal(p.Branch[1], q.Branch[1])
	}
	return false
}
