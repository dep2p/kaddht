package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// List 返回Trie中所有键值的列表
// 返回值:
//   - []key.Key Trie中所有键值的切片
func (trie *Trie) List() []key.Key {
	switch {
	case trie.IsEmptyLeaf():
		return nil
	case trie.IsNonEmptyLeaf():
		return []key.Key{trie.Key}
	default:
		return append(trie.Branch[0].List(), trie.Branch[1].List()...)
	}
}
