package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// Add 将key添加到trie中。Add会修改trie。
// 参数:
//   - q: key.Key 要添加的key
//
// 返回值:
//   - insertedDepth: int 插入深度
//   - insertedOK: bool 是否成功插入
func (trie *Trie) Add(q key.Key) (insertedDepth int, insertedOK bool) {
	return trie.AddAtDepth(0, q)
}

// AddAtDepth 在指定深度将key添加到trie中
// 参数:
//   - depth: int 深度
//   - q: key.Key 要添加的key
//
// 返回值:
//   - insertedDepth: int 插入深度
//   - insertedOK: bool 是否成功插入
func (trie *Trie) AddAtDepth(depth int, q key.Key) (insertedDepth int, insertedOK bool) {
	switch {
	case trie.IsEmptyLeaf():
		trie.Key = q
		return depth, true
	case trie.IsNonEmptyLeaf():
		if key.Equal(trie.Key, q) {
			return depth, false
		} else {
			p := trie.Key
			trie.Key = nil
			// 两个分支都为nil
			trie.Branch[0], trie.Branch[1] = &Trie{}, &Trie{}
			trie.Branch[p.BitAt(depth)].Key = p
			return trie.Branch[q.BitAt(depth)].AddAtDepth(depth+1, q)
		}
	default:
		return trie.Branch[q.BitAt(depth)].AddAtDepth(depth+1, q)
	}
}

// Add 将key添加到trie中,返回一个新的trie。
// Add是不可变/非破坏性的:原始trie保持不变。
// 参数:
//   - trie: *Trie 原始trie
//   - q: key.Key 要添加的key
//
// 返回值:
//   - *Trie 添加key后的新trie
func Add(trie *Trie, q key.Key) *Trie {
	return AddAtDepth(0, trie, q)
}

// AddAtDepth 在指定深度将key添加到trie中,返回一个新的trie
// 参数:
//   - depth: int 深度
//   - trie: *Trie 原始trie
//   - q: key.Key 要添加的key
//
// 返回值:
//   - *Trie 添加key后的新trie
func AddAtDepth(depth int, trie *Trie, q key.Key) *Trie {
	switch {
	case trie.IsEmptyLeaf():
		return &Trie{Key: q}
	case trie.IsNonEmptyLeaf():
		if key.Equal(trie.Key, q) {
			return trie
		} else {
			return trieForTwo(depth, trie.Key, q)
		}
	default:
		dir := q.BitAt(depth)
		s := &Trie{}
		s.Branch[dir] = AddAtDepth(depth+1, trie.Branch[dir], q)
		s.Branch[1-dir] = trie.Branch[1-dir]
		return s
	}
}

// trieForTwo 为两个key创建一个新的trie
// 参数:
//   - depth: int 深度
//   - p: key.Key 第一个key
//   - q: key.Key 第二个key
//
// 返回值:
//   - *Trie 包含两个key的新trie
func trieForTwo(depth int, p, q key.Key) *Trie {
	pDir, qDir := p.BitAt(depth), q.BitAt(depth)
	if qDir == pDir {
		s := &Trie{}
		s.Branch[pDir] = trieForTwo(depth+1, p, q)
		s.Branch[1-pDir] = &Trie{}
		return s
	} else {
		s := &Trie{}
		s.Branch[pDir] = &Trie{Key: p}
		s.Branch[qDir] = &Trie{Key: q}
		return s
	}
}
