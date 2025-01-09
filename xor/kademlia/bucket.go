package kademlia

import (
	"github.com/dep2p/kaddht/xor/key"
	"github.com/dep2p/kaddht/xor/trie"
)

// BucketAtDepth 返回路由表中指定深度的桶
// 参数:
//   - node: key.Key 节点ID
//   - table: *trie.Trie 路由表
//   - depth: int 深度
//
// 返回值:
//   - *trie.Trie 深度为D的桶,该桶包含与节点共享D位前缀的联系人
func BucketAtDepth(node key.Key, table *trie.Trie, depth int) *trie.Trie {
	dir := node.BitAt(depth)
	if table.IsLeaf() {
		return nil
	} else {
		if depth == 0 {
			return table.Branch[1-dir]
		} else {
			return BucketAtDepth(node, table.Branch[dir], depth-1)
		}
	}
}

// ClosestN 返回距离给定key最近的count个key
// 参数:
//   - node: key.Key 目标节点ID
//   - table: *trie.Trie 路由表
//   - count: int 需要返回的key数量
//
// 返回值:
//   - []key.Key 最近的key列表
func ClosestN(node key.Key, table *trie.Trie, count int) []key.Key {
	return closestAtDepth(node, table, 0, count, make([]key.Key, 0, count))
}

// closestAtDepth 在指定深度查找最近的key
// 参数:
//   - node: key.Key 目标节点ID
//   - table: *trie.Trie 路由表
//   - depth: int 当前深度
//   - count: int 需要返回的key数量
//   - found: []key.Key 已找到的key列表
//
// 返回值:
//   - []key.Key 最近的key列表
func closestAtDepth(node key.Key, table *trie.Trie, depth int, count int, found []key.Key) []key.Key {
	// 如果已经找到足够的节点,则终止
	if count == len(found) {
		return found
	}

	// 找到最近的方向
	dir := node.BitAt(depth)
	var chosenDir byte
	if table.Branch[dir] != nil {
		// 在"更近"的方向有节点
		chosenDir = dir
	} else if table.Branch[1-dir] != nil {
		// 在"较远"的方向有节点
		chosenDir = 1 - dir
	} else if table.Key != nil {
		// 找到了叶子节点
		return append(found, table.Key)
	} else {
		// 找到了空节点?
		return found
	}

	// 先从最近的方向添加节点,然后从另一个方向添加
	found = closestAtDepth(node, table.Branch[chosenDir], depth+1, count, found)
	found = closestAtDepth(node, table.Branch[1-chosenDir], depth+1, count, found)
	return found
}
