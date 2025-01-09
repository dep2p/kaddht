package trie

import (
	"github.com/dep2p/kaddht/xor/key"
)

// InvariantDiscrepancy 不变量差异
type InvariantDiscrepancy struct {
	// Reason 差异原因
	Reason string
	// PathToDiscrepancy 差异路径
	PathToDiscrepancy string
	// KeyAtDiscrepancy 差异处的键值
	KeyAtDiscrepancy string
}

// CheckInvariant 检查Trie是否满足不变量
// 返回值:
//   - *InvariantDiscrepancy 不变量差异信息
func (trie *Trie) CheckInvariant() *InvariantDiscrepancy {
	return trie.checkInvariant(0, nil)
}

// checkInvariant 检查Trie是否满足不变量
// 参数:
//   - depth: int 当前深度
//   - pathSoFar: *triePath 当前路径
//
// 返回值:
//   - *InvariantDiscrepancy 不变量差异信息
func (trie *Trie) checkInvariant(depth int, pathSoFar *triePath) *InvariantDiscrepancy {
	switch {
	case trie.IsEmptyLeaf():
		return nil
	case trie.IsNonEmptyLeaf():
		if !pathSoFar.matchesKey(trie.Key) {
			return &InvariantDiscrepancy{
				Reason:            "在Trie中发现键值位于无效位置",
				PathToDiscrepancy: pathSoFar.BitString(),
				KeyAtDiscrepancy:  trie.Key.BitString(),
			}
		}
		return nil
	default:
		if trie.IsEmpty() {
			b0, b1 := trie.Branch[0], trie.Branch[1]
			if d0 := b0.checkInvariant(depth+1, pathSoFar.Push(0)); d0 != nil {
				return d0
			}
			if d1 := b1.checkInvariant(depth+1, pathSoFar.Push(1)); d1 != nil {
				return d1
			}
			switch {
			case b0.IsEmptyLeaf() && b1.IsEmptyLeaf():
				return &InvariantDiscrepancy{
					Reason:            "中间节点包含两个空叶子节点",
					PathToDiscrepancy: pathSoFar.BitString(),
					KeyAtDiscrepancy:  "无",
				}
			case b0.IsEmptyLeaf() && b1.IsNonEmptyLeaf():
				return &InvariantDiscrepancy{
					Reason:            "中间节点包含一个空叶子节点/0",
					PathToDiscrepancy: pathSoFar.BitString(),
					KeyAtDiscrepancy:  "无",
				}
			case b0.IsNonEmptyLeaf() && b1.IsEmptyLeaf():
				return &InvariantDiscrepancy{
					Reason:            "中间节点包含一个空叶子节点/1",
					PathToDiscrepancy: pathSoFar.BitString(),
					KeyAtDiscrepancy:  "无",
				}
			default:
				return nil
			}
		} else {
			return &InvariantDiscrepancy{
				Reason:            "中间节点包含键值",
				PathToDiscrepancy: pathSoFar.BitString(),
				KeyAtDiscrepancy:  trie.Key.BitString(),
			}
		}
	}
}

// triePath Trie路径结构
type triePath struct {
	parent *triePath
	bit    byte
}

// Push 向路径添加一个位
// 参数:
//   - bit: byte 要添加的位值
//
// 返回值:
//   - *triePath 新的路径
func (p *triePath) Push(bit byte) *triePath {
	return &triePath{parent: p, bit: bit}
}

// RootPath 获取到根节点的路径
// 返回值:
//   - []byte 路径字节切片
func (p *triePath) RootPath() []byte {
	if p == nil {
		return nil
	} else {
		return append(p.parent.RootPath(), p.bit)
	}
}

// matchesKey 检查路径是否匹配键值
// 参数:
//   - k: key.Key 要匹配的键值
//
// 返回值:
//   - bool 是否匹配
func (p *triePath) matchesKey(k key.Key) bool {
	// 较慢但更明确的实现:
	// for i, b := range p.RootPath() {
	// 	if k.BitAt(i) != b {
	// 		return false
	// 	}
	// }
	// return true
	ok, _ := p.walk(k, 0)
	return ok
}

// walk 遍历路径检查键值匹配
// 参数:
//   - k: key.Key 要匹配的键值
//   - depthToLeaf: int 到叶子节点的深度
//
// 返回值:
//   - bool 是否匹配
//   - int 到根节点的深度
func (p *triePath) walk(k key.Key, depthToLeaf int) (ok bool, depthToRoot int) {
	if p == nil {
		return true, 0
	} else {
		parOk, parDepthToRoot := p.parent.walk(k, depthToLeaf+1)
		return k.BitAt(parDepthToRoot) == p.bit && parOk, parDepthToRoot + 1
	}
}

// BitString 获取路径的位字符串表示
// 返回值:
//   - string 位字符串
func (p *triePath) BitString() string {
	return p.bitString(0)
}

// bitString 获取路径的位字符串表示
// 参数:
//   - depthToLeaf: int 到叶子节点的深度
//
// 返回值:
//   - string 位字符串
func (p *triePath) bitString(depthToLeaf int) string {
	if p == nil {
		return ""
	} else {
		switch {
		case p.bit == 0:
			return p.parent.bitString(depthToLeaf+1) + "0"
		case p.bit == 1:
			return p.parent.bitString(depthToLeaf+1) + "1"
		default:
			panic("位数字大于1")
		}
	}
}
