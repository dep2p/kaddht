package kademlia

import (
	"encoding/json"
	"sort"

	"github.com/dep2p/kaddht/xor/key"
	"github.com/dep2p/kaddht/xor/trie"
)

// TableHealthReport 描述节点路由表与理论理想状态的差异,
// 基于对网络中所有节点的了解。
// TODO: 以易于在Python/matplotlib中使用的方式打印,方便在Jupyter notebook中查看。
// 例如,希望看到所有表格中(理想深度 - 实际深度)的直方图。
type TableHealthReport struct {
	// IdealDepth 是节点路由表应该具有的深度
	IdealDepth int
	// ActualDepth 是节点路由表实际的深度
	ActualDepth int
	// Bucket 包含节点每个路由桶的健康报告
	Bucket []*BucketHealthReport
}

// String 返回健康报告的JSON字符串表示
// 返回值:
//   - string JSON字符串
func (th *TableHealthReport) String() string {
	b, _ := json.Marshal(th)
	return string(b)
}

// BucketHealth 描述节点路由桶与理论理想状态的差异,
// 基于对网络中所有节点("已知"节点)的了解。
type BucketHealthReport struct {
	// Depth 是桶的深度,从零开始
	Depth int
	// MaxKnownContacts 是所有已知网络节点中,
	// 有资格在此桶中的节点数量
	MaxKnownContacts int
	// ActualKnownContacts 是实际在节点路由表中的
	// 已知网络节点数量
	ActualKnownContacts int
	// ActualUnknownContacts 是节点路由表中当前
	// 未知的联系人数量
	ActualUnknownContacts int
}

// String 返回桶健康报告的JSON字符串表示
// 返回值:
//   - string JSON字符串
func (bh *BucketHealthReport) String() string {
	b, _ := json.Marshal(bh)
	return string(b)
}

// sortedBucketHealthReport 按深度升序排序桶健康报告
type sortedBucketHealthReport []*BucketHealthReport

func (s sortedBucketHealthReport) Less(i, j int) bool {
	return s[i].Depth < s[j].Depth
}

func (s sortedBucketHealthReport) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s sortedBucketHealthReport) Len() int {
	return len(s)
}

// Table 表示路由表
type Table struct {
	Node     key.Key   // 节点ID
	Contacts []key.Key // 联系人列表
}

// AllTablesHealth 计算给定路由联系人的节点网络的健康报告
// 参数:
//   - tables: []*Table 路由表列表
//
// 返回值:
//   - []*TableHealthReport 健康报告列表
func AllTablesHealth(tables []*Table) (report []*TableHealthReport) {
	// 构建全局网络视图trie
	knownNodes := trie.New()
	for _, table := range tables {
		knownNodes.Add(table.Node)
	}
	// 计算单个表的健康状况
	for _, table := range tables {
		report = append(report, TableHealth(table.Node, table.Contacts, knownNodes))
	}
	return
}

// TableHealthFromSets 从节点集合计算表的健康报告
// 参数:
//   - node: key.Key 节点ID
//   - nodeContacts: []key.Key 节点联系人列表
//   - knownNodes: []key.Key 已知节点列表
//
// 返回值:
//   - *TableHealthReport 表健康报告
func TableHealthFromSets(node key.Key, nodeContacts []key.Key, knownNodes []key.Key) *TableHealthReport {
	knownNodesTrie := trie.New()
	for _, k := range knownNodes {
		knownNodesTrie.Add(k)
	}
	return TableHealth(node, nodeContacts, knownNodesTrie)
}

// TableHealth 计算节点的健康报告,
// 基于其路由联系人和当前网络中所有已知节点的列表
// 参数:
//   - node: key.Key 节点ID
//   - nodeContacts: []key.Key 节点联系人列表
//   - knownNodes: *trie.Trie 已知节点trie
//
// 返回值:
//   - *TableHealthReport 表健康报告
func TableHealth(node key.Key, nodeContacts []key.Key, knownNodes *trie.Trie) *TableHealthReport {
	// 重建节点的路由表为trie
	nodeTable := trie.New()
	nodeTable.Add(node)
	for _, u := range nodeContacts {
		nodeTable.Add(u)
	}
	// 计算健康报告
	idealDepth, _ := knownNodes.Find(node)
	actualDepth, _ := nodeTable.Find(node)
	return &TableHealthReport{
		IdealDepth:  idealDepth,
		ActualDepth: actualDepth,
		Bucket:      BucketHealth(node, nodeTable, knownNodes),
	}
}

// BucketHealth 计算节点路由表中每个桶的健康报告,
// 基于节点的路由表和当前网络中所有已知节点的列表
// 参数:
//   - node: key.Key 节点ID
//   - nodeTable: *trie.Trie 节点路由表
//   - knownNodes: *trie.Trie 已知节点trie
//
// 返回值:
//   - []*BucketHealthReport 桶健康报告列表
func BucketHealth(node key.Key, nodeTable, knownNodes *trie.Trie) []*BucketHealthReport {
	r := walkBucketHealth(0, node, nodeTable, knownNodes)
	sort.Sort(sortedBucketHealthReport(r))
	return r
}

// walkBucketHealth 遍历计算桶的健康报告
// 参数:
//   - depth: int 深度
//   - node: key.Key 节点ID
//   - nodeTable: *trie.Trie 节点路由表
//   - knownNodes: *trie.Trie 已知节点trie
//
// 返回值:
//   - []*BucketHealthReport 桶健康报告列表
func walkBucketHealth(depth int, node key.Key, nodeTable, knownNodes *trie.Trie) []*BucketHealthReport {
	if nodeTable.IsLeaf() {
		return nil
	} else {
		dir := node.BitAt(depth)
		switch {
		//
		case knownNodes == nil || knownNodes.IsEmptyLeaf():
			r := walkBucketHealth(depth+1, node, nodeTable.Branch[dir], nil)
			return append(r,
				&BucketHealthReport{
					Depth:                 depth,
					MaxKnownContacts:      0,
					ActualKnownContacts:   0,
					ActualUnknownContacts: nodeTable.Branch[1-dir].Size(),
				})
		case knownNodes.IsNonEmptyLeaf():
			if knownNodes.Key.BitAt(depth) == dir {
				r := walkBucketHealth(depth+1, node, nodeTable.Branch[dir], knownNodes)
				return append(r,
					&BucketHealthReport{
						Depth:                 depth,
						MaxKnownContacts:      0,
						ActualKnownContacts:   0,
						ActualUnknownContacts: nodeTable.Branch[1-dir].Size(),
					})
			} else {
				r := walkBucketHealth(depth+1, node, nodeTable.Branch[dir], nil)
				return append(r, bucketReportFromTries(depth+1, nodeTable.Branch[1-dir], knownNodes))
			}
		case !knownNodes.IsLeaf():
			r := walkBucketHealth(depth+1, node, nodeTable.Branch[dir], knownNodes.Branch[dir])
			return append(r,
				bucketReportFromTries(depth+1, nodeTable.Branch[1-dir], knownNodes.Branch[1-dir]))
		default:
			panic("不可能到达的代码")
		}
	}
}

// bucketReportFromTries 从tries生成桶健康报告
// 参数:
//   - depth: int 深度
//   - actualBucket: *trie.Trie 实际桶
//   - maxBucket: *trie.Trie 最大桶
//
// 返回值:
//   - *BucketHealthReport 桶健康报告
func bucketReportFromTries(depth int, actualBucket, maxBucket *trie.Trie) *BucketHealthReport {
	actualKnown := trie.IntersectAtDepth(depth, actualBucket, maxBucket)
	actualKnownSize := actualKnown.Size()
	return &BucketHealthReport{
		Depth:                 depth,
		MaxKnownContacts:      maxBucket.Size(),
		ActualKnownContacts:   actualKnownSize,
		ActualUnknownContacts: actualBucket.Size() - actualKnownSize,
	}
}
