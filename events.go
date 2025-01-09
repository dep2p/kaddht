package dht

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/google/uuid"

	kbucket "github.com/dep2p/kaddht/kbucket"
	"github.com/dep2p/libp2p/core/peer"
)

// KeyKadID 包含字符串和二进制形式的Kademlia键
type KeyKadID struct {
	Key string
	Kad kbucket.ID
}

// NewKeyKadID 从字符串Kademlia ID创建KeyKadID
// 参数:
//   - k: string Kademlia ID字符串
//
// 返回值:
//   - *KeyKadID 创建的KeyKadID对象
func NewKeyKadID(k string) *KeyKadID {
	return &KeyKadID{
		Key: k,
		Kad: kbucket.ConvertKey(k),
	}
}

// PeerKadID 包含libp2p节点ID和二进制Kademlia ID
type PeerKadID struct {
	Peer peer.ID
	Kad  kbucket.ID
}

// NewPeerKadID 从libp2p节点ID创建PeerKadID
// 参数:
//   - p: peer.ID libp2p节点ID
//
// 返回值:
//   - *PeerKadID 创建的PeerKadID对象
func NewPeerKadID(p peer.ID) *PeerKadID {
	return &PeerKadID{
		Peer: p,
		Kad:  kbucket.ConvertPeerID(p),
	}
}

// NewPeerKadIDSlice 从libp2p节点ID切片创建PeerKadID切片
// 参数:
//   - p: []peer.ID libp2p节点ID切片
//
// 返回值:
//   - []*PeerKadID PeerKadID对象切片
func NewPeerKadIDSlice(p []peer.ID) []*PeerKadID {
	r := make([]*PeerKadID, len(p))
	for i := range p {
		r[i] = NewPeerKadID(p[i])
	}
	return r
}

// OptPeerKadID 如果传入的节点ID为默认值则返回nil,否则返回PeerKadID指针
// 参数:
//   - p: peer.ID libp2p节点ID
//
// 返回值:
//   - *PeerKadID PeerKadID对象指针或nil
func OptPeerKadID(p peer.ID) *PeerKadID {
	if p == "" {
		return nil
	}
	return NewPeerKadID(p)
}

// NewLookupEvent 创建一个LookupEvent,自动将节点libp2p ID转换为PeerKadID,将字符串Kademlia键转换为KeyKadID
// 参数:
//   - node: peer.ID 节点ID
//   - id: uuid.UUID 查找实例的唯一标识符
//   - key: string Kademlia键
//   - request: *LookupUpdateEvent 请求更新事件
//   - response: *LookupUpdateEvent 响应更新事件
//   - terminate: *LookupTerminateEvent 终止事件
//
// 返回值:
//   - *LookupEvent 创建的查找事件对象
func NewLookupEvent(
	node peer.ID,
	id uuid.UUID,
	key string,
	request *LookupUpdateEvent,
	response *LookupUpdateEvent,
	terminate *LookupTerminateEvent,
) *LookupEvent {
	return &LookupEvent{
		Node:      NewPeerKadID(node),
		ID:        id,
		Key:       NewKeyKadID(key),
		Request:   request,
		Response:  response,
		Terminate: terminate,
	}
}

// LookupEvent 在DHT查找过程中发生的每个重要事件时发出
// LookupEvent支持JSON序列化,因为它的所有字段都支持递归序列化
type LookupEvent struct {
	// Node 执行查找的节点ID
	Node *PeerKadID
	// ID 查找实例的唯一标识符
	ID uuid.UUID
	// Key 用作查找目标的Kademlia键
	Key *KeyKadID
	// Request 如果不为nil,描述与传出查询请求相关的状态更新事件
	Request *LookupUpdateEvent
	// Response 如果不为nil,描述与传出查询响应相关的状态更新事件
	Response *LookupUpdateEvent
	// Terminate 如果不为nil,描述终止事件
	Terminate *LookupTerminateEvent
}

// NewLookupUpdateEvent 创建新的查找更新事件,自动将传入的节点ID转换为PeerKadID
// 参数:
//   - cause: peer.ID 导致更新的节点ID
//   - source: peer.ID 信息来源节点ID
//   - heard: []peer.ID 已听到的节点ID列表
//   - waiting: []peer.ID 等待中的节点ID列表
//   - queried: []peer.ID 已查询的节点ID列表
//   - unreachable: []peer.ID 不可达的节点ID列表
//
// 返回值:
//   - *LookupUpdateEvent 创建的查找更新事件对象
func NewLookupUpdateEvent(
	cause peer.ID,
	source peer.ID,
	heard []peer.ID,
	waiting []peer.ID,
	queried []peer.ID,
	unreachable []peer.ID,
) *LookupUpdateEvent {
	return &LookupUpdateEvent{
		Cause:       OptPeerKadID(cause),
		Source:      OptPeerKadID(source),
		Heard:       NewPeerKadIDSlice(heard),
		Waiting:     NewPeerKadIDSlice(waiting),
		Queried:     NewPeerKadIDSlice(queried),
		Unreachable: NewPeerKadIDSlice(unreachable),
	}
}

// LookupUpdateEvent 描述查找状态更新事件
type LookupUpdateEvent struct {
	// Cause 是导致更新事件的节点(通过响应或无响应)
	// 如果Cause为nil,这是查找中由种子引起的第一个更新事件
	Cause *PeerKadID
	// Source 是告知我们此更新中节点ID的节点
	Source *PeerKadID
	// Heard 是一组节点,其在查找的节点集中的状态被设置为"已听到"
	Heard []*PeerKadID
	// Waiting 是一组节点,其在查找的节点集中的状态被设置为"等待中"
	Waiting []*PeerKadID
	// Queried 是一组节点,其在查找的节点集中的状态被设置为"已查询"
	Queried []*PeerKadID
	// Unreachable 是一组节点,其在查找的节点集中的状态被设置为"不可达"
	Unreachable []*PeerKadID
}

// LookupTerminateEvent 描述查找终止事件
type LookupTerminateEvent struct {
	// Reason 是查找终止的原因
	Reason LookupTerminationReason
}

// NewLookupTerminateEvent 使用给定原因创建新的查找终止事件
// 参数:
//   - reason: LookupTerminationReason 终止原因
//
// 返回值:
//   - *LookupTerminateEvent 创建的查找终止事件对象
func NewLookupTerminateEvent(reason LookupTerminationReason) *LookupTerminateEvent {
	return &LookupTerminateEvent{Reason: reason}
}

// LookupTerminationReason 捕获终止查找的原因
type LookupTerminationReason int

// MarshalJSON 返回查找终止原因的JSON编码
// 参数:
//
// 返回值:
//   - []byte JSON编码的字节数组
//   - error 错误信息
func (r LookupTerminationReason) MarshalJSON() ([]byte, error) {
	return json.Marshal(r.String())
}

// String 返回终止原因的字符串表示
// 参数:
//
// 返回值:
//   - string 终止原因的字符串表示
func (r LookupTerminationReason) String() string {
	switch r {
	case LookupStopped:
		return "stopped"
	case LookupCancelled:
		return "cancelled"
	case LookupStarvation:
		return "starvation"
	case LookupCompleted:
		return "completed"
	}
	panic("unreachable")
}

const (
	// LookupStopped 表示查找被用户的stopFn中止
	LookupStopped LookupTerminationReason = iota
	// LookupCancelled 表示查找被上下文中止
	LookupCancelled
	// LookupStarvation 表示由于缺少未查询的节点而终止查找
	LookupStarvation
	// LookupCompleted 表示查找成功终止,达到Kademlia结束条件
	LookupCompleted
)

type routingLookupKey struct{}

// TODO: lookupEventChannel复制了eventChannel的实现
// 这两者应该重构为使用通用的事件通道实现
// 通用实现需要重新思考RegisterForEvents的签名,因为返回类型化通道在不创建额外"适配器"通道的情况下无法实现多态
// 当Go引入泛型时这将更容易处理
type lookupEventChannel struct {
	mu  sync.Mutex
	ctx context.Context
	ch  chan<- *LookupEvent
}

// waitThenClose 在注册通道时以goroutine方式启动
// 当上下文被取消时,这会安全地清理通道
// 参数:
//
// 返回值:
func (e *lookupEventChannel) waitThenClose() {
	<-e.ctx.Done()
	e.mu.Lock()
	close(e.ch)
	// 1. 表示我们已完成
	// 2. 释放内存(以防我们最终长时间持有它)
	e.ch = nil
	e.mu.Unlock()
}

// send 在事件通道上发送事件,如果传入的或内部上下文过期则中止
// 参数:
//   - ctx: context.Context 上下文
//   - ev: *LookupEvent 要发送的事件
//
// 返回值:
func (e *lookupEventChannel) send(ctx context.Context, ev *LookupEvent) {
	e.mu.Lock()
	// 已关闭
	if e.ch == nil {
		e.mu.Unlock()
		return
	}
	// 如果传入的上下文无关,则等待两者
	select {
	case e.ch <- ev:
	case <-e.ctx.Done():
	case <-ctx.Done():
	}
	e.mu.Unlock()
}

// RegisterForLookupEvents 使用给定上下文注册查找事件通道
// 返回的上下文可以传递给DHT查询以在返回的通道上接收查找事件
//
// 当调用者不再对查询事件感兴趣时,必须取消传入的上下文
// 参数:
//   - ctx: context.Context 上下文
//
// 返回值:
//   - context.Context 包含事件通道的上下文
//   - <-chan *LookupEvent 查找事件通道
func RegisterForLookupEvents(ctx context.Context) (context.Context, <-chan *LookupEvent) {
	ch := make(chan *LookupEvent, LookupEventBufferSize)
	ech := &lookupEventChannel{ch: ch, ctx: ctx}
	go ech.waitThenClose()
	return context.WithValue(ctx, routingLookupKey{}, ech), ch
}

// LookupEventBufferSize 是要缓冲的事件数量
var LookupEventBufferSize = 16

// PublishLookupEvent 将查询事件发布到与给定上下文关联的查询事件通道(如果有)
// 参数:
//   - ctx: context.Context 上下文
//   - ev: *LookupEvent 要发布的事件
//
// 返回值:
func PublishLookupEvent(ctx context.Context, ev *LookupEvent) {
	ich := ctx.Value(routingLookupKey{})
	if ich == nil {
		return
	}

	// 我们希望在这里panic
	ech := ich.(*lookupEventChannel)
	ech.send(ctx, ev)
}
