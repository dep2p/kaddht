package dht_pb

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	recpb "github.com/dep2p/kaddht/record/pb"
	"github.com/dep2p/libp2p/core/host"
	"github.com/dep2p/libp2p/core/peer"
	logging "github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"

	"github.com/dep2p/kaddht/internal"
)

var logger = logging.Logger("dht")

// ProtocolMessenger 用于向对等节点发送DHT消息并处理其响应。
// 这将协议格式与DHT协议实现和路由接口实现解耦。
//
// 注意: ProtocolMessenger的MessageSender仍需处理一些协议细节,如使用varint分隔的protobuf
type ProtocolMessenger struct {
	m MessageSender
}

// ProtocolMessengerOption ProtocolMessenger选项函数类型
type ProtocolMessengerOption func(*ProtocolMessenger) error

// NewProtocolMessenger 创建一个新的ProtocolMessenger用于向对等节点发送DHT消息并处理其响应
// 参数:
//   - msgSender: MessageSender 消息发送器
//   - opts: ...ProtocolMessengerOption 选项函数
//
// 返回值:
//   - *ProtocolMessenger 协议消息器
//   - error 错误信息
func NewProtocolMessenger(msgSender MessageSender, opts ...ProtocolMessengerOption) (*ProtocolMessenger, error) {
	pm := &ProtocolMessenger{
		m: msgSender,
	}

	for _, o := range opts {
		if err := o(pm); err != nil {
			return nil, err
		}
	}

	return pm, nil
}

// MessageSenderWithDisconnect 带断开连接处理的消息发送器接口
type MessageSenderWithDisconnect interface {
	MessageSender

	OnDisconnect(context.Context, peer.ID)
}

// MessageSender 处理向指定对等节点发送协议消息的接口
type MessageSender interface {
	// SendRequest 向对等节点发送消息并等待响应
	SendRequest(ctx context.Context, p peer.ID, pmes *Message) (*Message, error)
	// SendMessage 向对等节点发送消息但不等待响应
	SendMessage(ctx context.Context, p peer.ID, pmes *Message) error
}

// PutValue 请求对等节点存储给定的键值对
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//   - rec: *recpb.Record 记录
//
// 返回值:
//   - error 错误信息
func (pm *ProtocolMessenger) PutValue(ctx context.Context, p peer.ID, rec *recpb.Record) (err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.PutValue")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("record", rec))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
		}()
	}

	pmes := NewMessage(Message_PUT_VALUE, rec.Key, 0)
	pmes.Record = rec
	rpmes, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		logger.Debugw("向对等节点存储值失败", "to", p, "key", internal.LoggableRecordKeyBytes(rec.Key), "error", err)
		return err
	}

	if !bytes.Equal(rpmes.GetRecord().Value, pmes.GetRecord().Value) {
		const errStr = "值未正确存储"
		logger.Infow(errStr, "put-message", pmes, "get-message", rpmes)
		return errors.New(errStr)
	}

	return nil
}

// GetValue 向对等节点请求给定键对应的值。同时返回离键最近的K个对等节点,如GetClosestPeers所述
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//   - key: string 键
//
// 返回值:
//   - *recpb.Record 记录
//   - []*peer.AddrInfo 更近的对等节点
//   - error 错误信息
func (pm *ProtocolMessenger) GetValue(ctx context.Context, p peer.ID, key string) (record *recpb.Record, closerPeers []*peer.AddrInfo, err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.GetValue")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), internal.KeyAsAttribute("key", key))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			} else {
				peers := make([]string, len(closerPeers))
				for i, v := range closerPeers {
					peers[i] = v.String()
				}
				span.SetAttributes(
					attribute.Stringer("record", record),
					attribute.StringSlice("closestPeers", peers),
				)
			}
		}()
	}

	pmes := NewMessage(Message_GET_VALUE, []byte(key), 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}

	// 可能获得了更近的对等节点
	peers := PBPeersToPeerInfos(respMsg.GetCloserPeers())

	if rec := respMsg.GetRecord(); rec != nil {
		// 成功!我们获得了值
		logger.Debug("获得值")

		// 检查记录是否匹配我们要查找的记录(这里不进行记录验证)
		if !bytes.Equal([]byte(key), rec.GetKey()) {
			logger.Debug("收到错误的记录")
			return nil, nil, internal.ErrIncorrectRecord
		}

		return rec, peers, err
	}

	return nil, peers, nil
}

// GetClosestPeers 请求对等节点返回在XOR空间中离id最近的K个DHT服务器节点(K是DHT范围的参数)
// 注意:如果对等节点恰好知道一个peerID完全匹配给定id的对等节点,即使该节点不是DHT服务器节点也会返回
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//   - id: peer.ID 目标ID
//
// 返回值:
//   - []*peer.AddrInfo 更近的对等节点
//   - error 错误信息
func (pm *ProtocolMessenger) GetClosestPeers(ctx context.Context, p peer.ID, id peer.ID) (closerPeers []*peer.AddrInfo, err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.GetClosestPeers")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("key", id))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			} else {
				peers := make([]string, len(closerPeers))
				for i, v := range closerPeers {
					peers[i] = v.String()
				}
				span.SetAttributes(attribute.StringSlice("peers", peers))
			}
		}()
	}

	pmes := NewMessage(Message_FIND_NODE, []byte(id), 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, err
	}
	peers := PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return peers, nil
}

// PutProvider 已弃用:请使用[ProtocolMessenger.PutProviderAddrs]
func (pm *ProtocolMessenger) PutProvider(ctx context.Context, p peer.ID, key multihash.Multihash, h host.Host) error {
	return pm.PutProviderAddrs(ctx, p, key, peer.AddrInfo{
		ID:    h.ID(),
		Addrs: h.Addrs(),
	})
}

// PutProviderAddrs 请求对等节点存储我们是给定键的提供者
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//   - key: multihash.Multihash 键的多重哈希
//   - self: peer.AddrInfo 本节点地址信息
//
// 返回值:
//   - error 错误信息
func (pm *ProtocolMessenger) PutProviderAddrs(ctx context.Context, p peer.ID, key multihash.Multihash, self peer.AddrInfo) (err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.PutProvider")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("key", key))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
		}()
	}

	// TODO: 我们可能想限制提供者记录中的地址类型。例如,在仅WAN的DHT中禁止共享非WAN地址(如192.168.0.100)
	if len(self.Addrs) < 1 {
		return fmt.Errorf("没有已知的本地地址,无法设置提供者")
	}

	pmes := NewMessage(Message_ADD_PROVIDER, key, 0)
	pmes.ProviderPeers = RawPeerInfosToPBPeers([]peer.AddrInfo{self})

	return pm.m.SendMessage(ctx, p, pmes)
}

// GetProviders 向对等节点请求它所知道的给定键的提供者。同时返回离键最近的K个对等节点,如GetClosestPeers所述
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//   - key: multihash.Multihash 键的多重哈希
//
// 返回值:
//   - []*peer.AddrInfo 提供者列表
//   - []*peer.AddrInfo 更近的对等节点
//   - error 错误信息
func (pm *ProtocolMessenger) GetProviders(ctx context.Context, p peer.ID, key multihash.Multihash) (provs []*peer.AddrInfo, closerPeers []*peer.AddrInfo, err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.GetProviders")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p), attribute.Stringer("key", key))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			} else {
				provsStr := make([]string, len(provs))
				for i, v := range provs {
					provsStr[i] = v.String()
				}
				closerPeersStr := make([]string, len(provs))
				for i, v := range provs {
					closerPeersStr[i] = v.String()
				}
				span.SetAttributes(attribute.StringSlice("provs", provsStr), attribute.StringSlice("closestPeers", closerPeersStr))
			}
		}()
	}

	pmes := NewMessage(Message_GET_PROVIDERS, key, 0)
	respMsg, err := pm.m.SendRequest(ctx, p, pmes)
	if err != nil {
		return nil, nil, err
	}
	provs = PBPeersToPeerInfos(respMsg.GetProviderPeers())
	closerPeers = PBPeersToPeerInfos(respMsg.GetCloserPeers())
	return provs, closerPeers, nil
}

// Ping 向指定对等节点发送ping消息并等待响应
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - error 错误信息
func (pm *ProtocolMessenger) Ping(ctx context.Context, p peer.ID) (err error) {
	ctx, span := internal.StartSpan(ctx, "ProtocolMessenger.Ping")
	defer span.End()
	if span.IsRecording() {
		span.SetAttributes(attribute.Stringer("to", p))
		defer func() {
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
			}
		}()
	}

	req := NewMessage(Message_PING, nil, 0)
	resp, err := pm.m.SendRequest(ctx, p, req)
	if err != nil {
		return fmt.Errorf("发送请求: %w", err)
	}
	if resp.Type != Message_PING {
		return fmt.Errorf("收到意外的响应类型: %v", resp.Type)
	}
	return nil
}
