package dht

import (
	"fmt"

	"github.com/dep2p/libp2p/core/event"
	"github.com/dep2p/libp2p/core/network"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/p2p/host/eventbus"
)

// startNetworkSubscriber 启动网络事件订阅器
// 返回值:
//   - error: 如果订阅失败则返回错误
func (dht *IpfsDHT) startNetworkSubscriber() error {
	// 设置事件总线缓冲区大小为256
	bufSize := eventbus.BufSize(256)

	evts := []interface{}{
		// 注册事件总线通知,当对等点成功完成身份识别时更新路由表
		new(event.EvtPeerIdentificationCompleted),

		// 注册事件总线协议ID更改以更新路由表
		new(event.EvtPeerProtocolsUpdated),

		// 注册事件总线通知,当本地地址发生变化时通知网络
		new(event.EvtLocalAddressesUpdated),

		// 注册与其他对等点断开连接的事件
		new(event.EvtPeerConnectednessChanged),
	}

	// 仅当DHT在ModeAuto模式下运行时,注册事件总线本地可路由性更改以触发客户端和服务器模式之间的切换
	if dht.auto == ModeAuto || dht.auto == ModeAutoServer {
		evts = append(evts, new(event.EvtLocalReachabilityChanged))
	}

	subs, err := dht.host.EventBus().Subscribe(evts, bufSize)
	if err != nil {
		return fmt.Errorf("DHT无法订阅事件总线事件: %w", err)
	}

	dht.wg.Add(1)
	go func() {
		defer dht.wg.Done()
		defer subs.Close()

		for {
			select {
			case e, more := <-subs.Out():
				if !more {
					return
				}

				switch evt := e.(type) {
				case event.EvtLocalAddressesUpdated:
					// 当我们的地址发生变化时,我们应该主动告诉最近的对等点,以便快速被发现。
					// Identify协议会将带有我们新地址的签名对等记录推送给所有已连接的对等点。
					// 但是,我们可能不一定连接到最近的对等点,因此按照禅宗的真正精神,
					// 在网络中寻找自己确实是与那些重要的对等点建立连接的最佳方式。
					if dht.autoRefresh || dht.testAddressUpdateProcessing {
						dht.rtRefreshManager.RefreshNoWait()
					}
				case event.EvtPeerProtocolsUpdated:
					handlePeerChangeEvent(dht, evt.Peer)
				case event.EvtPeerIdentificationCompleted:
					handlePeerChangeEvent(dht, evt.Peer)
				case event.EvtPeerConnectednessChanged:
					if evt.Connectedness != network.Connected {
						dht.msgSender.OnDisconnect(dht.ctx, evt.Peer)
					}
				case event.EvtLocalReachabilityChanged:
					if dht.auto == ModeAuto || dht.auto == ModeAutoServer {
						handleLocalReachabilityChangedEvent(dht, evt)
					} else {
						// 如果收到未订阅的事件,说明出现严重错误
						logger.Errorf("收到未订阅的本地可达性变更事件")
					}
				default:
					// 如果收到其他类型的事件,说明出现严重错误
					logger.Errorf("从订阅中获得了错误的类型: %T", e)
				}
			case <-dht.ctx.Done():
				return
			}
		}
	}()

	return nil
}

// handlePeerChangeEvent 处理对等点变更事件
// 参数:
//   - dht: DHT实例
//   - p: 对等点ID
func handlePeerChangeEvent(dht *IpfsDHT, p peer.ID) {
	valid, err := dht.validRTPeer(p)
	if err != nil {
		logger.Errorf("无法检查对等存储中的协议支持: 错误: %s", err)
		return
	} else if valid {
		dht.peerFound(p)
	} else {
		dht.peerStoppedDHT(p)
	}
}

// handleLocalReachabilityChangedEvent 处理本地可达性变更事件
// 参数:
//   - dht: DHT实例
//   - e: 本地可达性变更事件
func handleLocalReachabilityChangedEvent(dht *IpfsDHT, e event.EvtLocalReachabilityChanged) {
	var target mode

	switch e.Reachability {
	case network.ReachabilityPrivate:
		target = modeClient
	case network.ReachabilityUnknown:
		if dht.auto == ModeAutoServer {
			target = modeServer
		} else {
			target = modeClient
		}
	case network.ReachabilityPublic:
		target = modeServer
	}

	logger.Infof("已处理事件 %T; 正在执行DHT模式切换", e)

	err := dht.setMode(target)
	// 注意: 模式将以十进制形式打印
	if err == nil {
		logger.Infow("DHT模式切换成功", "mode", target)
	} else {
		logger.Errorw("DHT模式切换失败", "mode", target, "error", err)
	}
}

// validRTPeer 检查对等点是否支持DHT协议
// 支持DHT协议意味着支持主要协议,我们不希望将使用过时次要协议的对等点添加到路由表中
// 参数:
//   - p: 对等点ID
//
// 返回值:
//   - bool: 如果对等点支持DHT协议则返回true,否则返回false
//   - error: 如果检查过程中出现错误则返回错误
func (dht *IpfsDHT) validRTPeer(p peer.ID) (bool, error) {
	b, err := dht.peerstore.FirstSupportedProtocol(p, dht.protocols...)
	if len(b) == 0 || err != nil {
		return false, err
	}

	return dht.routingTablePeerFilter == nil || dht.routingTablePeerFilter(dht, p), nil
}
