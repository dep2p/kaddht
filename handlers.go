package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dep2p/libp2p/core/peer"
	pstore "github.com/dep2p/libp2p/p2p/host/peerstore"

	"github.com/dep2p/kaddht/internal"
	pb "github.com/dep2p/kaddht/pb"
	recpb "github.com/dep2p/kaddht/record/pb"
	"github.com/gogo/protobuf/proto"
	u "github.com/ipfs/boxo/util"
	ds "github.com/ipfs/go-datastore"
	"github.com/multiformats/go-base32"
)

// dhtHandler 指定处理DHT消息的函数签名
type dhtHandler func(context.Context, peer.ID, *pb.Message) (*pb.Message, error)

// handlerForMsgType 根据消息类型返回对应的处理函数
// 参数:
//   - t: pb.Message_MessageType 消息类型
//
// 返回值:
//   - dhtHandler 处理函数
func (dht *IpfsDHT) handlerForMsgType(t pb.Message_MessageType) dhtHandler {
	switch t {
	case pb.Message_FIND_NODE:
		return dht.handleFindPeer
	case pb.Message_PING:
		return dht.handlePing
	}

	if dht.enableValues {
		switch t {
		case pb.Message_GET_VALUE:
			return dht.handleGetValue
		case pb.Message_PUT_VALUE:
			return dht.handlePutValue
		}
	}

	if dht.enableProviders {
		switch t {
		case pb.Message_ADD_PROVIDER:
			return dht.handleAddProvider
		case pb.Message_GET_PROVIDERS:
			return dht.handleGetProviders
		}
	}

	return nil
}

// handleGetValue 处理获取值的请求
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 请求节点ID
//   - pmes: *pb.Message 请求消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (dht *IpfsDHT) handleGetValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	// 首先检查是否有key
	k := pmes.GetKey()
	if len(k) == 0 {
		return nil, errors.New("处理获取值请求时未提供key")
	}

	// 设置响应
	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	rec, err := dht.checkLocalDatastore(ctx, k)
	if err != nil {
		return nil, err
	}
	resp.Record = rec

	// 在给定集群中找到离目标key最近的节点并回复该信息
	closer := dht.betterPeersToQuery(pmes, p, dht.bucketSize)
	if len(closer) > 0 {
		// TODO: pstore.PeerInfos 应该移到核心库(=> peerstore.AddrInfos)
		closerinfos := pstore.PeerInfos(dht.peerstore, closer)
		for _, pi := range closerinfos {
			logger.Debugf("handleGetValue 返回更近的节点: '%s'", pi.ID)
			if len(pi.Addrs) < 1 {
				logger.Warnw("发送的节点没有地址",
					"local", dht.self,
					"to", p,
					"sending", pi.ID,
				)
			}
		}

		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), closerinfos)
	}

	return resp, nil
}

// checkLocalDatastore 检查本地数据存储
// 参数:
//   - ctx: context.Context 上下文
//   - k: []byte 键值
//
// 返回值:
//   - *recpb.Record 记录
//   - error 错误信息
func (dht *IpfsDHT) checkLocalDatastore(ctx context.Context, k []byte) (*recpb.Record, error) {
	logger.Debugf("%s handleGetValue 查询数据存储", dht.self)
	dskey := convertToDsKey(k)
	buf, err := dht.datastore.Get(ctx, dskey)
	logger.Debugf("%s handleGetValue 从数据存储获取到 %v", dht.self, buf)

	if err == ds.ErrNotFound {
		return nil, nil
	}

	// 如果遇到意外错误,则退出
	if err != nil {
		return nil, err
	}

	// 如果我们有值,则返回
	logger.Debugf("%s handleGetValue 成功!", dht.self)

	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		logger.Debug("从数据存储解析DHT记录失败")
		return nil, err
	}

	var recordIsBad bool
	recvtime, err := u.ParseRFC3339(rec.GetTimeReceived())
	if err != nil {
		logger.Info("记录没有设置接收时间,或者时间无效: ", err)
		recordIsBad = true
	}

	if time.Since(recvtime) > dht.maxRecordAge {
		logger.Debug("发现过期记录,丢弃")
		recordIsBad = true
	}

	// 注意:我们在这里不验证记录,只检查这些时间戳。
	// 我们将验证记录的责任放在请求者身上,因为验证记录可能需要大量计算

	if recordIsBad {
		err := dht.datastore.Delete(ctx, dskey)
		if err != nil {
			logger.Error("从数据存储删除无效记录失败: ", err)
		}

		return nil, nil // 可以将其视为完全没有记录
	}

	return rec, nil
}

// cleanRecord 清理记录(避免存储任意数据)
// 参数:
//   - rec: *recpb.Record 要清理的记录
func cleanRecord(rec *recpb.Record) {
	rec.TimeReceived = ""
}

// handlePutValue 在本地存储中存储值
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 请求节点ID
//   - pmes: *pb.Message 请求消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (dht *IpfsDHT) handlePutValue(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, err error) {
	if len(pmes.GetKey()) == 0 {
		return nil, errors.New("处理获取值请求时未提供key")
	}

	rec := pmes.GetRecord()
	if rec == nil {
		logger.Debugw("收到空记录", "from", p)
		return nil, errors.New("记录为空")
	}

	if !bytes.Equal(pmes.GetKey(), rec.GetKey()) {
		return nil, errors.New("put的key与记录的key不匹配")
	}

	cleanRecord(rec)

	// 确保记录有效(未过期、签名有效等)
	if err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue()); err != nil {
		logger.Infow("PUT中的DHT记录无效", "from", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()), "error", err)
		return nil, err
	}

	dskey := convertToDsKey(rec.GetKey())

	// 获取该key的条带锁
	var indexForLock byte
	if len(rec.GetKey()) == 0 {
		indexForLock = 0
	} else {
		indexForLock = rec.GetKey()[len(rec.GetKey())-1]
	}
	lk := &dht.stripedPutLocks[indexForLock]
	lk.Lock()
	defer lk.Unlock()

	// 确保新记录比我们本地的记录"更好"。
	// 这可以防止序列号较低的记录覆盖序列号较高的记录。
	existing, err := dht.getRecordFromDatastore(ctx, dskey)
	if err != nil {
		return nil, err
	}

	if existing != nil {
		recs := [][]byte{rec.GetValue(), existing.GetValue()}
		i, err := dht.Validator.Select(string(rec.GetKey()), recs)
		if err != nil {
			logger.Warnw("DHT记录通过验证但选择失败", "from", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()), "error", err)
			return nil, err
		}
		if i != 0 {
			logger.Infow("PUT中的DHT记录比现有记录旧(忽略)", "peer", p, "key", internal.LoggableRecordKeyBytes(rec.GetKey()))
			return nil, errors.New("记录过期")
		}
	}

	// 记录我们接收每条记录的时间
	rec.TimeReceived = u.FormatRFC3339(time.Now())

	data, err := proto.Marshal(rec)
	if err != nil {
		return nil, err
	}

	err = dht.datastore.Put(ctx, dskey, data)
	return pmes, err
}

// getRecordFromDatastore 从数据存储获取记录
// 参数:
//   - ctx: context.Context 上下文
//   - dskey: ds.Key 数据存储键值
//
// 返回值:
//   - *recpb.Record 记录,当未找到记录或记录验证失败时返回nil
//   - error 数据存储错误
func (dht *IpfsDHT) getRecordFromDatastore(ctx context.Context, dskey ds.Key) (*recpb.Record, error) {
	buf, err := dht.datastore.Get(ctx, dskey)
	if err == ds.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		logger.Errorw("从数据存储获取记录出错", "key", dskey, "error", err)
		return nil, err
	}
	rec := new(recpb.Record)
	err = proto.Unmarshal(buf, rec)
	if err != nil {
		// 数据存储中的数据无效,记录日志但不返回错误,我们将覆盖它
		logger.Errorw("从数据存储解析记录失败", "key", dskey, "error", err)
		return nil, nil
	}

	err = dht.Validator.Validate(string(rec.GetKey()), rec.GetValue())
	if err != nil {
		// 数据存储中的记录无效,可能已过期,但不返回错误,我们将覆盖它
		logger.Debugw("本地记录验证失败", "key", rec.GetKey(), "error", err)
		return nil, nil
	}

	return rec, nil
}

// handlePing 处理ping请求
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 请求节点ID
//   - pmes: *pb.Message 请求消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (dht *IpfsDHT) handlePing(_ context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
	logger.Debugf("%s 响应来自 %s 的ping!\n", dht.self, p)
	return pmes, nil
}

// handleFindPeer 处理查找节点请求
// 参数:
//   - ctx: context.Context 上下文
//   - from: peer.ID 请求节点ID
//   - pmes: *pb.Message 请求消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (dht *IpfsDHT) handleFindPeer(ctx context.Context, from peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	resp := pb.NewMessage(pmes.GetType(), nil, pmes.GetClusterLevel())
	var closest []peer.ID

	if len(pmes.GetKey()) == 0 {
		return nil, fmt.Errorf("处理查找节点请求时key为空")
	}

	// 如果查找自己...特殊情况下我们在CloserPeers中发送它
	targetPid := peer.ID(pmes.GetKey())
	closest = dht.betterPeersToQuery(pmes, from, dht.bucketSize)

	// 永远不要告诉节点关于它自己的信息
	if targetPid != from {
		// 如果目标节点不在我们的路由表中,将其添加到最近节点集合中
		//
		// 稍后,当我们查找此集合中所有节点的已知地址时,如果我们实际上不知道它在哪里,我们将删除此节点
		found := false
		for _, p := range closest {
			if targetPid == p {
				found = true
				break
			}
		}
		if !found {
			closest = append(closest, targetPid)
		}
	}

	if closest == nil {
		return resp, nil
	}

	// TODO: pstore.PeerInfos 应该移到核心库(=> peerstore.AddrInfos)
	closestinfos := pstore.PeerInfos(dht.peerstore, closest)
	// 可能是过度分配但这个数组反正是临时的
	withAddresses := make([]peer.AddrInfo, 0, len(closestinfos))
	for _, pi := range closestinfos {
		if len(pi.Addrs) > 0 {
			withAddresses = append(withAddresses, pi)
		}
	}

	resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), withAddresses)
	return resp, nil
}

// handleGetProviders 处理获取提供者请求
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 请求节点ID
//   - pmes: *pb.Message 请求消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (dht *IpfsDHT) handleGetProviders(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, fmt.Errorf("处理获取提供者请求时key太大")
	} else if len(key) == 0 {
		return nil, fmt.Errorf("处理获取提供者请求时key为空")
	}

	resp := pb.NewMessage(pmes.GetType(), pmes.GetKey(), pmes.GetClusterLevel())

	// 设置提供者
	providers, err := dht.providerStore.GetProviders(ctx, key)
	if err != nil {
		return nil, err
	}

	filtered := make([]peer.AddrInfo, len(providers))
	for i, provider := range providers {
		filtered[i] = peer.AddrInfo{
			ID:    provider.ID,
			Addrs: dht.filterAddrs(provider.Addrs),
		}
	}

	resp.ProviderPeers = pb.PeerInfosToPBPeers(dht.host.Network(), filtered)

	// 同时发送更近的节点
	closer := dht.betterPeersToQuery(pmes, p, dht.bucketSize)
	if closer != nil {
		// TODO: pstore.PeerInfos 应该移到核心库(=> peerstore.AddrInfos)
		infos := pstore.PeerInfos(dht.peerstore, closer)
		resp.CloserPeers = pb.PeerInfosToPBPeers(dht.host.Network(), infos)
	}

	return resp, nil
}

// handleAddProvider 处理添加提供者请求
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 请求节点ID
//   - pmes: *pb.Message 请求消息
//
// 返回值:
//   - *pb.Message 响应消息
//   - error 错误信息
func (dht *IpfsDHT) handleAddProvider(ctx context.Context, p peer.ID, pmes *pb.Message) (_ *pb.Message, _err error) {
	key := pmes.GetKey()
	if len(key) > 80 {
		return nil, fmt.Errorf("处理添加提供者请求时key太大")
	} else if len(key) == 0 {
		return nil, fmt.Errorf("处理添加提供者请求时key为空")
	}

	logger.Debugw("添加提供者", "from", p, "key", internal.LoggableProviderRecordBytes(key))

	// 添加提供者应该使用消息中给出的地址
	pinfos := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
	for _, pi := range pinfos {
		if pi.ID != p {
			// 我们应该忽略这个提供者记录!不是来自发起者。
			// (我们应该签名并稍后检查签名...)
			logger.Debugw("收到来自错误节点的提供者", "from", p, "peer", pi.ID)
			continue
		}

		if len(pi.Addrs) < 1 {
			logger.Debugw("提供者没有有效地址", "from", p)
			continue
		}

		// 我们在检查长度后运行地址过滤器,这允许具有不同/p2p-circuit地址的临时节点仍然可以通过其公告
		addrs := dht.filterAddrs(pi.Addrs)
		dht.providerStore.AddProvider(ctx, key, peer.AddrInfo{ID: pi.ID, Addrs: addrs})
	}

	return nil, nil
}

// convertToDsKey 将字节切片转换为数据存储键值
// 参数:
//   - s: []byte 要转换的字节切片
//
// 返回值:
//   - ds.Key 数据存储键值
func convertToDsKey(s []byte) ds.Key {
	return ds.NewKey(base32.RawStdEncoding.EncodeToString(s))
}
