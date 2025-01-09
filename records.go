package dht

import (
	"context"
	"fmt"

	"github.com/dep2p/kaddht/internal"
	ci "github.com/dep2p/libp2p/core/crypto"
	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type pubkrs struct {
	pubk ci.PubKey
	err  error
}

// GetPublicKey 根据对等节点ID获取公钥
// 如果公钥内联在对等节点ID中,则直接提取;否则向节点本身或DHT查询
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (dht *IpfsDHT) GetPublicKey(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	ctx, span := internal.StartSpan(ctx, "IpfsDHT.GetPublicKey", trace.WithAttributes(attribute.Stringer("PeerID", p)))
	defer span.End()

	if !dht.enableValues {
		return nil, routing.ErrNotSupported
	}

	logger.Debugf("正在获取公钥: %s", p)

	// 本地检查。如果可能的话也会尝试从对等节点ID本身提取公钥(如果是内联的)
	pk := dht.peerstore.PubKey(p)
	if pk != nil {
		return pk, nil
	}

	// 并行从节点本身和DHT获取公钥
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	resp := make(chan pubkrs, 2)
	go func() {
		pubk, err := dht.getPublicKeyFromNode(ctx, p)
		resp <- pubkrs{pubk, err}
	}()

	// 注意:由于拨号限制器限制了打开的连接数,因此getPublicKeyFromDHT()可能会打开大量连接,
	// 从而阻止getPublicKeyFromNode()获取连接。目前这似乎不会造成问题,暂时保持现状。
	go func() {
		pubk, err := dht.getPublicKeyFromDHT(ctx, p)
		resp <- pubkrs{pubk, err}
	}()

	// 等待其中一个goroutine返回公钥(或两个都出错)
	var err error
	for i := 0; i < 2; i++ {
		r := <-resp
		if r.err == nil {
			// 找到公钥
			err := dht.peerstore.AddPubKey(p, r.pubk)
			if err != nil {
				logger.Errorw("添加公钥到peerstore失败", "peer", p)
			}
			return r.pubk, nil
		}
		err = r.err
	}

	// 两个goroutine都未能找到公钥
	return nil, err
}

// getPublicKeyFromDHT 从DHT获取公钥
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (dht *IpfsDHT) getPublicKeyFromDHT(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	// 只检索一个值,因为公钥是不可变的,所以不需要检索多个版本
	pkkey := routing.KeyForPublicKey(p)
	val, err := dht.GetValue(ctx, pkkey, Quorum(1))
	if err != nil {
		return nil, err
	}

	pubk, err := ci.UnmarshalPublicKey(val)
	if err != nil {
		logger.Errorf("无法解析从DHT获取的%v的公钥", p)
		return nil, err
	}

	// 注意:不需要检查公钥哈希是否匹配对等节点ID,因为这已经在GetValues()中完成
	logger.Debugf("从DHT获取到%s的公钥", p)
	return pubk, nil
}

// getPublicKeyFromNode 从节点本身获取公钥
// 参数:
//   - ctx: context.Context 上下文
//   - p: peer.ID 对等节点ID
//
// 返回值:
//   - ci.PubKey 公钥
//   - error 错误信息
func (dht *IpfsDHT) getPublicKeyFromNode(ctx context.Context, p peer.ID) (ci.PubKey, error) {
	// 以防万一再次本地检查...
	pk := dht.peerstore.PubKey(p)
	if pk != nil {
		return pk, nil
	}

	// 从节点本身获取密钥
	pkkey := routing.KeyForPublicKey(p)
	record, _, err := dht.protoMessenger.GetValue(ctx, p, pkkey)
	if err != nil {
		return nil, err
	}

	// 节点没有密钥 :(
	if record == nil {
		return nil, fmt.Errorf("节点%v未响应其公钥", p)
	}

	pubk, err := ci.UnmarshalPublicKey(record.GetValue())
	if err != nil {
		logger.Errorf("无法解析%v的公钥", p)
		return nil, err
	}

	// 确保公钥匹配对等节点ID
	id, err := peer.IDFromPublicKey(pubk)
	if err != nil {
		logger.Errorf("无法从%v的公钥提取对等节点ID", p)
		return nil, err
	}
	if id != p {
		return nil, fmt.Errorf("公钥%v与对等节点%v不匹配", id, p)
	}

	logger.Debugf("从节点%v本身获取到公钥", p)
	return pubk, nil
}
