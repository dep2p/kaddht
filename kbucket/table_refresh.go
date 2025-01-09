package kbucket

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dep2p/libp2p/core/peer"

	mh "github.com/multiformats/go-multihash"
)

// maxCplForRefresh 是我们支持刷新的最大公共前缀长度
// 这个限制存在是因为我们目前只能生成 'maxCplForRefresh' 位前缀
const maxCplForRefresh uint = 15

// GetTrackedCplsForRefresh 返回我们正在跟踪刷新的公共前缀长度
// 参数:
//   - 无
//
// 返回值:
//   - []time.Time 公共前缀长度对应的刷新时间列表
//
// 注意: 调用者可以自由修改返回的切片,因为这是一个防御性副本
func (rt *RoutingTable) GetTrackedCplsForRefresh() []time.Time {
	maxCommonPrefix := rt.maxCommonPrefix()
	if maxCommonPrefix > maxCplForRefresh {
		maxCommonPrefix = maxCplForRefresh
	}

	rt.cplRefreshLk.RLock()
	defer rt.cplRefreshLk.RUnlock()

	cpls := make([]time.Time, maxCommonPrefix+1)
	for i := uint(0); i <= maxCommonPrefix; i++ {
		// 如果我们还没有刷新它,则默认为零值
		cpls[i] = rt.cplRefreshedAt[i]
	}
	return cpls
}

// randUint16 生成一个随机的16位无符号整数
// 参数:
//   - 无
//
// 返回值:
//   - uint16 随机生成的16位无符号整数
//   - error 错误信息
func randUint16() (uint16, error) {
	// 读取一个随机前缀
	var prefixBytes [2]byte
	_, err := rand.Read(prefixBytes[:])
	return binary.BigEndian.Uint16(prefixBytes[:]), err
}

// GenRandPeerID 为给定的公共前缀长度生成一个随机对等节点ID
// 参数:
//   - targetCpl: uint 目标公共前缀长度
//
// 返回值:
//   - peer.ID 生成的对等节点ID
//   - error 错误信息
func (rt *RoutingTable) GenRandPeerID(targetCpl uint) (peer.ID, error) {
	if targetCpl > maxCplForRefresh {
		return "", fmt.Errorf("无法为大于 %d 的公共前缀长度生成对等节点ID", maxCplForRefresh)
	}

	localPrefix := binary.BigEndian.Uint16(rt.local)

	// 对于具有ID 'L'的主机,ID 'K'属于ID为'B'的桶,当且仅当 CommonPrefixLen(L,K) 恰好等于 B
	// 因此,要达到目标前缀'T',我们必须在L中翻转第(T+1)位,然后将L的(T+1)位复制到我们随机生成的前缀中
	toggledLocalPrefix := localPrefix ^ (uint16(0x8000) >> targetCpl)
	randPrefix, err := randUint16()
	if err != nil {
		return "", err
	}

	// 在正确的偏移处组合翻转的本地前缀和随机位,使得只有前 targetCpl 位与本地ID匹配
	mask := (^uint16(0)) << (16 - (targetCpl + 1))
	targetPrefix := (toggledLocalPrefix & mask) | (randPrefix & ^mask)

	// 转换为已知的对等节点ID
	key := keyPrefixMap[targetPrefix]
	id := [32 + 2]byte{mh.SHA2_256, 32}
	binary.BigEndian.PutUint32(id[2:], key)
	return peer.ID(id[:]), nil
}

// GenRandomKey 生成一个与本地标识符具有指定公共前缀长度(Cpl)的随机密钥
// 参数:
//   - targetCpl: uint 目标公共前缀长度
//
// 返回值:
//   - ID 生成的密钥
//   - error 错误信息
//
// 注意: 返回的密钥与本地密钥的前 targetCpl 位匹配,第 targetCpl+1 位是本地密钥该位的反转,其余位是随机生成的
func (rt *RoutingTable) GenRandomKey(targetCpl uint) (ID, error) {
	if int(targetCpl+1) >= len(rt.local)*8 {
		return nil, fmt.Errorf("无法为大于密钥长度的公共前缀长度生成对等节点ID")
	}
	partialOffset := targetCpl / 8

	// output 包含本地密钥的前 partialOffset 字节,其余字节是随机的
	output := make([]byte, len(rt.local))
	copy(output, rt.local[:partialOffset])
	_, err := rand.Read(output[partialOffset:])
	if err != nil {
		return nil, err
	}

	remainingBits := 8 - targetCpl%8
	orig := rt.local[partialOffset]

	origMask := ^uint8(0) << remainingBits
	randMask := ^origMask >> 1
	flippedBitOffset := remainingBits - 1
	flippedBitMask := uint8(1) << flippedBitOffset

	// 恢复 orig 的 remainingBits 个最高有效位,并翻转 orig 的第 flippedBitOffset 位
	output[partialOffset] = orig&origMask | (orig & flippedBitMask) ^ flippedBitMask | output[partialOffset]&randMask

	return ID(output), nil
}

// ResetCplRefreshedAtForID 重置给定ID的公共前缀长度的刷新时间
// 参数:
//   - id: ID 对等节点ID
//   - newTime: time.Time 新的刷新时间
//
// 返回值:
//   - 无
func (rt *RoutingTable) ResetCplRefreshedAtForID(id ID, newTime time.Time) {
	cpl := CommonPrefixLen(id, rt.local)
	if uint(cpl) > maxCplForRefresh {
		return
	}

	rt.cplRefreshLk.Lock()
	defer rt.cplRefreshLk.Unlock()

	rt.cplRefreshedAt[uint(cpl)] = newTime
}
