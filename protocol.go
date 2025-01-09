package dht

import (
	"github.com/dep2p/kaddht/amino"
	"github.com/dep2p/libp2p/core/protocol"
)

var (
	// ProtocolDHT 默认的DHT协议
	// 参数:
	//   - protocol.ID DHT协议标识符
	ProtocolDHT protocol.ID = amino.ProtocolID

	// DefaultProtocols DHT支持的默认协议列表
	// 参数:
	//   - []protocol.ID 协议标识符列表
	DefaultProtocols = amino.Protocols
)
