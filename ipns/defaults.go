package ipns

import "time"

const (

	// DefaultRecordLifetime IPNS记录在ValidityType为0时的有效期
	// 此默认值旨在匹配Amino DHT的记录过期窗口
	DefaultRecordLifetime = 48 * time.Hour

	// DefaultRecordTTL 指定记录在再次检查更新之前可以从缓存返回的时间
	// 此TTL的功能类似于DNS记录的TTL,这里的默认值是在更快的更新和利用各种类型的缓存之间的权衡
	DefaultRecordTTL = 1 * time.Hour
)
