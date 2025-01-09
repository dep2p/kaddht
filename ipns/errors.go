package ipns

import (
	"errors"
)

// MaxRecordSize 是IPNS记录的[大小限制]
//
// [大小限制]: https://specs.ipfs.tech/ipns/ipns-record/#record-size-limit
const MaxRecordSize int = 10 << (10 * 1)

// ErrExpiredRecord 当IPNS[记录]因过期而无效时返回此错误
var ErrExpiredRecord = errors.New("记录已过期")

// ErrUnrecognizedValidity 当IPNS[记录]包含未知的有效性类型时返回此错误
var ErrUnrecognizedValidity = errors.New("记录包含无法识别的有效性类型")

// ErrInvalidValidity 当IPNS[记录]具有已知的有效性类型但值无效时返回此错误
var ErrInvalidValidity = errors.New("记录包含无效的有效性")

// ErrRecordSize 当IPNS[记录]超过最大大小时返回此错误
var ErrRecordSize = errors.New("记录超过允许的大小限制")

// ErrDataMissing 当IPNS[记录]缺少数据字段时返回此错误
var ErrDataMissing = errors.New("记录缺少dag-cbor数据字段")

// ErrInvalidRecord 当IPNS[记录]格式错误时返回此错误
var ErrInvalidRecord = errors.New("记录格式错误")

// ErrPublicKeyMismatch 当IPNS[记录]中嵌入的公钥与预期的公钥不匹配时返回此错误
var ErrPublicKeyMismatch = errors.New("记录公钥与预期的公钥不匹配")

// ErrPublicKeyNotFound 当找不到公钥时返回此错误
var ErrPublicKeyNotFound = errors.New("找不到公钥")

// ErrInvalidPublicKey 当IPNS[记录]具有无效的公钥时返回此错误
var ErrInvalidPublicKey = errors.New("公钥无效")

// ErrSignature 当IPNS[记录]签名验证失败时返回此错误
var ErrSignature = errors.New("签名验证失败")

// ErrInvalidName 当IPNS[名称]无效时返回此错误
var ErrInvalidName = errors.New("名称无效")

// ErrInvalidPath 当IPNS[记录]具有无效路径时返回此错误
var ErrInvalidPath = errors.New("值不是有效的内容路径")
