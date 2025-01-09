package record

import (
	pb "github.com/dep2p/kaddht/record/pb"
)

// MakePutRecord 为给定的键值对创建一个DHT记录
// 参数:
//   - key: string 键名
//   - value: []byte 值
//
// 返回值:
//   - *pb.Record DHT记录
func MakePutRecord(key string, value []byte) *pb.Record {
	record := new(pb.Record)
	record.Key = []byte(key)
	record.Value = value
	return record
}
