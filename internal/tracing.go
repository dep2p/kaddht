package internal

import (
	"context"
	"fmt"
	"unicode/utf8"

	"github.com/multiformats/go-multibase"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// StartSpan 启动一个新的跟踪span
// 参数:
//   - ctx: context.Context 上下文
//   - name: string span名称
//   - opts: ...trace.SpanStartOption span启动选项
//
// 返回值:
//   - context.Context 新的上下文
//   - trace.Span 新创建的span
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer("dep2p-kad-dht").Start(ctx, fmt.Sprintf("KademliaDHT.%s", name), opts...)
}

// KeyAsAttribute 将DHT键格式化为合适的跟踪属性
// DHT键可以是有效的utf-8或二进制格式,例如当它们来自multihash时
// 跟踪(特别是OpenTelemetry+grpc导出器)要求字符串属性必须是有效的utf-8
// 参数:
//   - name: string 属性名称
//   - key: string DHT键值
//
// 返回值:
//   - attribute.KeyValue 格式化后的跟踪属性
func KeyAsAttribute(name string, key string) attribute.KeyValue {
	b := []byte(key)
	if utf8.Valid(b) {
		return attribute.String(name, key)
	}
	encoded, err := multibase.Encode(multibase.Base58BTC, b)
	if err != nil {
		// 不应该到达这里
		panic(err)
	}
	return attribute.String(name, encoded)
}
