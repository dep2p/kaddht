// tracing 为 [routing.Routing] API 提供高级方法追踪。
// API 的每个方法在 [Tracer] 上都有对应的方法,返回延迟包装回调或仅延迟回调。
package tracing

import (
	"context"
	"fmt"

	"github.com/dep2p/libp2p/core/peer"
	"github.com/dep2p/libp2p/core/routing"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multibase"
	"github.com/multiformats/go-multihash"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// Tracer 是将传递给 [otel.Tracer] 的库名
type Tracer string

// StartSpan 启动一个新的追踪span
// 参数:
//   - ctx: context.Context 上下文
//   - name: string span名称
//   - opts: ...trace.SpanStartOption span选项
//
// 返回值:
//   - context.Context 新的上下文
//   - trace.Span 新的span
func (t Tracer) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(string(t)).Start(ctx, name, opts...)
}

const base = multibase.Base64url

// bytesAsMultibase 将字节数组转换为multibase编码的字符串
// 参数:
//   - b: []byte 要编码的字节数组
//
// 返回值:
//   - string 编码后的字符串
func bytesAsMultibase(b []byte) string {
	r, err := multibase.Encode(base, b)
	if err != nil {
		panic(fmt.Errorf("不可能发生的错误: %w", err))
	}
	return r
}

// keysAsMultibase 避免返回非utf8字符串(otel不支持)
// 参数:
//   - name: string 属性名
//   - keys: []multihash.Multihash 要编码的键列表
//
// 返回值:
//   - attribute.KeyValue 编码后的键值对
func keysAsMultibase(name string, keys []multihash.Multihash) attribute.KeyValue {
	keysStr := make([]string, len(keys))
	for i, k := range keys {
		keysStr[i] = bytesAsMultibase(k)
	}
	return attribute.StringSlice(name, keysStr)
}

// Provide 追踪提供者操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 内容标识符
//   - announce: bool 是否公告
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) Provide(routerName string, ctx context.Context, key cid.Cid, announce bool) (_ context.Context, end func(error)) {
	return t.provide(routerName+".Provide", ctx, key, announce)
}

// provide 内部提供者追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 内容标识符
//   - announce: bool 是否公告
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) provide(traceName string, ctx context.Context, key cid.Cid, announce bool) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(
		attribute.Stringer("key", key),
		attribute.Bool("announce", announce),
	)

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// ProvideMany 追踪批量提供者操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - keys: []multihash.Multihash 要提供的键列表
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) ProvideMany(routerName string, ctx context.Context, keys []multihash.Multihash) (_ context.Context, end func(error)) {
	return t.provideMany(routerName+".ProvideMany", ctx, keys)
}

// provideMany 内部批量提供者追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - keys: []multihash.Multihash 要提供的键列表
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) provideMany(traceName string, ctx context.Context, keys []multihash.Multihash) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(keysAsMultibase("keys", keys))

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// peerInfoToAttributes 将对等节点信息转换为属性列表
// 参数:
//   - p: peer.AddrInfo 对等节点信息
//
// 返回值:
//   - []attribute.KeyValue 属性列表
func peerInfoToAttributes(p peer.AddrInfo) []attribute.KeyValue {
	strs := make([]string, len(p.Addrs))
	for i, v := range p.Addrs {
		strs[i] = v.String()
	}

	return []attribute.KeyValue{
		attribute.Stringer("id", p.ID),
		attribute.StringSlice("addrs", strs),
	}
}

// FindProvidersAsync 追踪异步查找提供者操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 要查找的内容标识符
//   - count: int 要查找的提供者数量
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo 结果通道转换函数
func (t Tracer) FindProvidersAsync(routerName string, ctx context.Context, key cid.Cid, count int) (_ context.Context, passthrough func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo) {
	return t.findProvidersAsync(routerName+".FindProvidersAsync", ctx, key, count)
}

// findProvidersAsync 内部异步查找提供者追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 要查找的内容标识符
//   - count: int 要查找的提供者数量
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo 结果通道转换函数
func (t Tracer) findProvidersAsync(traceName string, ctx context.Context, key cid.Cid, count int) (_ context.Context, passthrough func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(c <-chan peer.AddrInfo, _ error) <-chan peer.AddrInfo { return c }
	}

	span.SetAttributes(
		attribute.Stringer("key", key),
		attribute.Int("count", count),
	)

	return ctx, func(in <-chan peer.AddrInfo, err error) <-chan peer.AddrInfo {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return in
		}

		span.AddEvent("开始流式传输")

		out := make(chan peer.AddrInfo)
		go func() {
			defer span.End()
			defer close(out)

			for v := range in {
				span.AddEvent("找到提供者", trace.WithAttributes(peerInfoToAttributes(v)...))
				select {
				case out <- v:
				case <-ctx.Done():
					span.SetStatus(codes.Error, ctx.Err().Error())
				}
			}
		}()

		return out
	}
}

// FindPeer 追踪查找对等节点操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - id: peer.ID 要查找的对等节点ID
//
// 返回值:
//   - context.Context 新的上下文
//   - func(peer.AddrInfo, error) 结束回调函数
func (t Tracer) FindPeer(routerName string, ctx context.Context, id peer.ID) (_ context.Context, end func(peer.AddrInfo, error)) {
	return t.findPeer(routerName+".FindPeer", ctx, id)
}

// findPeer 内部查找对等节点追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - id: peer.ID 要查找的对等节点ID
//
// 返回值:
//   - context.Context 新的上下文
//   - func(peer.AddrInfo, error) 结束回调函数
func (t Tracer) findPeer(traceName string, ctx context.Context, id peer.ID) (_ context.Context, end func(peer.AddrInfo, error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(peer.AddrInfo, error) {}
	}

	span.SetAttributes(attribute.Stringer("key", id))

	return ctx, func(p peer.AddrInfo, err error) {
		defer span.End()

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return
		}

		span.AddEvent("找到对等节点", trace.WithAttributes(peerInfoToAttributes(p)...))
	}
}

// PutValue 追踪存储值操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) PutValue(routerName string, ctx context.Context, key string, val []byte, opts ...routing.Option) (_ context.Context, end func(error)) {
	return t.putValue(routerName+".PutValue", ctx, key, val, opts...)
}

// putValue 内部存储值追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) putValue(traceName string, ctx context.Context, key string, val []byte, opts ...routing.Option) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.String("value", bytesAsMultibase(val)),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}

// GetValue 追踪获取值操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func([]byte, error) 结束回调函数
func (t Tracer) GetValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, end func([]byte, error)) {
	return t.getValue(routerName+".GetValue", ctx, key, opts...)
}

// getValue 内部获取值追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func([]byte, error) 结束回调函数
func (t Tracer) getValue(traceName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, end func([]byte, error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func([]byte, error) {}
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(val []byte, err error) {
		defer span.End()

		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			return
		}

		span.AddEvent("找到值", trace.WithAttributes(
			attribute.String("value", bytesAsMultibase(val))))
	}
}

// SearchValue 追踪搜索值操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan []byte, error) (<-chan []byte, error) 结果通道转换函数
func (t Tracer) SearchValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, passthrough func(<-chan []byte, error) (<-chan []byte, error)) {
	return t.searchValue(routerName+".SearchValue", ctx, key, opts...)
}

// searchValue 内部搜索值追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 路由选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan []byte, error) (<-chan []byte, error) 结果通道转换函数
func (t Tracer) searchValue(traceName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, passthrough func(<-chan []byte, error) (<-chan []byte, error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(c <-chan []byte, err error) (<-chan []byte, error) { return c, err }
	}

	span.SetAttributes(
		attribute.String("key", bytesAsMultibase([]byte(key))),
		attribute.Int("len(opts)", len(opts)),
	)

	return ctx, func(in <-chan []byte, err error) (<-chan []byte, error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return in, err
		}

		span.AddEvent("开始流式传输")

		out := make(chan []byte)
		go func() {
			defer span.End()
			defer close(out)

			for v := range in {
				span.AddEvent("找到值", trace.WithAttributes(
					attribute.String("value", bytesAsMultibase(v))),
				)
				select {
				case out <- v:
				case <-ctx.Done():
					span.SetStatus(codes.Error, ctx.Err().Error())
				}
			}
		}()

		return out, nil
	}
}

// Bootstrap 追踪引导操作
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) Bootstrap(routerName string, ctx context.Context) (_ context.Context, end func(error)) {
	return t.bootstrap(routerName+".Bootstrap", ctx)
}

// bootstrap 内部引导追踪实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) bootstrap(traceName string, ctx context.Context) (_ context.Context, end func(error)) {
	ctx, span := t.StartSpan(ctx, traceName)
	if !span.IsRecording() {
		span.End()
		return ctx, func(error) {}
	}

	return ctx, func(err error) {
		if err != nil {
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
