// tracing 为 [routing.Routing] API 提供高级方法追踪。
// API 的每个方法在 [Tracer] 上都有对应的方法，返回延迟包装回调或仅延迟回调。
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

// Tracer 是将传递给 [otel.Tracer] 的库名。
type Tracer string

// StartSpan 启动一个新的追踪 span
// 参数:
//   - ctx: context.Context 上下文
//   - name: string span 名称
//   - opts: ...trace.SpanStartOption span 选项
//
// 返回值:
//   - context.Context 新的上下文
//   - trace.Span 新的 span
func (t Tracer) StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(string(t)).Start(ctx, name, opts...)
}

const base = multibase.Base64url

// bytesAsMultibase 将字节数组转换为 multibase 编码的字符串
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

// keysAsMultibase 避免返回 otel 不喜欢的非 utf8 字符
// 参数:
//   - name: string 属性名称
//   - keys: []multihash.Multihash 要编码的键列表
//
// 返回值:
//   - attribute.KeyValue 属性键值对
func keysAsMultibase(name string, keys []multihash.Multihash) attribute.KeyValue {
	keysStr := make([]string, len(keys))
	for i, k := range keys {
		keysStr[i] = bytesAsMultibase(k)
	}
	return attribute.StringSlice(name, keysStr)
}

// Provide 提供一个 CID
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 要提供的 CID
//   - announce: bool 是否公告
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) Provide(routerName string, ctx context.Context, key cid.Cid, announce bool) (_ context.Context, end func(error)) {
	// 轮廓以便在编译时折叠连接
	return t.provide(routerName+".Provide", ctx, key, announce)
}

// provide 内部提供实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 要提供的 CID
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

// ProvideMany 提供多个 Multihash
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - keys: []multihash.Multihash 要提供的 Multihash 列表
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) ProvideMany(routerName string, ctx context.Context, keys []multihash.Multihash) (_ context.Context, end func(error)) {
	// 轮廓以便在编译时折叠连接
	return t.provideMany(routerName+".ProvideMany", ctx, keys)
}

// provideMany 内部提供多个实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - keys: []multihash.Multihash 要提供的 Multihash 列表
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

// peerInfoToAttributes 将对等点信息转换为属性列表
// 参数:
//   - p: peer.AddrInfo 对等点信息
//
// 返回值:
//   - []attribute.KeyValue 属性键值对列表
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

// FindProvidersAsync 异步查找提供者
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 要查找的 CID
//   - count: int 最大返回数量
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo 处理函数
func (t Tracer) FindProvidersAsync(routerName string, ctx context.Context, key cid.Cid, count int) (_ context.Context, passthrough func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo) {
	// 轮廓以便在编译时折叠连接
	return t.findProvidersAsync(routerName+".FindProvidersAsync", ctx, key, count)
}

// findProvidersAsync 内部异步查找提供者实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: cid.Cid 要查找的 CID
//   - count: int 最大返回数量
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan peer.AddrInfo, error) <-chan peer.AddrInfo 处理函数
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

// FindPeer 查找对等点
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - id: peer.ID 要查找的对等点 ID
//
// 返回值:
//   - context.Context 新的上下文
//   - func(peer.AddrInfo, error) 结束回调函数
func (t Tracer) FindPeer(routerName string, ctx context.Context, id peer.ID) (_ context.Context, end func(peer.AddrInfo, error)) {
	// 轮廓以便在编译时折叠连接
	return t.findPeer(routerName+".FindPeer", ctx, id)
}

// findPeer 内部查找对等点实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - id: peer.ID 要查找的对等点 ID
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

		span.AddEvent("找到对等点", trace.WithAttributes(peerInfoToAttributes(p)...))
	}
}

// PutValue 存储值
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) PutValue(routerName string, ctx context.Context, key string, val []byte, opts ...routing.Option) (_ context.Context, end func(error)) {
	// 轮廓以便在编译时折叠连接
	return t.putValue(routerName+".PutValue", ctx, key, val, opts...)
}

// putValue 内部存储值实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - val: []byte 值
//   - opts: ...routing.Option 选项
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

// GetValue 获取值
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func([]byte, error) 结束回调函数
func (t Tracer) GetValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, end func([]byte, error)) {
	// 轮廓以便在编译时折叠连接
	return t.getValue(routerName+".GetValue", ctx, key, opts...)
}

// getValue 内部获取值实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
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

// SearchValue 搜索值
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan []byte, error) (<-chan []byte, error) 处理函数
func (t Tracer) SearchValue(routerName string, ctx context.Context, key string, opts ...routing.Option) (_ context.Context, passthrough func(<-chan []byte, error) (<-chan []byte, error)) {
	// 轮廓以便在编译时折叠连接
	return t.searchValue(routerName+".SearchValue", ctx, key, opts...)
}

// searchValue 内部搜索值实现
// 参数:
//   - traceName: string 追踪名称
//   - ctx: context.Context 上下文
//   - key: string 键
//   - opts: ...routing.Option 选项
//
// 返回值:
//   - context.Context 新的上下文
//   - func(<-chan []byte, error) (<-chan []byte, error) 处理函数
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

// Bootstrap 引导
// 参数:
//   - routerName: string 路由器名称
//   - ctx: context.Context 上下文
//
// 返回值:
//   - context.Context 新的上下文
//   - func(error) 结束回调函数
func (t Tracer) Bootstrap(routerName string, ctx context.Context) (_ context.Context, end func(error)) {
	// 轮廓以便在编译时折叠连接
	return t.bootstrap(routerName+".Bootstrap", ctx)
}

// bootstrap 内部引导实现
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
