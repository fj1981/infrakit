package cydist

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/fj1981/infrakit/pkg/cyutil"
	cache "github.com/mgtv-tech/jetcache-go"
	"github.com/mgtv-tech/jetcache-go/local"
)

// CacheStats holds statistics about cache operations
type CacheStats struct {
	Hits          int64
	Misses        int64
	Errors        int64
	CacheAttempts int64
}

func (s CacheStats) String() string {
	total := s.Hits + s.Misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(s.Hits) / float64(total) * 100
	}
	return fmt.Sprintf("Hits: %d, Misses: %d, Errors: %d, Attempts: %d, Hit Rate: %.2f%%",
		s.Hits, s.Misses, s.Errors, s.CacheAttempts, hitRate)
}

// CacheFuncOptions for the wrapped function
type CacheFuncOptions struct {
	TTL           time.Duration
	KeyPrefix     string
	Key           string
	KeyGenerator  func(ctx context.Context, args ...interface{}) string
	InternalCache cache.Cache
	RedisClient   *RedisClient
	Disabled      bool
}

func (c *CacheFuncOptions) Clone() *CacheFuncOptions {
	return &CacheFuncOptions{
		TTL:           c.TTL,
		KeyPrefix:     c.KeyPrefix,
		Key:           c.Key,
		KeyGenerator:  c.KeyGenerator,
		InternalCache: c.InternalCache,
		RedisClient:   c.RedisClient,
		Disabled:      c.Disabled,
	}
}

// CacheFuncOption is a functional option for CacheFuncOptions
type CacheFuncOption func(*CacheFuncOptions)

func WithTTL(ttl time.Duration) CacheFuncOption {
	return func(o *CacheFuncOptions) { o.TTL = ttl }
}

func WithKeyPrefix(prefix string) CacheFuncOption {
	return func(o *CacheFuncOptions) { o.KeyPrefix = prefix }
}

func WithKey(key string) CacheFuncOption {
	return func(o *CacheFuncOptions) { o.Key = key }
}

func WithKeyGenerator(gen func(ctx context.Context, args ...interface{}) string) CacheFuncOption {
	return func(o *CacheFuncOptions) { o.KeyGenerator = gen }
}

func WithRedisClient(client *RedisClient) CacheFuncOption {
	return func(o *CacheFuncOptions) { o.RedisClient = client }
}

func WithDisabled(disabled bool) CacheFuncOption {
	return func(o *CacheFuncOptions) { o.Disabled = disabled }
}

func DefaultCacheFuncOptions() *CacheFuncOptions {
	return &CacheFuncOptions{
		TTL:       5 * time.Minute,
		KeyPrefix: "func:",
		Disabled:  false,
	}
}

type baseKey struct {
	baseKeys []string
	cacheKey string
	ttl      time.Duration
}

// CacheWrapper provides a way to wrap functions with caching capabilities
type CacheWrapper struct {
	options     *CacheFuncOptions
	ch          cache.Cache
	stats       CacheStats
	basekeyChan chan baseKey
	cancelFunc  context.CancelFunc
}

func NewCacheWrapper(opts ...CacheFuncOption) *CacheWrapper {
	opt := DefaultCacheFuncOptions()
	for _, o := range opts {
		o(opt)
	}
	var ch cache.Cache
	if opt.InternalCache == nil {
		if opt.RedisClient == nil {
			ch = cache.New(cache.WithLocal(local.NewFreeCache(100*local.MB, opt.TTL)))
		} else {
			ch = cache.New(cache.WithLocal(local.NewFreeCache(100*local.MB, opt.TTL)),
				cache.WithRemote(opt.RedisClient),
				cache.WithRefreshDuration(opt.TTL))
		}
	} else {
		ch = opt.InternalCache
	}

	// 使用可取消的上下文
	ctx, cancel := context.WithCancel(context.Background())

	w := &CacheWrapper{
		options:     opt,
		ch:          ch,
		basekeyChan: make(chan baseKey, 100),
		cancelFunc:  cancel,
	}
	go w.updateBaseKey(ctx)
	return w
}

func (w *CacheWrapper) updateBaseKey(ctx context.Context) {
	for {
		select {
		case baseKey, ok := <-w.basekeyChan:
			if !ok {
				// Channel closed, exit goroutine
				return
			}
			if err := w.setBaseKeyIndex(ctx, baseKey.baseKeys, baseKey.cacheKey, baseKey.ttl); err != nil {
				// Handle or log error
				// 可以根据实际需求添加日志记录
				// log.Printf("Error setting base key index: %v", err)
			}
		case <-ctx.Done():
			// Context canceled, exit goroutine
			return
		}
	}
}

func (w *CacheWrapper) Set(ctx context.Context, key string, value any, opts ...CacheFuncOption) error {
	opt := w.options.Clone()
	for _, o := range opts {
		o(opt)
	}
	return w.ch.Set(ctx, key, cache.Value(value), cache.TTL(opt.TTL))
}

func (w *CacheWrapper) Get(ctx context.Context, key string, value any) error {
	return w.ch.Get(ctx, key, value)
}

func (w *CacheWrapper) Delete(ctx context.Context, key string) error {
	return w.ch.Delete(ctx, key)
}

// WrapFunc wraps a function with caching (low-level, interface{})
func (w *CacheWrapper) WrapFunc(fn interface{}, opts ...CacheFuncOption) interface{} {
	opt := w.options.Clone()
	for _, o := range opts {
		o(opt)
	}

	fnType := reflect.TypeOf(fn)
	fnValue := reflect.ValueOf(fn)

	if fnType.Kind() != reflect.Func {
		panic("WrapFunc: fn must be a function")
	}

	if fnType.NumIn() < 1 || !fnType.In(0).Implements(contextType) {
		panic("WrapFunc: first parameter must be context.Context")
	}

	if fnType.NumOut() < 1 || !fnType.Out(fnType.NumOut()-1).Implements(errorType) {
		panic("WrapFunc: last return value must be error")
	}

	wrappedFnType := reflect.FuncOf(
		makeTypes(fnType, fnType.NumIn(), true),
		makeTypes(fnType, fnType.NumOut(), false),
		fnType.IsVariadic(),
	)

	wrappedFn := reflect.MakeFunc(wrappedFnType, func(args []reflect.Value) []reflect.Value {
		ctx := args[0].Interface().(context.Context)

		// If caching is disabled, just call the original function
		if opt.Disabled {
			return fnValue.Call(args)
		}

		cacheKey := w.generateCacheKey(ctx, opt, fnValue, args)

		if fnType.NumOut() > 1 {
			returnValues := make([]any, fnType.NumOut()-1)
			for i := 0; i < fnType.NumOut()-1; i++ {
				returnValues[i] = reflect.New(fnType.Out(i)).Interface()
			}

			atomic.AddInt64(&w.stats.CacheAttempts, 1)

			err := w.ch.Get(ctx, cacheKey, &returnValues)
			if err == nil {
				atomic.AddInt64(&w.stats.Hits, 1)
				results := make([]reflect.Value, fnType.NumOut())
				for i := 0; i < fnType.NumOut()-1; i++ {
					val := reflect.ValueOf(returnValues[i])
					if val.IsNil() {
						results[i] = reflect.Zero(fnType.Out(i))
					} else {
						results[i] = val.Elem()
					}
				}
				results[fnType.NumOut()-1] = reflect.Zero(fnType.Out(fnType.NumOut() - 1))
				return results
			}

			if err == cache.ErrCacheMiss {
				atomic.AddInt64(&w.stats.Misses, 1)
			} else {
				atomic.AddInt64(&w.stats.Errors, 1)
			}
		}

		results := fnValue.Call(args)
		errValue := results[fnType.NumOut()-1]
		if !errValue.IsNil() {
			return results
		}

		if fnType.NumOut() > 1 {
			returnValues := make([]interface{}, fnType.NumOut()-1)
			for i := 0; i < fnType.NumOut()-1; i++ {
				returnValues[i] = results[i].Interface()
			}
			_ = w.ch.Set(ctx, cacheKey, cache.Value(returnValues), cache.TTL(opt.TTL))
		}

		return results
	})

	return wrappedFn.Interface()
}

// Typed wrappers (clean and safe)
func CacheWrap[T any](
	w *CacheWrapper,
	fn func(context.Context) (T, error),
	opts ...CacheFuncOption,
) func(context.Context) (T, error) {
	return w.WrapFunc(fn, opts...).(func(context.Context) (T, error))
}

func CacheWrap1[T, A any](
	w *CacheWrapper,
	fn func(context.Context, A) (T, error),
	opts ...CacheFuncOption,
) func(context.Context, A) (T, error) {
	return w.WrapFunc(fn, opts...).(func(context.Context, A) (T, error))
}

func CacheWrap2[T, A, B any](
	w *CacheWrapper,
	fn func(context.Context, A, B) (T, error),
	opts ...CacheFuncOption,
) func(context.Context, A, B) (T, error) {
	return w.WrapFunc(fn, opts...).(func(context.Context, A, B) (T, error))
}

func CacheWrap3[T, A, B, C any](
	w *CacheWrapper,
	fn func(context.Context, A, B, C) (T, error),
	opts ...CacheFuncOption,
) func(context.Context, A, B, C) (T, error) {
	return w.WrapFunc(fn, opts...).(func(context.Context, A, B, C) (T, error))
}

func CacheWrap4[T, A, B, C, D any](
	w *CacheWrapper,
	fn func(context.Context, A, B, C, D) (T, error),
	opts ...CacheFuncOption,
) func(context.Context, A, B, C, D) (T, error) {
	return w.WrapFunc(fn, opts...).(func(context.Context, A, B, C, D) (T, error))
}

// ResetCacheByKey removes the cached result for a specific key
func (w *CacheWrapper) ResetCacheByKey(ctx context.Context, key string) error {
	err := w.ch.Delete(ctx, key)
	if err != nil && err != cache.ErrCacheMiss {
		return err
	}
	return nil
}

type baseKeyHolder struct {
	Keys map[string]struct{}
	TTL  time.Duration
}

func (w *CacheWrapper) getBaseIndexKey(baseKey string) string {
	prefix := strings.TrimRight(w.options.KeyPrefix, ":")
	return prefix + "_" + "idx:" + baseKey
}

// ResetCacheByBaseKey removes all cached entries associated with the given baseKey
func (w *CacheWrapper) ResetCacheByBaseKey(ctx context.Context, baseKey string) error {
	indexCacheKey := w.getBaseIndexKey(baseKey)
	var holder baseKeyHolder
	if err := w.Get(ctx, indexCacheKey, &holder); err != nil {
		if err == cache.ErrCacheMiss {
			// 如果索引不存在，则认为没有需要清理的缓存
			return nil
		}
		return fmt.Errorf("failed to get base key index: %w", err)
	}
	for k := range holder.Keys {
		if err := w.Delete(ctx, k); err != nil && err != cache.ErrCacheMiss {
			return fmt.Errorf("failed to delete cache key %s: %w", k, err)
		}
	}

	if err := w.Delete(ctx, indexCacheKey); err != nil && err != cache.ErrCacheMiss {
		return fmt.Errorf("failed to delete index cache key: %w", err)
	}

	return nil
}

func (w *CacheWrapper) setBaseKeyIndex(ctx context.Context, basekeys []string, cacheKey string, ttl time.Duration) error {
	for _, basekey := range basekeys {
		indexCacheKey := w.getBaseIndexKey(basekey)
		var holder baseKeyHolder
		if err := w.Get(ctx, indexCacheKey, &holder); err != nil && err != cache.ErrCacheMiss {
			return fmt.Errorf("failed to get base key index: %w", err)
		}
		if holder.Keys == nil {
			holder.Keys = make(map[string]struct{})
		}
		holder.Keys[cacheKey] = struct{}{}
		if ttl > holder.TTL {
			holder.TTL = ttl
		}
		if err := w.Set(ctx, indexCacheKey, &holder, WithTTL(holder.TTL)); err != nil {
			return fmt.Errorf("failed to set base key index: %w", err)
		}
	}
	return nil
}

// GetStats returns current cache stats
func (w *CacheWrapper) GetStats() CacheStats {
	return CacheStats{
		Hits:          atomic.LoadInt64(&w.stats.Hits),
		Misses:        atomic.LoadInt64(&w.stats.Misses),
		Errors:        atomic.LoadInt64(&w.stats.Errors),
		CacheAttempts: atomic.LoadInt64(&w.stats.CacheAttempts),
	}
}

// Close properly cleans up resources used by the CacheWrapper
func (w *CacheWrapper) Close() {
	if w.cancelFunc != nil {
		w.cancelFunc() // Cancel the context to stop the goroutine
	}
	// 关闭 channel前确保所有数据已处理完毕
	close(w.basekeyChan)
	// 如果需要关闭底层缓存实例，可以在这里添加代码
	// if closer, ok := w.ch.(io.Closer); ok {
	// 	closer.Close()
	// }
}

// generateCacheKey generates a cache key and registers it to baseKey if present
func (w *CacheWrapper) generateCacheKey(ctx context.Context, opts *CacheFuncOptions, fnValue reflect.Value, args []reflect.Value) string {

	if opts.KeyGenerator != nil {
		interfaceArgs := make([]interface{}, len(args)-1)
		for i := 1; i < len(args); i++ {
			interfaceArgs[i-1] = args[i].Interface()
		}
		return opts.KeyGenerator(ctx, interfaceArgs...)
	}

	var keyParts []string
	baseKeys := []string{}
	if opts.Key != "" {
		keyParts = append(keyParts, opts.Key)
		parts := strings.Split(opts.Key, ":")
		for i := 0; i < len(parts)-1; i++ {
			baseKeys = append(baseKeys, strings.Join(parts[:i+1], ":"))
		}
	} else {
		fnName := runtime.FuncForPC(fnValue.Pointer()).Name()
		keyParts = append(keyParts, fnName)
	}

	argsParts := make([]any, 0, len(args))
	for i := 1; i < len(args); i++ {
		arg := args[i]
		if arg.Kind() == reflect.Ptr && arg.IsNil() {
			argsParts = append(argsParts, "nil")
			continue
		}

		switch arg.Kind() {
		case reflect.Func, reflect.Chan, reflect.UnsafePointer:
			argsParts = append(argsParts, fmt.Sprintf("%s@%p", arg.Type().String(), arg.Interface()))
			continue
		}

		jsonBytes, err := json.Marshal(arg.Interface())
		if err != nil {
			argsParts = append(argsParts, fmt.Sprintf("%T:%v", arg.Interface(), arg.Interface()))
		} else {
			argsParts = append(argsParts, string(jsonBytes))
		}
	}
	keyParts = append(keyParts, cyutil.MD5(argsParts...))
	cacheKey := opts.KeyPrefix + ":" + strings.Join(keyParts, ":")

	// 如果使用了 BaseKey，则注册该 cacheKey
	if len(baseKeys) > 0 {
		// 使用 select 避免在 channel 满时阻塞
		select {
		case w.basekeyChan <- baseKey{
			baseKeys: baseKeys,
			cacheKey: cacheKey,
			ttl:      opts.TTL,
		}:
			// 成功发送
		default:
			// channel 已满，记录日志或采取其他措施
			// log.Printf("Warning: basekeyChan is full, skipping registration for %s", cacheKey)
		}
	}
	return cacheKey
}

// Helpers
var (
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

func makeTypes(t reflect.Type, n int, in bool) []reflect.Type {
	types := make([]reflect.Type, n)
	for i := 0; i < n; i++ {
		if in {
			types[i] = t.In(i)
		} else {
			types[i] = t.Out(i)
		}
	}
	return types
}
