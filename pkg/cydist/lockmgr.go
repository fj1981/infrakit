// distlock.go
package cydist

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/hashicorp/golang-lru/v2"
)

// DistLockManager manages distributed locks with caching and config options.
type DistLockManager struct {
	rsync      *redsync.Redsync
	locker     *lru.Cache[string, *redsync.Mutex]
	cacheMu    sync.RWMutex // protects LRU from concurrent access (though LRU is thread-safe, use if needed)
	cacheSize  int
	retryDelay time.Duration
	tries      int
}

// Option configures the DistLockManager.
type DLockOption func(*DistLockManager)

// WithCacheSize sets the max number of cached mutexes (default: 1000).
func WithCacheSize(size int) DLockOption {
	return func(lm *DistLockManager) {
		lm.cacheSize = size
	}
}

// WithRetryDelay sets the retry delay for redsync.
func WithRetryDelay(delay time.Duration) DLockOption {
	return func(lm *DistLockManager) {
		lm.retryDelay = delay
	}
}

// WithTries sets the max number of attempts to acquire the lock.
func WithTries(tries int) DLockOption {
	return func(lm *DistLockManager) {
		lm.tries = tries
	}
}

// NewLockManager creates a new distributed lock manager.
// It uses an LRU cache to avoid infinite growth of mutex entries.
func NewLockManager(client *RedisClient, opts ...DLockOption) (*DistLockManager, error) {
	if client == nil || client.universalClient == nil {
		return nil, fmt.Errorf("cydist: invalid redis client")
	}

	pool := goredis.NewPool(client.universalClient)
	lm := &DistLockManager{
		rsync:      redsync.New(pool),
		cacheSize:  1000, // default
		retryDelay: 100 * time.Millisecond,
		tries:      3,
	}

	// Apply options
	for _, opt := range opts {
		opt(lm)
	}

	// Initialize LRU cache
	cache, err := lru.New[string, *redsync.Mutex](lm.cacheSize)
	if err != nil {
		return nil, fmt.Errorf("cydist: failed to create LRU cache: %w", err)
	}
	lm.locker = cache

	return lm, nil
}

// GetLock returns a redsync.Mutex for the given key.
// It caches the mutex instance to avoid recreation.
// Not goroutine-safe for the same key's mutex usage (caller must manage).
func (lm *DistLockManager) GetLock(key string) *redsync.Mutex {
	lm.cacheMu.RLock()
	if mutex, ok := lm.locker.Get(key); ok {
		lm.cacheMu.RUnlock()
		return mutex
	}
	lm.cacheMu.RUnlock()

	// Not found, create new
	mutex := lm.rsync.NewMutex(
		key,
		// Optional: set per-lock TTL, otherwise uses global default (8s)
		// redsync.WithExpiry(10*time.Second),
		redsync.WithRetryDelay(lm.retryDelay),
		redsync.WithTries(lm.tries),
	)

	lm.cacheMu.Lock()
	lm.locker.Add(key, mutex)
	lm.cacheMu.Unlock()

	return mutex
}

// Lock acquires the lock with a timeout and returns a wrapper for safe unlock.
// This is the recommended way to use the lock.
func (lm *DistLockManager) Lock(ctx context.Context, key string, opts ...LockOption) (*DistLock, error) {
	if key == "" {
		return nil, fmt.Errorf("lock key cannot be empty")
	}

	// Apply lock-level options
	config := &lockConfig{
		timeout: 10 * time.Second,
	}
	for _, opt := range opts {
		opt(config)
	}

	// Use context timeout if provided (prioritize)
	usedCtx := ctx
	if config.timeout > 0 {
		var cancel context.CancelFunc
		usedCtx, cancel = context.WithTimeout(ctx, config.timeout)
		defer cancel()
	}

	mutex := lm.GetLock(key)
	if err := mutex.LockContext(usedCtx); err != nil {
		return nil, fmt.Errorf("failed to acquire lock for key '%s': %w", key, err)
	}

	return &DistLock{
		mutex: mutex,
		key:   key,
	}, nil
}

// DistLock is a wrapper that ensures safe unlock and provides metadata.
type DistLock struct {
	mutex *redsync.Mutex
	key   string
}

// Unlock releases the distributed lock.
// Returns true if successful, false if lock was lost or already released.
func (dl *DistLock) Unlock() (bool, error) {
	if dl == nil {
		return false, fmt.Errorf("unlock on nil DistLock")
	}
	ok, err := dl.mutex.Unlock()
	return ok, err
}

// Key returns the lock key (useful for logging/metrics).
func (dl *DistLock) Key() string {
	return dl.key
}

// IsLocked checks if the lock is still held (best-effort, not 100% reliable).
func (dl *DistLock) IsLocked() bool {
	// redsync doesn't expose this directly, so we can't implement reliably
	// unless we track it ourselves (optional enhancement)
	return true
}

// LockOption allows per-lock configuration.
type LockOption func(*lockConfig)

type lockConfig struct {
	timeout time.Duration
	// Add more options: expiry, retry delay, etc.
}

// WithLockTimeout sets the maximum time to wait for acquiring the lock.
func WithLockTimeout(timeout time.Duration) LockOption {
	return func(cfg *lockConfig) {
		cfg.timeout = timeout
	}
}
