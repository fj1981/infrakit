// Package cydist provides a generic in-memory event store backed by Redis.
package cydist

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// KeyConfig stores configuration for a key.
type KeyConfig struct {
	Expire     time.Duration `json:"expire"`
	MaxRecords int64         `json:"max_records"`
}

// Record holds the data and timestamp.
type Record[T any] struct {
	Data      T         `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// RecordStore is a generic in-memory store for time-series records.
type RecordStore[T any] struct {
	client           *RedisClient
	namespace        string
	configCache      map[string]*KeyConfig
	configCacheTimes map[string]time.Time
	createdCache     map[string]bool // Cache for IsKeyCreated
	configCacheTTL   time.Duration
	cacheMutex       sync.RWMutex
}

// NewR'e'a'co'r'dStore creates a new RecordStore instance.
func NewRecordStore[T any](client *RedisClient, namespace string) *RecordStore[T] {
	ms := &RecordStore[T]{
		client:           client,
		namespace:        namespace,
		configCache:      make(map[string]*KeyConfig),
		configCacheTimes: make(map[string]time.Time),
		createdCache:     make(map[string]bool),
		configCacheTTL:   5 * time.Minute,
	}
	go ms.cleanupExpiredCache()
	return ms
}

// SetConfigCacheTTL sets the TTL for config cache.
func (ms *RecordStore[T]) SetConfigCacheTTL(ttl time.Duration) {
	ms.cacheMutex.Lock()
	defer ms.cacheMutex.Unlock()
	ms.configCacheTTL = ttl
	ms.configCache = make(map[string]*KeyConfig)
	ms.configCacheTimes = make(map[string]time.Time)
	ms.createdCache = make(map[string]bool)
}

// cleanupExpiredCache runs periodically to clean expired cache entries.
func (ms *RecordStore[T]) cleanupExpiredCache() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		var keysToDelete []string
		ms.cacheMutex.RLock()
		for key, createTime := range ms.configCacheTimes {
			if now.Sub(createTime) > ms.configCacheTTL {
				keysToDelete = append(keysToDelete, key)
			}
		}
		ms.cacheMutex.RUnlock()

		ms.cacheMutex.Lock()
		for _, key := range keysToDelete {
			delete(ms.configCache, key)
			delete(ms.configCacheTimes, key)
			delete(ms.createdCache, key)
		}
		ms.cacheMutex.Unlock()
	}
}

// getNamespacedKey adds namespace prefix.
func (ms *RecordStore[T]) getNamespacedKey(key string) string {
	if ms.namespace != "" {
		return fmt.Sprintf("%s:%s", ms.namespace, key)
	}
	return key
}

// getConfigKey returns the Redis key for storing config.
func (ms *RecordStore[T]) getConfigKey(key string) string {
	return ms.getNamespacedKey(key) + ":config"
}

// CreateKey creates a new key with config (idempotent).
// Uses SETNX to prevent race conditions in multi-instance environments.
func (ms *RecordStore[T]) CreateKey(ctx context.Context, key string, expire time.Duration, maxRecords int64) error {
	config := &KeyConfig{
		Expire:     expire,
		MaxRecords: maxRecords,
	}
	configBytes, err := json.Marshal(config)
	if err != nil {
		return err
	}

	configKey := ms.getConfigKey(key)

	// Use SET with NX: only set if key does not exist
	_, err = ms.client.SetNX(ctx, configKey, configBytes, expire)
	if err != nil {
		return err
	}

	// Cache the config and created status
	ms.cacheMutex.Lock()
	ms.configCache[key] = config
	ms.configCacheTimes[key] = time.Now()
	ms.createdCache[key] = true
	ms.cacheMutex.Unlock()

	return nil
}

// DeleteKey removes the key and its config.
func (ms *RecordStore[T]) DeleteKey(ctx context.Context, key string) (int64, int64, error) {
	configKey := ms.getConfigKey(key)
	deletedConfig, err := ms.client.Del(ctx, configKey)
	if err != nil {
		return 0, 0, err
	}

	namespacedKey := ms.getNamespacedKey(key)
	deletedRecords, err := (*ms.client).Del(ctx, namespacedKey)
	if err != nil {
		return deletedConfig, 0, err
	}

	// Clear caches
	ms.cacheMutex.Lock()
	delete(ms.configCache, key)
	delete(ms.configCacheTimes, key)
	delete(ms.createdCache, key)
	ms.cacheMutex.Unlock()

	return deletedConfig, deletedRecords, nil
}

// getKeyConfig retrieves the config for a key (with cache).
func (ms *RecordStore[T]) getKeyConfig(ctx context.Context, key string) (*KeyConfig, error) {
	ms.cacheMutex.RLock()
	config, exists := ms.configCache[key]
	createTime, timeExists := ms.configCacheTimes[key]
	ttl := ms.configCacheTTL
	ms.cacheMutex.RUnlock()

	if exists && timeExists && time.Since(createTime) <= ttl {
		return config, nil
	}

	configKey := ms.getConfigKey(key)
	configBytes, err := ms.client.Get(ctx, configKey)
	if err != nil {
		if err == (*ms.client).Nil() {
			return nil, &KeyNotConfiguredError{Key: key}
		}
		return nil, err
	}

	var configObj KeyConfig
	err = json.Unmarshal([]byte(configBytes), &configObj)
	if err != nil {
		return nil, err
	}

	ms.cacheMutex.Lock()
	ms.configCache[key] = &configObj
	ms.configCacheTimes[key] = time.Now()
	ms.createdCache[key] = true
	ms.cacheMutex.Unlock()

	return &configObj, nil
}

// IsKeyCreated checks if the key has been created (i.e., config exists).
// Uses local cache for performance.
func (ms *RecordStore[T]) IsKeyCreated(ctx context.Context, key string) (bool, error) {
	// Check local cache first
	ms.cacheMutex.RLock()
	if exists, found := ms.createdCache[key]; found {
		ms.cacheMutex.RUnlock()
		return exists, nil
	}
	ms.cacheMutex.RUnlock()

	// Check Redis
	configKey := ms.getConfigKey(key)
	exists, err := ms.client.Exists(ctx, configKey)
	if err != nil {
		return false, err
	}

	result := exists == 1

	// Update cache
	ms.cacheMutex.Lock()
	ms.createdCache[key] = result
	ms.cacheMutex.Unlock()

	return result, nil
}

// WriteRecord writes a new record to the key.
func (ms *RecordStore[T]) WriteRecord(ctx context.Context, key string, data T) error {
	config, err := ms.getKeyConfig(ctx, key)
	if err != nil {
		return err
	}

	namespacedKey := ms.getNamespacedKey(key)
	record := Record[T]{
		Data:      data,
		Timestamp: time.Now(),
	}
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return err
	}

	var txErr error
	for i := 0; i < 3; i++ {
		txErr = ms.client.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.LPush(ctx, namespacedKey, recordBytes).Result()
			if err != nil {
				return err
			}
			if config.MaxRecords > 0 {
				_, err = tx.LTrim(ctx, namespacedKey, 0, config.MaxRecords-1).Result()
				if err != nil {
					return err
				}
			}
			_, err = tx.Expire(ctx, namespacedKey, config.Expire).Result()
			return err
		}, namespacedKey)

		if txErr == nil || txErr != redis.TxFailedErr {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return txErr
}

// GetLatestRecords retrieves the latest N records.
func (ms *RecordStore[T]) GetLatestRecords(ctx context.Context, key string, count int64) ([]Record[T], error) {
	if count <= 0 {
		return []Record[T]{}, nil
	}

	namespacedKey := ms.getNamespacedKey(key)
	values, err := ms.client.LRange(ctx, namespacedKey, 0, count-1)
	if err != nil {
		return nil, err
	}

	records := make([]Record[T], 0, len(values))
	for _, value := range values {
		var record Record[T]
		err := json.Unmarshal([]byte(value), &record)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// GetRecordCount returns the number of records for a key.
func (ms *RecordStore[T]) GetRecordCount(ctx context.Context, key string) (int64, error) {
	namespacedKey := ms.getNamespacedKey(key)
	return ms.client.LLen(ctx, namespacedKey)
}

// KeyNotConfiguredError is returned when a key is not configured.
type KeyNotConfiguredError struct {
	Key string
}

func (e *KeyNotConfiguredError) Error() string {
	return "key not configured: " + e.Key
}
