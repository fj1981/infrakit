package cydist

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter represents a rate limiter using Redis.
type RateLimiter struct {
	client *RedisClient
	key    string
	limit  int
	window time.Duration
}

// NewRateLimiter creates a new rate limiter.
func NewRateLimiter(client *RedisClient, key string, limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		client: client,
		key:    fmt.Sprintf("ratelimit:%s", key),
		limit:  limit,
		window: window,
	}
}

// Allow checks if the action is allowed by the rate limiter.
// Returns true if the action is allowed, false otherwise.
func (r *RateLimiter) Allow(ctx context.Context) (bool, error) {
	now := time.Now().UnixNano() / int64(time.Millisecond)
	windowStart := now - int64(r.window.Milliseconds())

	pipe := r.client.universalClient.Pipeline()
	pipe.ZRemRangeByScore(ctx, r.key, "0", fmt.Sprintf("%d", windowStart))
	pipe.ZAdd(ctx, r.key, redis.Z{Score: float64(now), Member: now})
	pipe.ZCard(ctx, r.key)
	pipe.Expire(ctx, r.key, r.window)

	results, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	count := results[2].(*redis.IntCmd).Val()
	return count <= int64(r.limit), nil
}

// Reset resets the rate limiter.
func (r *RateLimiter) Reset(ctx context.Context) error {
	return r.client.universalClient.Del(ctx, r.key).Err()
}

// Counter represents a distributed counter using Redis.
type Counter struct {
	client *RedisClient
	key    string
}

// NewCounter creates a new distributed counter.
func NewCounter(client *RedisClient, key string) *Counter {
	return &Counter{
		client: client,
		key:    fmt.Sprintf("counter:%s", key),
	}
}

// Increment increments the counter by the given amount.
func (c *Counter) Increment(ctx context.Context, amount int64) (int64, error) {
	return c.client.universalClient.IncrBy(ctx, c.key, amount).Result()
}

// Decrement decrements the counter by the given amount.
func (c *Counter) Decrement(ctx context.Context, amount int64) (int64, error) {
	return c.client.universalClient.DecrBy(ctx, c.key, amount).Result()
}

// Get returns the current value of the counter.
func (c *Counter) Get(ctx context.Context) (int64, error) {
	val, err := c.client.universalClient.Get(ctx, c.key).Int64()
	if err != nil {
		if err.Error() == "redis: nil" {
			return 0, nil
		}
		return 0, err
	}
	return val, nil
}

// Reset resets the counter to zero.
func (c *Counter) Reset(ctx context.Context) error {
	return c.client.universalClient.Del(ctx, c.key).Err()
}

// SetExpiry sets the expiry time for the counter.
func (c *Counter) SetExpiry(ctx context.Context, expiry time.Duration) (bool, error) {
	return c.client.universalClient.Expire(ctx, c.key, expiry).Result()
}

// PubSub represents a publish/subscribe system using Redis.
type PubSub struct {
	client *RedisClient
}

// NewPubSub creates a new publish/subscribe system.
func NewPubSub(client *RedisClient) *PubSub {
	return &PubSub{
		client: client,
	}
}

// Publish publishes a message to the given channel.
func (p *PubSub) Publish(ctx context.Context, channel string, message interface{}) error {
	var msgStr string

	switch msg := message.(type) {
	case string:
		msgStr = msg
	case []byte:
		msgStr = string(msg)
	default:
		data, err := json.Marshal(message)
		if err != nil {
			return err
		}
		msgStr = string(data)
	}

	return p.client.universalClient.Publish(ctx, channel, msgStr).Err()
}

// Subscribe subscribes to the given channels and returns a subscription.
func (p *PubSub) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return p.client.universalClient.Subscribe(ctx, channels...)
}

// BloomFilter represents a probabilistic data structure using Redis.
type BloomFilter struct {
	client *RedisClient
	key    string
	script *redis.Script
}

// NewBloomFilter creates a new bloom filter.
func NewBloomFilter(client *RedisClient, key string) *BloomFilter {
	script := redis.NewScript(`
		local key = KEYS[1]
		local item = ARGV[1]
		local hash1 = tonumber(redis.sha1hex(item .. "1"), 16) % 100000
		local hash2 = tonumber(redis.sha1hex(item .. "2"), 16) % 100000
		local hash3 = tonumber(redis.sha1hex(item .. "3"), 16) % 100000
		
		local exists = redis.call("GETBIT", key, hash1) 
			and redis.call("GETBIT", key, hash2) 
			and redis.call("GETBIT", key, hash3)
		
		if exists == 0 then
			redis.call("SETBIT", key, hash1, 1)
			redis.call("SETBIT", key, hash2, 1)
			redis.call("SETBIT", key, hash3, 1)
			return 0
		end
		
		return 1
	`)

	return &BloomFilter{
		client: client,
		key:    fmt.Sprintf("bloom:%s", key),
		script: script,
	}
}

// Add adds an item to the bloom filter.
// Returns true if the item might have already been in the filter, false otherwise.
func (b *BloomFilter) Add(ctx context.Context, item string) (bool, error) {
	result, err := b.script.Run(ctx, b.client.universalClient, []string{b.key}, item).Int64()
	if err != nil {
		return false, err
	}

	return result == 1, nil
}

// SetExpiry sets the expiry time for the bloom filter.
func (b *BloomFilter) SetExpiry(ctx context.Context, expiry time.Duration) (bool, error) {
	return b.client.universalClient.Expire(ctx, b.key, expiry).Result()
}

// Reset resets the bloom filter.
func (b *BloomFilter) Reset(ctx context.Context) error {
	return b.client.universalClient.Del(ctx, b.key).Err()
}
