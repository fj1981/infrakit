package cydist

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Mode represents the Redis connection mode.
type Mode int

const (
	// StandaloneMode represents a standalone Redis server.
	StandaloneMode Mode = iota
	// ClusterMode represents a Redis cluster.
	ClusterMode
	// SentinelMode represents a Redis sentinel setup.
	SentinelMode
)

// Config holds the configuration for the Redis client.
type Config struct {
	// Mode specifies the Redis connection mode (standalone, cluster, or sentinel).
	Mode Mode

	// Common options
	// Password is the password for the Redis server.
	Password string
	// DB is the database to select (not used in cluster mode).
	DB int
	// PoolSize is the maximum number of socket connections.
	PoolSize int
	// MinIdleConns is the minimum number of idle connections.
	MinIdleConns int
	// DialTimeout is the timeout for establishing new connections.
	DialTimeout time.Duration
	// ReadTimeout is the timeout for socket reads.
	ReadTimeout time.Duration
	// WriteTimeout is the timeout for socket writes.
	WriteTimeout time.Duration
	// PoolTimeout is the timeout for getting a connection from the pool.
	PoolTimeout time.Duration
	// IdleTimeout is the timeout for idle connections.
	IdleTimeout time.Duration
	// MaxRetries is the maximum number of retries before giving up.
	MaxRetries int
	// MinRetryBackoff is the minimum backoff between each retry.
	MinRetryBackoff time.Duration
	// MaxRetryBackoff is the maximum backoff between each retry.
	MaxRetryBackoff time.Duration

	// Standalone options
	// Addr is the address of the Redis server (used in standalone mode).
	Addr string

	// Cluster options
	// Addrs is a list of Redis cluster node addresses (used in cluster mode).
	Addrs []string
	// MaxRedirects is the maximum number of redirects to follow (used in cluster mode).
	MaxRedirects int
	// RouteByLatency enables routing read-only commands to the closest master or replica node (used in cluster mode).
	RouteByLatency bool
	// RouteRandomly enables routing read-only commands to random nodes (used in cluster mode).
	RouteRandomly bool

	// Sentinel options
	// MasterName is the name of the master node (used in sentinel mode).
	MasterName string
	// SentinelAddrs is a list of Redis sentinel addresses (used in sentinel mode).
	SentinelAddrs []string
	// SentinelPassword is the password for the sentinel servers (used in sentinel mode).
	SentinelPassword string
}

// DefaultConfig returns a default configuration for the Redis client.
func DefaultConfig() *Config {
	return &Config{
		Addr:            "localhost:6379",
		Password:        "",
		DB:              0,
		PoolSize:        10,
		MinIdleConns:    2,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
		PoolTimeout:     4 * time.Second,
		IdleTimeout:     5 * time.Minute,
		MaxRetries:      3,
		MinRetryBackoff: 8 * time.Millisecond,
		MaxRetryBackoff: 512 * time.Millisecond,
	}
}

// Option defines a function that configures the Redis client.
type Option func(*Config)

// WithAddr sets the Redis server address.
func WithAddr(addr string) Option {
	return func(c *Config) { c.Addr = addr }
}

// WithPassword sets the Redis server password.
func WithPassword(password string) Option {
	return func(c *Config) { c.Password = password }
}

// WithDB sets the Redis database to select.
func WithDB(db int) Option {
	return func(c *Config) { c.DB = db }
}

// WithPoolSize sets the maximum number of socket connections.
func WithPoolSize(poolSize int) Option {
	return func(c *Config) { c.PoolSize = poolSize }
}

// WithMinIdleConns sets the minimum number of idle connections.
func WithMinIdleConns(minIdleConns int) Option {
	return func(c *Config) { c.MinIdleConns = minIdleConns }
}

// WithDialTimeout sets the timeout for establishing new connections.
func WithDialTimeout(dialTimeout time.Duration) Option {
	return func(c *Config) { c.DialTimeout = dialTimeout }
}

// WithReadTimeout sets the timeout for socket reads.
func WithReadTimeout(readTimeout time.Duration) Option {
	return func(c *Config) { c.ReadTimeout = readTimeout }
}

// WithWriteTimeout sets the timeout for socket writes.
func WithWriteTimeout(writeTimeout time.Duration) Option {
	return func(c *Config) { c.WriteTimeout = writeTimeout }
}

// WithPoolTimeout sets the timeout for getting a connection from the pool.
func WithPoolTimeout(poolTimeout time.Duration) Option {
	return func(c *Config) { c.PoolTimeout = poolTimeout }
}

// WithIdleTimeout sets the timeout for idle connections.
func WithIdleTimeout(idleTimeout time.Duration) Option {
	return func(c *Config) { c.IdleTimeout = idleTimeout }
}

// WithMaxRetries sets the maximum number of retries before giving up.
func WithMaxRetries(maxRetries int) Option {
	return func(c *Config) { c.MaxRetries = maxRetries }
}

// WithMinRetryBackoff sets the minimum backoff between each retry.
func WithMinRetryBackoff(minRetryBackoff time.Duration) Option {
	return func(c *Config) { c.MinRetryBackoff = minRetryBackoff }
}

// WithMaxRetryBackoff sets the maximum backoff between each retry.
func WithMaxRetryBackoff(maxRetryBackoff time.Duration) Option {
	return func(c *Config) { c.MaxRetryBackoff = maxRetryBackoff }
}

// WithMode sets the Redis connection mode.
func WithMode(mode Mode) Option {
	return func(c *Config) { c.Mode = mode }
}

// WithAddrs sets the Redis cluster node addresses.
func WithAddrs(addrs []string) Option {
	return func(c *Config) { c.Addrs = addrs }
}

// WithMaxRedirects sets the maximum number of redirects to follow.
func WithMaxRedirects(maxRedirects int) Option {
	return func(c *Config) { c.MaxRedirects = maxRedirects }
}

// WithRouteByLatency enables routing read-only commands to the closest master or replica node.
func WithRouteByLatency(routeByLatency bool) Option {
	return func(c *Config) { c.RouteByLatency = routeByLatency }
}

// WithRouteRandomly enables routing read-only commands to random nodes.
func WithRouteRandomly(routeRandomly bool) Option {
	return func(c *Config) { c.RouteRandomly = routeRandomly }
}

// WithMasterName sets the name of the master node for sentinel mode.
func WithMasterName(masterName string) Option {
	return func(c *Config) { c.MasterName = masterName }
}

// WithSentinelAddrs sets the Redis sentinel addresses.
func WithSentinelAddrs(sentinelAddrs []string) Option {
	return func(c *Config) { c.SentinelAddrs = sentinelAddrs }
}

// WithSentinelPassword sets the password for the sentinel servers.
func WithSentinelPassword(sentinelPassword string) Option {
	return func(c *Config) { c.SentinelPassword = sentinelPassword }
}

// RedisClient is a wrapper around the Redis client.
type RedisClient struct {
	// Universal interface for different Redis client types
	universalClient redis.UniversalClient
	// Client mode (standalone, cluster, or sentinel)
	mode Mode
}

func (r *RedisClient) SetEX(ctx context.Context, key string, value any, expire time.Duration) error {
	return r.universalClient.SetEx(ctx, key, value, expire).Err()
}

func (r *RedisClient) SetNX(ctx context.Context, key string, value any, expire time.Duration) (val bool, err error) {
	return r.universalClient.SetNX(ctx, key, value, expire).Result()
}

func (r *RedisClient) SetXX(ctx context.Context, key string, value any, expire time.Duration) (val bool, err error) {
	return r.universalClient.SetXX(ctx, key, value, expire).Result()
}

func (r *RedisClient) MGet(ctx context.Context, keys ...string) (map[string]any, error) {
	pipeline := r.universalClient.Pipeline()
	keyIdxMap := make(map[int]string, len(keys))
	ret := make(map[string]any, len(keys))

	for idx, key := range keys {
		keyIdxMap[idx] = key
		pipeline.Get(ctx, key)
	}

	cmder, err := pipeline.Exec(ctx)
	if err != nil && !errors.Is(err, r.Nil()) {
		return nil, err
	}

	for idx, cmd := range cmder {
		if strCmd, ok := cmd.(*redis.StringCmd); ok {
			key := keyIdxMap[idx]
			if val, _ := strCmd.Result(); len(val) > 0 {
				ret[key] = val
			}
		}
	}

	return ret, nil
}

func (r *RedisClient) MSet(ctx context.Context, value map[string]any, expire time.Duration) error {
	pipeline := r.universalClient.Pipeline()

	for key, val := range value {
		pipeline.SetEx(ctx, key, val, expire)
	}
	_, err := pipeline.Exec(ctx)

	return err
}

func (r *RedisClient) Nil() error {
	return redis.Nil
}

var defaultClient *RedisClient

// New creates a new RedisClient instance with the given options.
func New(opts ...Option) *RedisClient {
	cfg := DefaultConfig()
	for _, opt := range opts {
		opt(cfg)
	}

	var client redis.UniversalClient

	switch cfg.Mode {
	case ClusterMode:
		// Create a Redis cluster client
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:           cfg.Addrs,
			Password:        cfg.Password,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			PoolTimeout:     cfg.PoolTimeout,
			ConnMaxIdleTime: cfg.IdleTimeout,
			MaxRetries:      cfg.MaxRetries,
			MinRetryBackoff: cfg.MinRetryBackoff,
			MaxRetryBackoff: cfg.MaxRetryBackoff,
			MaxRedirects:    cfg.MaxRedirects,
			RouteByLatency:  cfg.RouteByLatency,
			RouteRandomly:   cfg.RouteRandomly,
		})
	case SentinelMode:
		// Create a Redis sentinel client
		client = redis.NewFailoverClient(&redis.FailoverOptions{
			MasterName:       cfg.MasterName,
			SentinelAddrs:    cfg.SentinelAddrs,
			SentinelPassword: cfg.SentinelPassword,
			Password:         cfg.Password,
			DB:               cfg.DB,
			PoolSize:         cfg.PoolSize,
			MinIdleConns:     cfg.MinIdleConns,
			DialTimeout:      cfg.DialTimeout,
			ReadTimeout:      cfg.ReadTimeout,
			WriteTimeout:     cfg.WriteTimeout,
			PoolTimeout:      cfg.PoolTimeout,
			ConnMaxIdleTime:  cfg.IdleTimeout,
			MaxRetries:       cfg.MaxRetries,
			MinRetryBackoff:  cfg.MinRetryBackoff,
			MaxRetryBackoff:  cfg.MaxRetryBackoff,
		})
	default: // StandaloneMode
		// Create a standalone Redis client
		client = redis.NewClient(&redis.Options{
			Addr:            cfg.Addr,
			Password:        cfg.Password,
			DB:              cfg.DB,
			PoolSize:        cfg.PoolSize,
			MinIdleConns:    cfg.MinIdleConns,
			DialTimeout:     cfg.DialTimeout,
			ReadTimeout:     cfg.ReadTimeout,
			WriteTimeout:    cfg.WriteTimeout,
			PoolTimeout:     cfg.PoolTimeout,
			ConnMaxIdleTime: cfg.IdleTimeout,
			MaxRetries:      cfg.MaxRetries,
			MinRetryBackoff: cfg.MinRetryBackoff,
			MaxRetryBackoff: cfg.MaxRetryBackoff,
		})
	}

	return &RedisClient{
		universalClient: client,
		mode:            cfg.Mode,
	}
}

// InitDefault initializes the default package-level client.
func InitDefault(opts ...Option) {
	defaultClient = New(opts...)
}

// Default returns the default client.
func Default() *RedisClient {
	if defaultClient == nil {
		defaultClient = New()
	}
	return defaultClient
}

// Close closes the client, releasing any open resources.
func (c *RedisClient) Close() error {
	return c.universalClient.Close()
}

// Client returns the underlying Redis client.
func (c *RedisClient) Client() *redis.Client {
	return c.universalClient.(*redis.Client)
}

// UniversalClient returns the underlying universal Redis client.
func (c *RedisClient) UniversalClient() redis.UniversalClient {
	return c.universalClient
}

// Ping pings the Redis server.
func (c *RedisClient) Ping(ctx context.Context) (string, error) {
	return c.universalClient.Ping(ctx).Result()
}

// Get gets the value of a key.
func (c *RedisClient) Get(ctx context.Context, key string) (string, error) {
	return c.universalClient.Get(ctx, key).Result()
}

// GetBytes gets the value of a key as bytes.
func (c *RedisClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	return c.universalClient.Get(ctx, key).Bytes()
}

// GetObject gets the value of a key and unmarshals it into the given object.
func (c *RedisClient) GetObject(ctx context.Context, key string, value interface{}) error {
	data, err := c.universalClient.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

// Set sets the value of a key.
func (c *RedisClient) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	return c.universalClient.Set(ctx, key, value, expiration).Err()
}

// SetObject sets the value of a key to the marshaled object.
func (c *RedisClient) SetObject(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.universalClient.Set(ctx, key, data, expiration).Err()
}

// Del deletes one or more keys.
func (c *RedisClient) Del(ctx context.Context, key string) (int64, error) {
	return c.universalClient.Del(ctx, key).Result()
}

// Exists checks if one or more keys exist.
func (c *RedisClient) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.universalClient.Exists(ctx, keys...).Result()
}

// Expire sets the expiration for a key.
func (c *RedisClient) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return c.universalClient.Expire(ctx, key, expiration).Result()
}

// TTL returns the remaining time to live of a key.
func (c *RedisClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.universalClient.TTL(ctx, key).Result()
}

// Incr increments the integer value of a key by one.
func (c *RedisClient) Incr(ctx context.Context, key string) (int64, error) {
	return c.universalClient.Incr(ctx, key).Result()
}

// IncrBy increments the integer value of a key by the given amount.
func (c *RedisClient) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.universalClient.IncrBy(ctx, key, value).Result()
}

// Decr decrements the integer value of a key by one.
func (c *RedisClient) Decr(ctx context.Context, key string) (int64, error) {
	return c.universalClient.Decr(ctx, key).Result()
}

// DecrBy decrements the integer value of a key by the given amount.
func (c *RedisClient) DecrBy(ctx context.Context, key string, value int64) (int64, error) {
	return c.universalClient.DecrBy(ctx, key, value).Result()
}

// HSet sets field in the hash stored at key to value.
func (c *RedisClient) HSet(ctx context.Context, key, field string, value interface{}) error {
	return c.universalClient.HSet(ctx, key, field, value).Err()
}

// HGet returns the value associated with field in the hash stored at key.
func (c *RedisClient) HGet(ctx context.Context, key, field string) (string, error) {
	return c.universalClient.HGet(ctx, key, field).Result()
}

// HGetAll returns all fields and values of the hash stored at key.
func (c *RedisClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.universalClient.HGetAll(ctx, key).Result()
}

// HDel deletes one or more hash fields.
func (c *RedisClient) HDel(ctx context.Context, key string, fields ...string) (int64, error) {
	return c.universalClient.HDel(ctx, key, fields...).Result()
}

// HExists returns if field is an existing field in the hash stored at key.
func (c *RedisClient) HExists(ctx context.Context, key, field string) (bool, error) {
	return c.universalClient.HExists(ctx, key, field).Result()
}

// HKeys returns all field names in the hash stored at key.
func (c *RedisClient) HKeys(ctx context.Context, key string) ([]string, error) {
	return c.universalClient.HKeys(ctx, key).Result()
}

// HLen returns the number of fields in the hash stored at key.
func (c *RedisClient) HLen(ctx context.Context, key string) (int64, error) {
	return c.universalClient.HLen(ctx, key).Result()
}

// LPush inserts all the specified values at the head of the list stored at key.
func (c *RedisClient) LPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.universalClient.LPush(ctx, key, values...).Result()
}

// RPush inserts all the specified values at the tail of the list stored at key.
func (c *RedisClient) RPush(ctx context.Context, key string, values ...interface{}) (int64, error) {
	return c.universalClient.RPush(ctx, key, values...).Result()
}

// LPop removes and returns the first element of the list stored at key.
func (c *RedisClient) LPop(ctx context.Context, key string) (string, error) {
	return c.universalClient.LPop(ctx, key).Result()
}

// RPop removes and returns the last element of the list stored at key.
func (c *RedisClient) RPop(ctx context.Context, key string) (string, error) {
	return c.universalClient.RPop(ctx, key).Result()
}

// LLen returns the length of the list stored at key.
func (c *RedisClient) LLen(ctx context.Context, key string) (int64, error) {
	return c.universalClient.LLen(ctx, key).Result()
}

// LRange returns the specified elements of the list stored at key.
func (c *RedisClient) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.universalClient.LRange(ctx, key, start, stop).Result()
}

// SAdd adds one or more members to a set.
func (c *RedisClient) SAdd(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return c.universalClient.SAdd(ctx, key, members...).Result()
}

// SMembers returns all the members of the set value stored at key.
func (c *RedisClient) SMembers(ctx context.Context, key string) ([]string, error) {
	return c.universalClient.SMembers(ctx, key).Result()
}

// SRem removes one or more members from a set.
func (c *RedisClient) SRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return c.universalClient.SRem(ctx, key, members...).Result()
}

// SCard returns the set cardinality (number of elements) of the set stored at key.
func (c *RedisClient) SCard(ctx context.Context, key string) (int64, error) {
	return c.universalClient.SCard(ctx, key).Result()
}

// SIsMember returns if member is a member of the set stored at key.
func (c *RedisClient) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	return c.universalClient.SIsMember(ctx, key, member).Result()
}

// ZAdd adds one or more members to a sorted set, or updates its score if it already exists.
func (c *RedisClient) ZAdd(ctx context.Context, key string, members ...redis.Z) (int64, error) {
	return c.universalClient.ZAdd(ctx, key, members...).Result()
}

// ZRange returns a range of members in a sorted set, by index.
func (c *RedisClient) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.universalClient.ZRange(ctx, key, start, stop).Result()
}

// ZRangeWithScores returns a range of members with scores in a sorted set, by index.
func (c *RedisClient) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]redis.Z, error) {
	return c.universalClient.ZRangeWithScores(ctx, key, start, stop).Result()
}

// ZRem removes one or more members from a sorted set.
func (c *RedisClient) ZRem(ctx context.Context, key string, members ...interface{}) (int64, error) {
	return c.universalClient.ZRem(ctx, key, members...).Result()
}

// ZCard returns the sorted set cardinality (number of elements) of the sorted set stored at key.
func (c *RedisClient) ZCard(ctx context.Context, key string) (int64, error) {
	return c.universalClient.ZCard(ctx, key).Result()
}

// ZScore returns the score of member in the sorted set at key.
func (c *RedisClient) ZScore(ctx context.Context, key, member string) (float64, error) {
	return c.universalClient.ZScore(ctx, key, member).Result()
}

// ZRank returns the rank of member in the sorted set stored at key, with the scores ordered from low to high.
func (c *RedisClient) ZRank(ctx context.Context, key, member string) (int64, error) {
	return c.universalClient.ZRank(ctx, key, member).Result()
}

// Subscribe subscribes to the specified channels.
func (c *RedisClient) Subscribe(ctx context.Context, channels ...string) *redis.PubSub {
	return c.universalClient.Subscribe(ctx, channels...)
}

// Publish publishes a message to the specified channel.
func (c *RedisClient) Publish(ctx context.Context, channel string, message interface{}) (int64, error) {
	return c.universalClient.Publish(ctx, channel, message).Result()
}

// ScriptLoad loads a Lua script into the scripts cache.
func (c *RedisClient) ScriptLoad(ctx context.Context, script string) (string, error) {
	return c.universalClient.ScriptLoad(ctx, script).Result()
}

// Eval evaluates a Lua script.
func (c *RedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) (interface{}, error) {
	return c.universalClient.Eval(ctx, script, keys, args...).Result()
}

// Pipeline creates a new pipeline.
func (c *RedisClient) Pipeline() redis.Pipeliner {
	return c.universalClient.Pipeline()
}

// TxPipeline creates a new transaction pipeline.
func (c *RedisClient) TxPipeline() redis.Pipeliner {
	return c.universalClient.TxPipeline()
}

// Watch watches the given keys to determine execution of the MULTI/EXEC block.
func (c *RedisClient) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return c.universalClient.Watch(ctx, fn, keys...)
}
