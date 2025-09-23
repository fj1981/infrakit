package cydist_test
package cydist_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockRedisClient is a mock implementation of RedisClient for testing
type mockRedisClient struct {
	mock.Mock
}

func (m *mockRedisClient) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	args := m.Called(ctx, key, value, expiration)
	cmd := redis.NewBoolCmd(ctx)
	if args.Get(0) != nil {
		cmd.SetVal(args.Bool(0))
	}
	if args.Get(1) != nil {
		cmd.SetErr(args.Error(1))
	}
	return cmd
}

func (m *mockRedisClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	mockArgs := m.Called(ctx, script, keys, args)
	cmd := redis.NewCmd(ctx)
	if mockArgs.Get(0) != nil {
		cmd.SetVal(mockArgs.Get(0))
	}
	if mockArgs.Get(1) != nil {
		cmd.SetErr(mockArgs.Error(1))
	}
	return cmd
}

func (m *mockRedisClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	args := m.Called(ctx, keys)
	cmd := redis.NewIntCmd(ctx)
	if args.Get(0) != nil {
		cmd.SetVal(args.Int64(0))
	}
	if args.Get(1) != nil {
		cmd.SetErr(args.Error(1))
	}
	return cmd
}

func TestLockManager_GetLock(t *testing.T) {
	// Create a mock Redis client
	mockClient := new(mockRedisClient)
	
	// Create a lock manager
	lm := NewLockManager(mockClient)
	
	// Test getting a lock
	lock1 := lm.GetLock("test-key", time.Second*10)
	assert.NotNil(t, lock1)
	assert.Equal(t, "lock:test-key", lock1.key)
	assert.Equal(t, time.Second*10, lock1.ttl)
	
	// Test getting the same lock again - should return the same instance
	lock2 := lm.GetLock("test-key", time.Second*5)
	assert.Equal(t, lock1, lock2)
	assert.Equal(t, time.Second*10, lock2.ttl) // TTL should remain the same as the first lock
	
	// Test getting a different lock
	lock3 := lm.GetLock("test-key-2", time.Second*15)
	assert.NotEqual(t, lock1, lock3)
	assert.Equal(t, "lock:test-key-2", lock3.key)
	assert.Equal(t, time.Second*15, lock3.ttl)
}

func TestLockManager_RemoveLock(t *testing.T) {
	// Create a mock Redis client
	mockClient := new(mockRedisClient)
	
	// Create a lock manager
	lm := NewLockManager(mockClient)
	
	// Add a lock
	lock := lm.GetLock("test-key", time.Second*10)
	assert.NotNil(t, lock)
	
	// Verify the lock exists
	assert.True(t, lm.HasLock("test-key"))
	
	// Remove the lock
	lm.RemoveLock("test-key")
	
	// Verify the lock no longer exists
	assert.False(t, lm.HasLock("test-key"))
}

func TestLockManager_HasLock(t *testing.T) {
	// Create a mock Redis client
	mockClient := new(mockRedisClient)
	
	// Create a lock manager
	lm := NewLockManager(mockClient)
	
	// Initially no locks should exist
	assert.False(t, lm.HasLock("test-key"))
	
	// Add a lock
	lm.GetLock("test-key", time.Second*10)
	
	// Now the lock should exist
	assert.True(t, lm.HasLock("test-key"))
}

func TestLockManager_ListLocks(t *testing.T) {
	// Create a mock Redis client
	mockClient := new(mockRedisClient)
	
	// Create a lock manager
	lm := NewLockManager(mockClient)
	
	// Initially no locks should exist
	keys := lm.ListLocks()
	assert.Empty(t, keys)
	
	// Add some locks
	lm.GetLock("key1", time.Second*10)
	lm.GetLock("key2", time.Second*10)
	lm.GetLock("key3", time.Second*10)
	
	// Check that all keys are listed
	keys = lm.ListLocks()
	assert.Len(t, keys, 3)
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
	assert.Contains(t, keys, "key3")
}