package cydist_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/fj1981/infrakit/pkg/cydist"
	cache "github.com/mgtv-tech/jetcache-go"
	"github.com/mgtv-tech/jetcache-go/local"
)

// TestUser is a test struct for caching
type TestUser2 struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestCacheWrapper(t *testing.T) {
	// Skip this test if Redis is not available
	//t.Skip("Skipping CacheWrapper tests - requires Redis server")

	// Create JetCache instance
	jcache := cache.New(cache.WithName("any"),
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)))
	defer jcache.Close()

	// Create cache wrapper with real JetCache
	wrapper := cydist.NewCacheWrapper()

	ctx := context.Background()

	t.Run("WrapFunc", func(t *testing.T) {
		// Define a function to be wrapped

		// No need to set up mock expectations with real cache

		// Wrap the function with caching
		wrappedGetUser := cydist.CacheWrap1(
			wrapper,
			func(ctx context.Context, id int) (TestUser, error) {
				// This function should only be called on cache miss
				return TestUser{ID: id, Name: "User" + string(rune('0'+id))}, nil
			},
			cydist.WithTTL(5*time.Minute),
			cydist.WithKeyPrefix("testuser"),
			cydist.WithKey("users:User"),
		)

		// First call should miss cache and call the original function
		user, err := wrappedGetUser(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, user.ID)
		assert.Equal(t, "User1", user.Name)

		// No need to set up mock expectations with real cache
		time.Sleep(1000)
		// Second call should hit cache and not call the original function
		user, err = wrappedGetUser(ctx, 1)
		require.NoError(t, err)
		assert.Equal(t, 1, user.ID)
		assert.Equal(t, "User1", user.Name)
		wrapper.ResetCacheByBaseKey(ctx, "users")
		// No need to verify mock expectations with real cache
	})

	t.Run("ResetCacheByKey", func(t *testing.T) {
		// First set a value to ensure there's something to reset
		err := jcache.Set(ctx, "test:key", cache.Value("test-value"), cache.TTL(time.Minute))
		require.NoError(t, err)

		// Reset cache for a specific key
		err = wrapper.ResetCacheByKey(ctx, "test:key")
		require.NoError(t, err)
	})

	t.Run("ResetCacheByBaseKey", func(t *testing.T) {
		// First, we need to create some cache entries with a common base key
		getUserFn := func(ctx context.Context, id int) (TestUser, error) {
			return TestUser{ID: id, Name: "User" + string(rune('0'+id))}, nil
		}

		baseKey := "users:User"

		// No need to set up mock expectations with real cache

		// Wrap the function with a base key
		wrappedGetUser := cydist.CacheWrap1(
			wrapper,
			getUserFn,
			cydist.WithKey(baseKey),
		)

		// Call the function to create a cache entry
		_, err := wrappedGetUser(ctx, 1)
		require.NoError(t, err)
		time.Sleep(1000)
		// No need to set up mock expectations with real cache

		// Reset all cache entries with the base key
		err = wrapper.ResetCacheByBaseKey(ctx, "users")
		require.NoError(t, err)

	})

	t.Run("GetStats", func(t *testing.T) {
		// Get initial stats
		initialStats := wrapper.GetStats()

		// Set a value in the cache to ensure a cache hit
		user := TestUser{ID: 1, Name: "User1"}
		cacheKey := "stats:1"
		err := jcache.Set(ctx, cacheKey, cache.Value(user), cache.TTL(time.Minute))
		require.NoError(t, err)

		// Define a function to be wrapped
		getUserFn := func(ctx context.Context, id int) (TestUser, error) {
			return TestUser{ID: id, Name: "User" + string(rune('0'+id))}, nil
		}

		// Wrap the function
		wrappedGetUser := cydist.CacheWrap1(
			wrapper,
			getUserFn,
			cydist.WithKeyPrefix("stats:"),
		)

		// Call the function to trigger a cache hit
		_, err = wrappedGetUser(ctx, 1)
		require.NoError(t, err)

		// Get updated stats
		updatedStats := wrapper.GetStats()

		// Verify hit count increased
		assert.Equal(t, initialStats.Hits+1, updatedStats.Hits)

		// No need to set up mock expectations with real cache

		// Call with a different ID to trigger a cache miss
		_, err = wrappedGetUser(ctx, 2)
		require.NoError(t, err)

		// Get final stats
		finalStats := wrapper.GetStats()

		// Verify miss count increased
		assert.Equal(t, updatedStats.Misses+1, finalStats.Misses)

	})

	t.Run("WithDirectKey", func(t *testing.T) {
		// Define a function to be wrapped
		getUserFn := func(ctx context.Context, id int) (TestUser, error) {
			return TestUser{ID: id, Name: "User" + string(rune('0'+id))}, nil
		}

		directKey := "direct:user:1"

		// No need to set up mock expectations with real cache

		// Wrap the function with a direct key
		wrappedGetUser := cydist.CacheWrap1(
			wrapper,
			getUserFn,
			cydist.WithKey(directKey),
		)

		// Call the function
		_, err := wrappedGetUser(ctx, 1)
		require.NoError(t, err)

		// Verify the direct key was used by checking if the value exists in cache
		var result TestUser
		err = jcache.Get(ctx, directKey, &result)
		require.NoError(t, err)
		assert.Equal(t, 1, result.ID)
		assert.Equal(t, "User1", result.Name)
	})

	t.Run("WithKeyGenerator", func(t *testing.T) {
		// Define a function to be wrapped
		getUserFn := func(ctx context.Context, id int) (TestUser, error) {
			return TestUser{ID: id, Name: "User" + string(rune('0'+id))}, nil
		}

		customKey := "custom:user:1"

		// Define a custom key generator
		keyGen := func(ctx context.Context, args ...interface{}) string {
			id := args[0].(int)
			return "custom:user:" + string(rune('0'+id))
		}

		// No need to set up mock expectations with real cache

		// Wrap the function with a custom key generator
		wrappedGetUser := cydist.CacheWrap1(
			wrapper,
			getUserFn,
			cydist.WithKeyGenerator(keyGen),
		)

		// Call the function
		_, err := wrappedGetUser(ctx, 1)
		require.NoError(t, err)

		// Verify the custom key was used by checking if the value exists in cache
		var result TestUser
		err = jcache.Get(ctx, customKey, &result)
		require.NoError(t, err)
		assert.Equal(t, 1, result.ID)
		assert.Equal(t, "User1", result.Name)
	})
}
