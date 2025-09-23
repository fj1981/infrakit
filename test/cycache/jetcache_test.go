package cydist_test

import (
	"context"
	"testing"
	"time"

	cache "github.com/mgtv-tech/jetcache-go"
	"github.com/mgtv-tech/jetcache-go/local"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestUser struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestJetCache(t *testing.T) {

	// Create JetCache instance
	jcache := cache.New(cache.WithName("any"),
		cache.WithLocal(local.NewFreeCache(256*local.MB, time.Minute)))
	defer jcache.Close()

	ctx := context.Background()

	t.Run("Set and Get", func(t *testing.T) {
		user := []any{1, "John"}
		err := jcache.Set(ctx, "user:1", cache.Value(user), cache.TTL(time.Minute))
		require.NoError(t, err)

		var result []any
		err = jcache.Get(ctx, "user:1", &result)
		require.NoError(t, err)
		assert.Equal(t, user[0], result[0])
		assert.Equal(t, user[1], result[1])
	})

	t.Run("Delete", func(t *testing.T) {
		user := TestUser{ID: 2, Name: "Jane"}
		err := jcache.Set(ctx, "user:2", cache.Value(user), cache.TTL(time.Minute))
		require.NoError(t, err)

		err = jcache.Delete(ctx, "user:2")
		require.NoError(t, err)

		var result TestUser
		err = jcache.Get(ctx, "user:2", &result)
		assert.Error(t, err) // Should be not found error
	})

}
