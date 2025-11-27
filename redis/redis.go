package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
)

type client struct {
	conn    *Connection
	isOwner bool
}

// NewClient returns a Cache backed by the default shared Redis connection.
// The underlying connection must have been initialized via package-level setup.
func NewClient() sop.L2Cache {
	return &client{
		conn: connection,
	}
}

// NewConnectionClient opens a new Redis connection with the given options and returns a CloseableCache.
// Call Close on the returned cache when no longer needed. Useful for isolating blob-related caches.
func NewConnectionClient(options Options) sop.CloseableCache {
	c := openConnection(options)
	return &client{
		conn:    c,
		isOwner: true,
	}
}

// Close closes the owned Redis connection, if any.
func (c *client) Close() error {
	if !c.isOwner || c.conn == nil {
		return nil
	}
	err := closeConnection(c.conn)
	c.conn = nil
	return err
}

// keyNotFound reports whether the provided error corresponds to a missing key in Redis.
func (c client) keyNotFound(err error) bool {
	return err == redis.Nil
}

// Ping tests connectivity to Redis.
func (c client) Ping(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("redis connection is not open; can't create new client")
	}
	pong, err := c.conn.Client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	fmt.Println(pong, err)
	// Output: PONG <nil>

	return nil
}

// Clear removes all keys in the current Redis database. Use with caution.
func (c client) Clear(ctx context.Context) error {
	return c.conn.Client.FlushDB(ctx).Err()
}

// Set stores a string value with the specified expiration; expiration < 0 disables caching.
func (c client) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	if c.conn == nil {
		return fmt.Errorf("redis connection is not open; can't create new client")
	}
	// No caching if expiration < 0.
	if expiration < 0 {
		return nil
	}
	return c.conn.Client.Set(ctx, key, value, expiration).Err()
}

// Get retrieves a string value. Returns (found, value, error-from-backend).
func (c client) Get(ctx context.Context, key string) (bool, string, error) {
	if c.conn == nil {
		return false, "", fmt.Errorf("redis connection is not open; can't create new client")
	}
	s, err := c.conn.Client.Get(ctx, key).Result()
	// Convert key not found into returning false and nil err.
	r := err == nil
	if c.keyNotFound(err) {
		err = nil
	}
	return r, s, err
}

// GetEx retrieves a string value and sets its expiration (TTL) at the same time.
func (c client) GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error) {
	if c.conn == nil {
		return false, "", fmt.Errorf("redis connection is not open; can't create new client")
	}
	s, err := c.conn.Client.GetEx(ctx, key, expiration).Result()
	// Convert key not found into returning false and nil err.
	r := err == nil
	if c.keyNotFound(err) {
		err = nil
	}
	return r, s, err
}

// SetStruct marshals a struct and stores it with the specified expiration; expiration < 0 disables caching.
func (c client) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if c.conn == nil {
		return fmt.Errorf("redis connection is not open; can't create new client")
	}

	// No caching if expiration < 0.
	if expiration < 0 {
		return nil
	}

	// serialize User object to JSON
	ba, err := encoding.DefaultMarshaler.Marshal(value)
	if err != nil {
		return err
	}
	return c.conn.Client.Set(ctx, key, ba, expiration).Err()
}

// GetStruct retrieves a struct value and unmarshals it into target.
func (c client) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	if c.conn == nil {
		return false, fmt.Errorf("redis connection is not open; can't create new client")
	}
	if target == nil {
		return false, fmt.Errorf("target can't be nil")
	}
	ba, err := c.conn.Client.Get(ctx, key).Bytes()
	if err == nil {
		err = encoding.DefaultMarshaler.Unmarshal(ba, target)
	}

	// Convert key not found into returning false and nil err.
	r := err == nil
	if c.keyNotFound(err) {
		err = nil
	}
	return r, err
}

// GetStructEx retrieves a struct value with TTL behavior and unmarshals it into target.
func (c client) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error) {
	if c.conn == nil {
		return false, fmt.Errorf("redis connection is not open; can't create new client")
	}
	if target == nil {
		return false, fmt.Errorf("target can't be nil")
	}
	ba, err := c.conn.Client.GetEx(ctx, key, expiration).Bytes()
	if err == nil {
		err = encoding.DefaultMarshaler.Unmarshal(ba, target)
	}

	// Convert key not found into returning false and nil err.
	r := err == nil
	if c.keyNotFound(err) {
		err = nil
	}
	return r, err
}

// Delete removes keys and returns whether the operation completed without backend errors.
func (c client) Delete(ctx context.Context, keys []string) (bool, error) {
	if c.conn == nil {
		return false, fmt.Errorf("redis connection is not open; can't create new client")
	}
	var rs = c.conn.Client.Del(ctx, keys...)

	err := rs.Err()
	// Convert key not found into returning false and nil err.
	r := err == nil
	if c.keyNotFound(err) {
		err = nil
	}
	return r, err
}

// IsRestarted returns true if the Redis server run_id has changed since the previous call.
// It uses the INFO server section to read run_id, caching it per client instance.
func (c *client) IsRestarted(ctx context.Context) bool {
	if atomic.SwapInt64(&hasRestarted, 0) == 1 {
		return true
	}
	return false
}

func init() {
	sop.RegisterCacheFactory(sop.Redis, NewClient)
	sop.SetCacheFactory(sop.Redis)
}
