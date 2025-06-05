package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

type client struct {
	conn    *Connection
	isOwner bool
}

// Checks if Redis connection is open and returns the client interface if it is,
// otherwise returns an error.
func NewClient() sop.Cache {
	return &client{
		conn: connection,
	}
}

// Opens a new Redis connection then returns a client wrapper for it.
// Returned wrapper has "Close" method you can call when you don't need it anymore.
//
// This ctor was provided for the case of wanting to use another separate Redis cluster,
// perhaps wanting to use another one dedicated for Blobs management. Watch out later
// when this feature is supported & you can pass this (blobs) cache in the Transaction options.
func NewConnectionClient(options Options) sop.CloseableCache {
	c := openConnection(options)
	return &client{
		conn:    c,
		isOwner: true,
	}
}

// Close this client's connection.
func (c *client) Close() error {
	if !c.isOwner || c.conn == nil {
		return nil
	}
	err := closeConnection(c.conn)
	c.conn = nil
	return err
}

// keyNotFound will detect whether error signifies key not found by Redis.
func (c client) keyNotFound(err error) bool {
	return err == redis.Nil
}

// Ping tests connectivity for redis (PONG should be returned)
func (c client) Ping(ctx context.Context) error {
	if c.conn == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	pong, err := c.conn.Client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	fmt.Println(pong, err)
	// Output: PONG <nil>

	return nil
}

// Clear the cache. Be cautions calling this method as it will clear the Redis cache.
func (c client) Clear(ctx context.Context) error {
	return c.conn.Client.FlushDB(ctx).Err()
}

// Set executes the redis Set command
func (c client) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	if c.conn == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	// No caching if expiration < 0.
	if expiration < 0 {
		return nil
	}
	return c.conn.Client.Set(ctx, key, value, expiration).Err()
}

// Get executes the redis Get command
func (c client) Get(ctx context.Context, key string) (bool, string, error) {
	if c.conn == nil {
		return false, "", fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	s, err := c.conn.Client.Get(ctx, key).Result()
	// Convert key not found into returning false and nil err.
	r := err == nil
	if c.keyNotFound(err) {
		err = nil
	}
	return r, s, err
}

// Get executes the redis GetEx command
func (c client) GetEx(ctx context.Context, key string, expiration time.Duration) (bool, string, error) {
	if c.conn == nil {
		return false, "", fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	s, err := c.conn.Client.GetEx(ctx, key, expiration).Result()
	// Convert key not found into returning false and nil err.
	r := err == nil
	if c.keyNotFound(err) {
		err = nil
	}
	return r, s, err
}

// SetStruct executes the redis Set command
func (c client) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if c.conn == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
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

// GetStruct executes the redis Get command
func (c client) GetStruct(ctx context.Context, key string, target interface{}) (bool, error) {
	if c.conn == nil {
		return false, fmt.Errorf("Redis connection is not open, 'can't create new client")
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

// GetStructEx executes the redis GetEx command
func (c client) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) (bool, error) {
	if c.conn == nil {
		return false, fmt.Errorf("Redis connection is not open, 'can't create new client")
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

// Delete executes the redis Del command
func (c client) Delete(ctx context.Context, keys []string) (bool, error) {
	if c.conn == nil {
		return false, fmt.Errorf("Redis connection is not open, 'can't create new client")
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
