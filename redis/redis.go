package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/SharedCode/sop/encoding"
)

// Cache interface specifies the methods implemented for Redis caching.
// String key and interface{} value are the supported types.
type Cache interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	GetEx(ctx context.Context, key string, expiration time.Duration) (string, error)

	// SetStruct upserts a given object with a key to it.
	SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	// GetStruct fetches a given object given a key.
	GetStruct(ctx context.Context, key string, target interface{}) error
	// GetStruct fetches a given object given a key in a TTL manner, that is, sliding time.
	GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) error
	// Delete removes the object given a key.
	Delete(ctx context.Context, keys ...string) error
	// Ping is a utility function to check if connection is good.
	Ping(ctx context.Context) error
}

type client struct{}

// Checks if Redis connection is open and returns the client interface if it is,
// otherwise returns an error.
func NewClient() Cache {
	return &client{}
}

// KeyNotFound will detect whether error signifies key not found by Redis.
func KeyNotFound(err error) bool {
	return err == redis.Nil
}

// Ping tests connectivity for redis (PONG should be returned)
func (c client) Ping(ctx context.Context) error {
	if connection == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	pong, err := connection.Client.Ping(ctx).Result()
	if err != nil {
		return err
	}
	fmt.Println(pong, err)
	// Output: PONG <nil>

	return nil
}

// Set executes the redis Set command
func (c client) Set(ctx context.Context, key string, value string, expiration time.Duration) error {
	if connection == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	if expiration < 0 {
		expiration = connection.Options.GetDefaultDuration()
	}
	return connection.Client.Set(ctx, key, value, expiration).Err()
}

// Get executes the redis Get command
func (c client) Get(ctx context.Context, key string) (string, error) {
	if connection == nil {
		return "", fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	return connection.Client.Get(ctx, key).Result()
}

// Get executes the redis GetEx command
func (c client) GetEx(ctx context.Context, key string, expiration time.Duration) (string, error) {
	if connection == nil {
		return "", fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	return connection.Client.GetEx(ctx, key, expiration).Result()
}

// SetStruct executes the redis Set command
func (c client) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	if connection == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	// serialize User object to JSON
	ba, err := encoding.BlobMarshaler.Marshal(value)
	if err != nil {
		return err
	}
	// SET object
	if expiration < 0 {
		expiration = connection.Options.GetDefaultDuration()
	}
	return connection.Client.Set(ctx, key, ba, expiration).Err()
}

// GetStruct executes the redis Get command
func (c client) GetStruct(ctx context.Context, key string, target interface{}) error {
	if connection == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	if target == nil {
		return fmt.Errorf("target can't be nil")
	}
	ba, err := connection.Client.Get(ctx, key).Bytes()
	if err == nil {
		err = encoding.BlobMarshaler.Unmarshal(ba, target)
	}
	return err
}

// GetStructEx executes the redis GetEx command
func (c client) GetStructEx(ctx context.Context, key string, target interface{}, expiration time.Duration) error {
	if connection == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	if target == nil {
		return fmt.Errorf("target can't be nil")
	}
	ba, err := connection.Client.GetEx(ctx, key, expiration).Bytes()
	if err == nil {
		err = encoding.BlobMarshaler.Unmarshal(ba, target)
	}
	return err
}

// Delete executes the redis Del command
func (c client) Delete(ctx context.Context, keys ...string) error {
	if connection == nil {
		return fmt.Errorf("Redis connection is not open, 'can't create new client")
	}
	var r = connection.Client.Del(ctx, keys...)
	return r.Err()
}
