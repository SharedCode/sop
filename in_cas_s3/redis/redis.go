package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"time"
)

// Cache interface specifies the methods implemented for Redis caching.
// String key and interface{} value are the supported types.
type Cache interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	Get(ctx context.Context, key string) (string, error)
	SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	GetStruct(ctx context.Context, key string, target interface{}) error
	Delete(ctx context.Context, key string) error
}

type client struct {}

func NewClient() (Cache, error) {
	if connection == nil {
		return nil, fmt.Errorf("Redis connection is not open, 'can't create new client.")
	}
	return &client{}, nil
}

// KeyNotFound will detect whether error signifies key not found by Redis.
func KeyNotFound(err error) bool {
	return err == redis.Nil
}

// Ping tests connectivity for redis (PONG should be returned)
func (c client) Ping(ctx context.Context) error {
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
	if expiration < 0 {
		expiration = connection.Options.GetDefaultDuration()
	}
	return connection.Client.Set(ctx, key, value, expiration).Err()
}

// Get executes the redis Get command
func (c client) Get(ctx context.Context, key string) (string, error) {
	return connection.Client.Get(ctx, key).Result()
}

// SetStruct executes the redis Set command
func (c client) SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error {
	// serialize User object to JSON
	json, err := json.Marshal(value)
	if err != nil {
		return err
	}
	// SET object
	if expiration < 0 {
		expiration = connection.Options.GetDefaultDuration()
	}
	return connection.Client.Set(ctx, key, json, expiration).Err()
}

// GetStruct executes the redis Get command
func (c client) GetStruct(ctx context.Context, key string, target interface{}) error {
	if target == nil {
		panic("target can't be nil.")
	}
	s, err := connection.Client.Get(ctx, key).Result()
	if err == nil {
		err = json.Unmarshal([]byte(s), target)
	}
	if err == redis.Nil {
		return err
	}
	return err
}

// Delete executes the redis Del command
func (c client) Delete(ctx context.Context, key string) error {
	var r = connection.Client.Del(ctx, key)
	return r.Err()
}
