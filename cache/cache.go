package cache

import (
	"context"
	"time"
)

// Cache interface specifies the methods of the cache system. Allows us to use either in-memory or Redis.
type Cache interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) error
	Get(ctx context.Context, key string) (string, error)

	SetStruct(ctx context.Context, key string, value interface{}, expiration time.Duration) error
	GetStruct(ctx context.Context, key string, targetRef interface{}) (interface{}, error)
	Delete(ctx context.Context, key string) error
}
