package s3

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

type s3Bucket struct {
	redisCache redis.Cache
}

func NewBucket() sop.KeyValueStore[string, []byte] {
	return &s3Bucket{
		redisCache: redis.NewClient(),
	}
}

// Fetch bucket entry with a given name.
func (s3 *s3Bucket)Get(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	return nil
}
func (s3 *s3Bucket)Add(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[string] {
	return nil
}
func (s3 *s3Bucket)Update(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[string] {
	return nil
}
func (s3 *s3Bucket)Remove(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[string] {
	return nil
}
