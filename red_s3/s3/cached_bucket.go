
package s3

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

type cachedBucket struct {
	redisCache redis.Cache
	bucketStore sop.KeyValueStore[string, s3Object]
}

func NewCachedBucket(ctx context.Context, bucketName string) (sop.KeyValueStore[string, []byte], error) {
	bs, err := NewBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	return &cachedBucket{
		redisCache: redis.NewClient(),
		bucketStore: bs,
	}, nil
}

// Fetch bucket entry with a given name.
func (b *cachedBucket)Fetch(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {

	return nil
}

// Fetch large bucket entry with a given name. NOTE: no caching, straight fetch from S3.
func (b *cachedBucket)FetchLargeObject(ctx context.Context, name string) ([]byte, error) {
	return nil, nil
}

func (b *cachedBucket)Add(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[string] {
	return nil
}

func (b *cachedBucket)Update(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[string] {
	return b.Add(ctx, entries...)
}

func (b *cachedBucket)Remove(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[string] {
	return nil
}
