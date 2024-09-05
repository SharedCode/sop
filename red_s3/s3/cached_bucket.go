package s3

import (
	"context"
	"fmt"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

type cachedBucket struct {
	redisCache redis.Cache
	bucketStore sop.KeyValueStore[string, s3Object]
	refreshInterval time.Duration
}

func NewCachedBucket(ctx context.Context, bucketName string, refreshInterval time.Duration) (sop.KeyValueStore[string, []byte], error) {
	bs, err := NewBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	// Minimum refresh interval is 5 seconds, if less then assign 5 minute refresh interval.
	if refreshInterval < time.Duration(5*time.Second) {
		refreshInterval = time.Duration(5*time.Minute)
	}
	return &cachedBucket{
		redisCache: redis.NewClient(),
		bucketStore: bs,
		refreshInterval: refreshInterval,
	}, nil
}

// Fetch entry(ies) with given name(s).
// Fetch term is used here because this CRUD interface is NOT part of the B-Tree system, thus, the context is
// to "fetch" from the remote data storage sub-system like AWS S3.
func (b *cachedBucket)Fetch(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {

	return nil
}

// Fetch a large entry with the given name. NOTE: no caching, straight fetch from S3.
func (b *cachedBucket)FetchLargeObject(ctx context.Context, name string) ([]byte, error) {
	r, err := b.bucketStore.FetchLargeObject(ctx, name)
	return r.Data, err
}

func (b *cachedBucket)Add(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[string] {
	
	return nil
}

func (b *cachedBucket)Update(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[string] {
	return b.Add(ctx, entries...)
}

func (b *cachedBucket)Remove(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[string] {
	keys := make([]string, len(names))
	for i, name := range names {
		keys[i] = formatKey(name)
	}
	// Remove from cache.
	b.redisCache.Delete(ctx, keys...)
	// Remove from AWS S3 bucket.
	return b.bucketStore.Remove(ctx, names...)
}

func formatKey(key string) string {
	return fmt.Sprintf("AWSS3%s", key)
}
