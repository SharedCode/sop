package s3

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/redis"
)

type cachedBucket struct {
	redisCache redis.Cache
	bucketStore *s3Bucket
	refreshInterval time.Duration
	cacheExpiry time.Duration
	maxCacheableSize int
}

type cacheObject struct {
	Object *s3Object
	LastRefreshTime time.Time
}

// Now returns the current time and can be "synthesized" if needed.
var Now = time.Now

// NewCacheBucket is synonymous to NewCacheBucketExt but sets to use default values for the extended parameters.
func NewCachedBucket(ctx context.Context, bucketName string) (sop.KeyValueStore[string, []byte], error) {
	return NewCachedBucketExt(ctx, bucketName, -1, -1, 0)
}

// NewCacheBucketExt returns a KeyValueStore that adds caching on top of the AWS S3 bucket "store".
// Keep the bucketName short & set refreshInterval to decent period like ever 5mins "etag" check
// and cacheExpiry to longer time(5 hrs?) or no expiry(0). maxCacheableSize defaults to 500MB.
func NewCachedBucketExt(ctx context.Context, bucketName string, refreshInterval time.Duration, cacheExpiry time.Duration, maxCacheableSize int) (sop.KeyValueStore[string, []byte], error) {
	if bucketName == "" {
		return nil, fmt.Errorf("can't use empty string bucketName")
	}
	bs, err := newBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	// Minimum refresh interval is 5 seconds, if less then assign 5 minute refresh interval.
	if refreshInterval < 0 || (refreshInterval > 0 && refreshInterval < time.Duration(5*time.Second)) {
		refreshInterval = time.Duration(5*time.Minute)
	}
	// Defaults to 2hr cache expiry.
	if cacheExpiry < 0 || (cacheExpiry > 0 && cacheExpiry < time.Duration(1*time.Minute)) {
		cacheExpiry = time.Duration(2*time.Hour)
	}

	// Defaults cacheable size to 500MB.
	if maxCacheableSize <= 0 {
		maxCacheableSize = 500 * 1024 * 1024
	}

	return &cachedBucket{
		redisCache: redis.NewClient(),
		bucketStore: bs,
		refreshInterval: refreshInterval,
		cacheExpiry: cacheExpiry,
		maxCacheableSize: maxCacheableSize,
	}, nil
}

// Fetch entry(ies) with given name(s).
// Fetch term is used here because this CRUD interface is NOT part of the B-Tree system, thus, the context is
// to "fetch" from the remote data storage sub-system like AWS S3.
func (b *cachedBucket)Fetch(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	return b.fetch(ctx, false, names...)
}

// Fetch a large entry with the given name.
func (b *cachedBucket)FetchLargeObject(ctx context.Context, name string) ([]byte, error) {
	r := b.fetch(ctx, true, name)
	return r[0].Payload.Value, r[0].Error
}

func (b *cachedBucket)fetch(ctx context.Context, isLargeObjects bool, names ...string) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	r := make([]sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]], len(names))
	now := Now()
	for i := range names {
		var t cacheObject
		err := b.redisCache.GetStruct(ctx, b.formatKey(names[i]), &t)
		if redis.KeyNotFound(err) || err != nil{
			r[i] = b.fetchAndCache(ctx, names[i], now, false)
			if r[i].Error != nil {
				if !redis.KeyNotFound(err) {
					b.redisCache.Delete(ctx, b.formatKey(names[i]))
				}
			}
			continue
		}
		// Package for return the cache copy since it is not time to refetch.
		if now.Sub(t.LastRefreshTime) <= b.refreshInterval {
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
				Payload: sop.KeyValuePair[string, []byte]{
					Key: names[i],
					Value: t.Object.Data,
				},
			}
			continue
		}
		// Read object's ETag from S3 bucket.
		result, err := b.bucketStore.S3Client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(b.bucketStore.bucketName),
			Key:    aws.String(names[i]),
			ObjectAttributes: []types.ObjectAttributes{
				types.ObjectAttributesEtag,
			},
		})
		etag := ""
		if err == nil {
			etag = *result.ETag
		}
		// On error, etag will be set to empty thus, causing a refetch.

		// If object's ETag is same then not time yet to refetch.
		if etag == t.Object.ETag {
			// Update cache's last refresh time.
			cd := cacheObject{
				Object: t.Object,
				LastRefreshTime: now,
			}
			b.redisCache.SetStruct(ctx, b.formatKey(names[i]), cd, b.cacheExpiry)

			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
				Payload: sop.KeyValuePair[string, []byte]{
					Key: names[i],
					Value: t.Object.Data,
				},
			}
			continue
		}
		// Different or unknown ETag, refetch and recache.
		r[i] = b.fetchAndCache(ctx, names[i], now, isLargeObjects)
		if r[i].Error != nil {
			b.redisCache.Delete(ctx, b.formatKey(names[i]))
		}
	}

	return r
}

func (b *cachedBucket)fetchAndCache(ctx context.Context, name string, now time.Time, isLargeObject bool) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	// Refetch, recache if not large and package for return.
	var res sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]]
	if !isLargeObject {
		r := b.bucketStore.Fetch(ctx, name)
		res = r[0]
	} else {
		r, err := b.bucketStore.FetchLargeObject(ctx, name)
		res = sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
			Payload: sop.KeyValuePair[string, *s3Object]{
				Key: name,
				Value: r,
			},
			Error: err,
		}
	}
	if res.Error != nil {
		return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
			Payload: sop.KeyValuePair[string, []byte]{
				Key: name,
			},
			Error: res.Error,
		}
	}
	if b.isCacheableSize(res.Payload.Value.Data) {
		// Cache to Redis if not a large object.
		cd := cacheObject{
			Object: res.Payload.Value,
			LastRefreshTime: now,
		}
		b.redisCache.SetStruct(ctx, b.formatKey(name), cd, b.cacheExpiry)
	}
	// Package to return the newly fetched object.
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
		Payload: sop.KeyValuePair[string, []byte]{
			Key: name,
			Value: res.Payload.Value.Data,
		},
	}
}

func (b *cachedBucket)Add(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	r := make([]sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]], len(entries))

	now := Now()
	for i := range entries {
		res := b.bucketStore.Add(ctx, sop.KeyValuePair[string,*s3Object]{
			Key: entries[i].Key,
			Value: &s3Object{
				Data: entries[i].Value,
			},
		})
		r[i].Error = res[0].Error
		r[i].Payload = sop.KeyValuePair[string, []byte]{
			Key: res[0].Payload.Key,
		}

		// Encache if there is no error.
		if res[0].Error == nil {
			if b.isCacheableSize(entries[i].Value) {
				// Cache to Redis if not a large object.
				cd := cacheObject{
					Object: res[0].Payload.Value,
					LastRefreshTime: now,
				}
				b.redisCache.SetStruct(ctx, b.formatKey(entries[i].Key), cd, b.cacheExpiry)
			}
		}
	}

	return r
}

func (b *cachedBucket)Update(ctx context.Context, entries ...sop.KeyValuePair[string, []byte]) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	return b.Add(ctx, entries...)
}

func (b *cachedBucket)Remove(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[string] {
	keys := make([]string, len(names))
	for i, name := range names {
		keys[i] = b.formatKey(name)
	}
	// Remove from cache.
	b.redisCache.Delete(ctx, keys...)
	// Remove from AWS S3 bucket.
	return b.bucketStore.Remove(ctx, names...)
}

func (b *cachedBucket)formatKey(key string) string {
	return fmt.Sprintf("s3%s%s", b.bucketStore.bucketName, key)
}

// Cacheable size is < 500 MB.
func (b *cachedBucket)isCacheableSize(data []byte) bool {
	return len(data) <= b.maxCacheableSize
}
