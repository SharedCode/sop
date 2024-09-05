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
}

type cacheObject struct {
	Object *s3Object
	LastRefreshTime time.Time
}

// Now returns the current time and can be "synthesized" if needed.
var Now = time.Now

// NewCacheBucket returns a KeyValueStore that adds caching on top of the AWS S3 bucket "store".
func NewCachedBucket(ctx context.Context, bucketName string, refreshInterval time.Duration) (sop.KeyValueStore[string, []byte], error) {
	bs, err := newBucket(ctx, bucketName)
	if err != nil {
		return nil, err
	}
	// Minimum refresh interval is 5 seconds, if less then assign 5 minute refresh interval.
	if refreshInterval > 0 && refreshInterval < time.Duration(5*time.Second) {
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
	r := make([]sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]], len(names))
	now := Now()
	for i := range names {
		var t cacheObject
		err := b.redisCache.GetStruct(ctx, formatKey(names[i]), &t)
		if redis.KeyNotFound(err) || err != nil{
			r[i] = b.fetchAndCache(ctx, names[i], now, false)
			if r[i].Error != nil {
				if !redis.KeyNotFound(err) {
					b.redisCache.Delete(ctx, formatKey(names[i]))
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
		if err != nil {
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
				Payload: sop.KeyValuePair[string, []byte]{
					Key: names[i],
				},
				Error: err,
			}
			// Remove from Redis if S3 reported can't get Object attributes from the bucket.
			b.redisCache.Delete(ctx, formatKey(names[i]))
			continue
		}
		// If object's ETag is same then not time yet to refetch.
		if *result.ETag == t.Object.ETag {
			// Update cache's last refresh time.
			cd := cacheObject{
				Object: t.Object,
				LastRefreshTime: now,
			}
			b.redisCache.SetStruct(ctx, formatKey(names[i]), cd, 0)

			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
				Payload: sop.KeyValuePair[string, []byte]{
					Key: names[i],
					Value: t.Object.Data,
				},
			}
			continue
		}
		// Different ETag, refetch and recache.	
		r[i] = b.fetchAndCache(ctx, names[i], now, false)
		if r[i].Error != nil {
			b.redisCache.Delete(ctx, formatKey(names[i]))
		}
	}

	return r
}

// Fetch a large entry with the given name. NOTE: no caching, straight fetch from S3.
func (b *cachedBucket)FetchLargeObject(ctx context.Context, name string) ([]byte, error) {
	var t cacheObject
	now := Now()
	err := b.redisCache.GetStruct(ctx, formatKey(name), &t)
	if redis.KeyNotFound(err) || err != nil{
		r := b.fetchAndCache(ctx, name, now, true)
		if r.Error != nil {
			if !redis.KeyNotFound(err) {
				b.redisCache.Delete(ctx, formatKey(name))
			}
		}
		return r.Payload.Value, r.Error
	}
	// Package for return the cache copy since it is not time to refetch.
	if now.Sub(t.LastRefreshTime) <= b.refreshInterval {
		return t.Object.Data, nil
	}
	// Read object's ETag from S3 bucket.
	result, err := b.bucketStore.S3Client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
		Bucket: aws.String(b.bucketStore.bucketName),
		Key:    aws.String(name),
		ObjectAttributes: []types.ObjectAttributes{
			types.ObjectAttributesEtag,
		},
	})
	if err != nil {
		// Remove from Redis if S3 reported can't get Object attributes from the bucket.
		b.redisCache.Delete(ctx, formatKey(name))
		return nil, err
	}
	// If object's ETag is same then not time yet to refetch.
	if *result.ETag == t.Object.ETag {
		// Update cache's last refresh time.
		cd := cacheObject{
			Object: t.Object,
			LastRefreshTime: now,
		}
		b.redisCache.SetStruct(ctx, formatKey(name), cd, 0)

		return t.Object.Data, nil
	}
	// Different ETag, refetch and recache.	
	r := b.fetchAndCache(ctx, name, now, true)
	if r.Error != nil {
		b.redisCache.Delete(ctx, formatKey(name))
	}
	return r.Payload.Value, r.Error
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
	if isCacheableSize(res.Payload.Value.Data) {
		// Cache to Redis if not a large object.
		cd := cacheObject{
			Object: res.Payload.Value,
			LastRefreshTime: now,
		}
		b.redisCache.SetStruct(ctx, formatKey(name), cd, 0)
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
			if isCacheableSize(entries[i].Value) {
				// Cache to Redis if not a large object.
				cd := cacheObject{
					Object: res[0].Payload.Value,
					LastRefreshTime: now,
				}
				b.redisCache.SetStruct(ctx, formatKey(entries[i].Key), cd, 0)
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

// Cacheable size is < 500 MB.
func isCacheableSize(data []byte) bool {
	return len(data) <= 500 * 1024 * 1024
}
