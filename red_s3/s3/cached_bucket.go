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
			// Refetch, recache if not large and package for return.
			res := b.bucketStore.Fetch(ctx, names[i])
			if res[0].Error != nil {
				r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
					Payload: sop.KeyValuePair[string, []byte]{
						Key: names[i],
					},
					Error: res[0].Error,
				}
				if !redis.KeyNotFound(err) {
					b.redisCache.Delete(ctx, formatKey(names[i]))
				}
				continue
			}
			if !isLargeObject(res[0].Payload.Value.Data) {
				// Cache to Redis if not a large object.
				cd := cacheObject{
					Object: res[0].Payload.Value,
					LastRefreshTime: now,
				}
				b.redisCache.SetStruct(ctx, formatKey(names[i]), cd, 0)
			}
			// Package to return the newly fetched object.
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
				Payload: sop.KeyValuePair[string, []byte]{
					Key: names[i],
					Value: res[0].Payload.Value.Data,
				},
			}
			continue
		}
		// Package for return the cache copy since it is not time to refetch.
		if now.Sub(t.LastRefreshTime) <= b.refreshInterval {
			// Package to return the cached copy since it has not been changed.
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
		// If object's ETag is different then refetch it & update cache.
		if *result.ETag == t.Object.ETag {
			// Package to return the cached copy since it has not been changed.
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
				Payload: sop.KeyValuePair[string, []byte]{
					Key: names[i],
					Value: t.Object.Data,
				},
			}
			continue
		}
		// Object had changed, refetch, recache if not large and package for return.
		res := b.bucketStore.Fetch(ctx, names[i])
		if res[0].Error != nil {
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
				Payload: sop.KeyValuePair[string, []byte]{
					Key: names[i],
				},
				Error: res[0].Error,
			}
			b.redisCache.Delete(ctx, formatKey(names[i]))
			continue
		}
		if !isLargeObject(res[0].Payload.Value.Data) {
			// Cache to Redis if not a large object.
			cd := cacheObject{
				Object: res[0].Payload.Value,
				LastRefreshTime: now,
			}
			b.redisCache.SetStruct(ctx, formatKey(names[i]), cd, 0)
		}
		// Package to return the newly fetched object.
		r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
			Payload: sop.KeyValuePair[string, []byte]{
				Key: names[i],
				Value: res[0].Payload.Value.Data,
			},
		}
	}

	return r
}

// Fetch a large entry with the given name. NOTE: no caching, straight fetch from S3.
func (b *cachedBucket)FetchLargeObject(ctx context.Context, name string) ([]byte, error) {
	r, err := b.bucketStore.FetchLargeObject(ctx, name)
	return r.Data, err
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
			if !isLargeObject(entries[i].Value) {
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
