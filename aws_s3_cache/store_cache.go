package aws_s3_cache

import (
	"context"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/aws_s3"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_cfs"
)

type storeCache struct {
	store *aws_s3.S3Bucket
	cache btree.BtreeInterface[string, []byte]
	refreshInterval  time.Duration
	maxCacheableSize int
}

type cacheObject struct {
	Object          *aws_s3.S3Object
	LastRefreshTime time.Time
}

// Now returns the current time and can be "synthesized" if needed.
var Now = time.Now

const(
	eTagCheckInterval = time.Duration(15 * time.Minute)
)

func NewStoreCache(ctx context.Context, transaction sop.Transaction, name string, blobStoreBaseFolderPath string, refreshInterval time.Duration, cacheExpiry time.Duration) (KeyValueStore[string, []byte], error) {
	// Minimum refresh interval is 15 mins.
	if refreshInterval < 0 || (refreshInterval > 0 && refreshInterval < eTagCheckInterval) {
		refreshInterval = eTagCheckInterval
	}
	// Defaults to 2hr cache expiry.
	if cacheExpiry < 0 || (cacheExpiry > 0 && cacheExpiry < eTagCheckInterval) {
		cacheExpiry = time.Duration(2 * time.Hour)
	}

	storeCacheConfig := *sop.NewStoreCacheConfig(cacheExpiry, true)
	// Create/Open the Btree.
	b3, err := in_red_cfs.NewBtree[string, []byte](ctx, sop.StoreOptions{
		Name:                     name,
		SlotLength:               400,
		IsValueDataInNodeSegment: true,
		BlobStoreBaseFolderPath:  blobStoreBaseFolderPath,
		CacheConfig:              &storeCacheConfig,
	}, transaction)
	if err != nil {
		return nil, err
	}
	store, err := aws_s3.NewBucketAsStore(S3Client)
	if err != nil {
		return nil, err
	}
	return &storeCache{
		store: store,
		cache: b3,
	}, nil
}

// Fetch entry(ies) with given key(s).
// Fetch term is used here because this CRUD interface is NOT part of the B-Tree system, thus, the context is
// to "fetch" from the remote data storage sub-system like AWS S3.
func (s *storeCache) Fetch(ctx context.Context, keys ...string) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	result := sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{}
	result.Details = make([]sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]], len(keys))
	//needsReFetch := make(map[string]int)
	for i := range keys {
		var v []byte
		var err error
		var ok bool
		if ok, err = s.cache.FindOne(ctx, keys[i], false); !ok || err != nil {

			continue
		}
		if v, err = s.cache.GetCurrentValue(ctx); err != nil {
			continue
		}
		// Package the result from cache.
		result.Details[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]]{
			Payload: sop.KeyValuePair[string, []byte]{
				Key:   keys[i],
				Value: v,
			},
		}
	}
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{}
}

// Fetch a large entry with the given key.
func (s *storeCache) FetchLargeObject(context.Context, string) ([]byte, error) {
	return nil, nil
}

// Add entry(ies) to the store.
func (s *storeCache) Add(context.Context, ...sop.KeyValuePair[string, []byte]) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{}
}

// Update entry(ies) of the store.
func (s *storeCache) Update(context.Context, ...sop.KeyValuePair[string, []byte]) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{}
}

// Remove entry(ies) from the store given their names.
func (s *storeCache) Remove(context.Context, ...string) sop.KeyValueStoreResponse[string] {
	return sop.KeyValueStoreResponse[string]{}
}

func (b *cachedBucket) Add(ctx context.Context, bucketName string, entries ...sop.KeyValuePair[string, []byte]) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	r := make([]sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]], len(entries))
	var lastError error
	now := Now()
	for i := range entries {
		res := b.bucketStore.Add(ctx, bucketName, sop.KeyValuePair[string, *S3Object]{
			Key: entries[i].Key,
			Value: &S3Object{
				Data: entries[i].Value,
			},
		})
		r[i].Error = res.Details[0].Error
		r[i].Payload = sop.KeyValuePair[string, []byte]{
			Key: res.Details[0].Payload.Key,
		}

		// Encache if there is no error.
		if res.Error == nil {
			if b.isCacheableSize(entries[i].Value) {
				// Cache to Redis if not a large object.
				cd := cacheObject{
					Object:          res.Details[0].Payload.Value,
					LastRefreshTime: now,
				}
				k := b.formatKey(bucketName, entries[i].Key)
				if err := b.redisCache.SetStruct(ctx, k, cd, b.cacheExpiry); err != nil {
					log.Warn(fmt.Sprintf("redis setstruct for key %s failed, details: %v", k, err))
				}
			}
			continue
		}
		lastError = res.Details[0].Error
	}
	if lastError != nil {
		return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{
			Error:   fmt.Errorf("failed to completely add items to bucket %s, last error: %v", bucketName, lastError),
			Details: r,
		}
	}
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{
		Details: r,
	}
}

func (b *cachedBucket) Update(ctx context.Context, bucketName string, entries ...sop.KeyValuePair[string, []byte]) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	return b.Add(ctx, bucketName, entries...)
}

func (b *cachedBucket) Remove(ctx context.Context, bucketName string, names ...string) sop.KeyValueStoreResponse[string] {
	keys := make([]string, len(names))
	for i, name := range names {
		keys[i] = b.formatKey(bucketName, name)
	}
	// Remove from cache.
	err := b.redisCache.Delete(ctx, keys...)
	if err != nil {
		log.Warn(fmt.Sprintf("redis deletes for bucket %s failed, details: %v", bucketName, err))
	}
	// Remove from AWS S3 bucket.
	return b.bucketStore.Remove(ctx, bucketName, names...)
}

func (b *cachedBucket) formatKey(bucketName string, key string) string {
	return fmt.Sprintf("s3%s%s", bucketName, key)
}

// Cacheable size is < 500 MB.
func (b *cachedBucket) isCacheableSize(data []byte) bool {
	return len(data) <= b.maxCacheableSize
}







func (b *cachedBucket) fetch(ctx context.Context, bucketName string, isLargeObjects bool, names ...string) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	r := make([]sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]], len(names))
	now := Now()
	var lastError error
	for i := range names {
		var t cacheObject
		err := b.redisCache.GetStruct(ctx, b.formatKey(bucketName, names[i]), &t)
		if redis.KeyNotFound(err) || err != nil {
			res := b.fetchAndCache(ctx, bucketName, names[i], now, false)
			r[i] = res.Details[0]
			if r[i].Error != nil {
				if !redis.KeyNotFound(err) {
					// Tolerate Redis cache failure.
					k := b.formatKey(bucketName, names[i])
					if err := b.redisCache.Delete(ctx, k); err != nil {
						log.Warn(fmt.Sprintf("redis delete for key %s failed, details: %v", k, err))
					}
				}
				lastError = r[i].Error
			}
			continue
		}
		// Package for return the cache copy since it is not time to refetch.
		if now.Sub(t.LastRefreshTime) <= b.refreshInterval {
			r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]]{
				Payload: sop.KeyValuePair[string, []byte]{
					Key:   names[i],
					Value: t.Object.Data,
				},
			}
			continue
		}
		// Read object's ETag from S3 bucket.
		result, err := b.bucketStore.S3Client.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucketName),
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
				Object:          t.Object,
				LastRefreshTime: now,
			}
			k := b.formatKey(bucketName, names[i])
			if err := b.redisCache.SetStruct(ctx, k, cd, b.cacheExpiry); err != nil {
				log.Warn(fmt.Sprintf("redis setstruct for key %s failed, details: %v", k, err))
			}
			r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]]{
				Payload: sop.KeyValuePair[string, []byte]{
					Key:   names[i],
					Value: t.Object.Data,
				},
			}
			continue
		}
		// Different or unknown ETag, refetch and recache.
		res := b.fetchAndCache(ctx, bucketName, names[i], now, isLargeObjects)
		r[i] = res.Details[0]
		if r[i].Error != nil {
			k := b.formatKey(bucketName, names[i])
			if err := b.redisCache.Delete(ctx, k); err != nil {
				log.Warn(fmt.Sprintf("redis setstruct for key %s failed, details: %v", k, err))
			}
			lastError = r[i].Error
		}
	}
	if lastError != nil {
		return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{
			Error:   fmt.Errorf("failed to completely fetch(large:%v) from bucket %s, details: %v", isLargeObjects, bucketName, lastError),
			Details: r,
		}
	}
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{
		Details: r,
	}
}

func (b *cachedBucket) fetchAndCache(ctx context.Context, bucketName string, name string, now time.Time, isLargeObject bool) sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]] {
	// Refetch, recache if not large and package for return.
	var res sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]
	if !isLargeObject {
		r := b.bucketStore.Fetch(ctx, bucketName, name)
		res = r.Details[0]
	} else {
		r, err := b.bucketStore.FetchLargeObject(ctx, bucketName, name)
		res = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]{
			Payload: sop.KeyValuePair[string, *S3Object]{
				Key:   name,
				Value: r,
			},
			Error: err,
		}
	}
	if res.Error != nil {
		return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{
			Details: []sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]]{
				{
					Payload: sop.KeyValuePair[string, []byte]{
						Key: name,
					},
					Error: res.Error,
				},
			},
		}
	}
	if b.isCacheableSize(res.Payload.Value.Data) {
		// Cache to Redis if not a large object.
		cd := cacheObject{
			Object:          res.Payload.Value,
			LastRefreshTime: now,
		}
		k := b.formatKey(bucketName, name)
		if err := b.redisCache.SetStruct(ctx, k, cd, b.cacheExpiry); err != nil {
			log.Warn(fmt.Sprintf("redis setstruct for key %s failed, details: %v", k, err))
		}
	}
	// Package to return the newly fetched object.
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, []byte]]{
		Details: []sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, []byte]]{
			{
				Payload: sop.KeyValuePair[string, []byte]{
					Key:   name,
					Value: res.Payload.Value.Data,
				},
			},
		},
	}
}
