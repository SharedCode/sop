package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
)

// Covers deleteTrackedItemsValues for both cache-deletion and skip-cache branches.
func Test_Transaction_DeleteTrackedItemsValues_CacheAndNoCache(t *testing.T) {
	ctx := context.Background()
	// Set up isolated mocks
	redis := mocks.NewMockClient()
	cache.NewGlobalCache(redis, cache.DefaultMinCapacity, cache.DefaultMaxCapacity)
	blobs := mocks.NewMockBlobStore()
	tx := &Transaction{l2Cache: redis, blobStore: blobs}

	// Two blob IDs, one globally cached, one not
	cachedID := sop.NewUUID()
	nonCachedID := sop.NewUUID()

	// Seed blobs
	_ = blobs.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		{BlobTable: "it", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: cachedID, Value: []byte("c")}}},
		{BlobTable: "it", Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: nonCachedID, Value: []byte("n")}}},
	})
	// Seed cache for the first id
	type stub struct{ A int }
	_ = redis.SetStruct(ctx, formatItemKey(cachedID.String()), &stub{A: 1}, time.Minute)

	payload := []sop.Tuple[bool, sop.BlobsPayload[sop.UUID]]{
		{First: true, Second: sop.BlobsPayload[sop.UUID]{BlobTable: "it", Blobs: []sop.UUID{cachedID}}},
		{First: false, Second: sop.BlobsPayload[sop.UUID]{BlobTable: "it", Blobs: []sop.UUID{nonCachedID}}},
	}

	if err := tx.deleteTrackedItemsValues(ctx, payload); err != nil {
		t.Fatalf("deleteTrackedItemsValues error: %v", err)
	}

	// Cache for cachedID should be removed
	var x stub
	if ok, _ := redis.GetStruct(ctx, formatItemKey(cachedID.String()), &x); ok {
		t.Fatalf("cache not deleted for cachedID")
	}
	// Blobs for both should be removed
	if ba, _ := blobs.GetOne(ctx, "it", cachedID); len(ba) != 0 {
		t.Fatalf("cachedID blob not removed")
	}
	if ba, _ := blobs.GetOne(ctx, "it", nonCachedID); len(ba) != 0 {
		t.Fatalf("nonCachedID blob not removed")
	}
}
