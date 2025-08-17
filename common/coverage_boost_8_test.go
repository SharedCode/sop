package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

func Test_ItemActionTracker_Get_TTL_CacheHit_SetsValue(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{
		Name:                      "iat_get_ttl_hit",
		SlotLength:                4,
		IsValueDataInNodeSegment:  false,
		IsValueDataGloballyCached: true,
		CacheConfig: &sop.StoreCacheConfig{
			IsValueDataCacheTTL:    true,
			ValueDataCacheDuration: time.Minute,
		},
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	// Prepare item and seed cache
	pk, pv := newPerson("tt", "l", "m", "e@x", "p")
	id := sop.NewUUID()
	it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: nil, ValueNeedsFetch: true}
	if err := mockRedisCache.SetStruct(ctx, formatItemKey(id.String()), &pv, time.Minute); err != nil {
		t.Fatalf("seed cache err: %v", err)
	}
	if err := tr.Get(ctx, it); err != nil {
		t.Fatalf("Get err: %v", err)
	}
	if it.Value == nil || it.ValueNeedsFetch {
		t.Fatalf("expected value set from cache and NeedsFetch=false")
	}
}

func Test_ItemActionTracker_Get_NonGlobal_FetchesFromBlob(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{
		Name:                      "iat_get_blob",
		SlotLength:                4,
		IsValueDataInNodeSegment:  false,
		IsValueDataGloballyCached: false,
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, pv := newPerson("nb", "l", "m", "e@x", "p")
	id := sop.NewUUID()
	it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: nil, ValueNeedsFetch: true}

	// Seed blob store with encoded value via tracker's Add path
	// Alternatively, marshal and call blob store directly
	// Use blob store directly for determinism
	ba, _ := encoding.BlobMarshaler.Marshal(pv)
	_ = mockNodeBlobStore.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{BlobTable: si.BlobTable, Blobs: []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: ba}}}})

	if err := tr.Get(ctx, it); err != nil {
		t.Fatalf("Get err: %v", err)
	}
	if it.Value == nil || it.ValueNeedsFetch {
		t.Fatalf("expected value set from blob and NeedsFetch=false")
	}
}
