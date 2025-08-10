package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
	"github.com/sharedcode/sop/encoding"
)

func Test_ItemActionTracker_Add_ActivelyPersisted_PersistsAndCaches(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{
		Name:                         "iat_add_active",
		SlotLength:                   8,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: true,
		IsValueDataGloballyCached:    true,
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("john", "doe", "male", "j@d", "111")
	item := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	oldVersion := item.Version
	if err := tracker.Add(ctx, item); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	// Version should increment
	if item.Version != oldVersion+1 {
		t.Fatalf("version not incremented: got %d want %d", item.Version, oldVersion+1)
	}
	// Blob should be written immediately and cache populated
	if ba, _ := mockNodeBlobStore.GetOne(ctx, si.BlobTable, item.ID); len(ba) == 0 {
		t.Fatalf("blob not written for actively persisted add")
	}
	var out Person
	if ok, _ := mockRedisCache.GetStruct(ctx, formatItemKey(item.ID.String()), &out); !ok {
		t.Fatalf("cache not populated for actively persisted add")
	}
}

func Test_ItemActionTracker_Get_CacheHit_TTL(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{
		Name:                      "iat_get_cache_ttl",
		SlotLength:                8,
		IsValueDataInNodeSegment:  false,
		IsValueDataGloballyCached: true,
	}
	si := sop.NewStoreInfo(so)
	si.CacheConfig.IsValueDataCacheTTL = true
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("tim", "berners-lee", "male", "t@b", "222")
	id := sop.NewUUID()
	// Pre-seed cache only
	_ = mockRedisCache.SetStruct(ctx, formatItemKey(id.String()), &p, si.CacheConfig.ValueDataCacheDuration)
	it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, ValueNeedsFetch: true}
	if err := tracker.Get(ctx, it); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if it.Value == nil || !(*it.Value == p) || it.ValueNeedsFetch {
		t.Fatalf("Get did not hydrate from cache; val=%v needsFetch=%v", it.Value, it.ValueNeedsFetch)
	}
}

func Test_ItemActionTracker_Get_FallbackFromBlob_PopulatesCache(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{
		Name:                      "iat_get_blob",
		SlotLength:                8,
		IsValueDataInNodeSegment:  false,
		IsValueDataGloballyCached: true,
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("grace", "hopper", "female", "g@h", "333")
	id := sop.NewUUID()
	// Pre-seed blob only
	b := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: &p}
	if err := mockNodeBlobStore.Add(ctx, []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{{
		BlobTable: si.BlobTable,
		Blobs:     []sop.KeyValuePair[sop.UUID, []byte]{{Key: id, Value: must(encoding.Marshal(p))}},
	}}); err != nil {
		t.Fatalf("seed blob: %v", err)
	}
	b.Value = nil
	b.ValueNeedsFetch = true
	if err := tracker.Get(ctx, b); err != nil {
		t.Fatalf("Get error: %v", err)
	}
	// Value should be hydrated and cache populated
	if b.Value == nil || *b.Value != p || b.ValueNeedsFetch {
		t.Fatalf("blob fallback not applied")
	}
	var out Person
	if ok, _ := mockRedisCache.GetStruct(ctx, formatItemKey(id.String()), &out); !ok {
		t.Fatalf("cache not populated from blob fallback")
	}
}

// must is a helper for tests to ignore marshal error in deterministic structs
func must(b []byte, _ error) []byte { return b }
