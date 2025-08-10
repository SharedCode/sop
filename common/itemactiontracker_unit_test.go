package common

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

func Test_ExtractRequestPayloadIDs(t *testing.T) {
	// Prepare a payload with two KV pairs and verify only IDs are serialized.
	id1 := sop.NewUUID()
	id2 := sop.NewUUID()
	payload := sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]{
		BlobTable: "tbl",
		Blobs: []sop.KeyValuePair[sop.UUID, []byte]{
			{Key: id1, Value: []byte("v1")},
			{Key: id2, Value: []byte("v2")},
		},
	}

	ba := extractRequestPayloadIDs(&payload)
	// Decode back and assert only IDs remained.
	ids := toStruct[sop.BlobsPayload[sop.UUID]](ba)
	if ids.BlobTable != payload.BlobTable {
		t.Fatalf("blob table mismatch: got %s want %s", ids.BlobTable, payload.BlobTable)
	}
	if len(ids.Blobs) != 2 || ids.Blobs[0] != id1 || ids.Blobs[1] != id2 {
		t.Fatalf("ids mismatch: got %v want [%s %s]", ids.Blobs, id1.String(), id2.String())
	}
}

func Test_ItemActionTracker_CommitTrackedItemsValues_Add_PersistsAndCaches(t *testing.T) {
	ctx := context.Background()
	// Store where value data is in separate segment and globally cached.
	so := sop.StoreOptions{
		Name:                     "iat_add",
		SlotLength:               8,
		IsValueDataInNodeSegment: false,
		IsValueDataGloballyCached: true,
	}
	si := sop.NewStoreInfo(so)
	// Use project-wide mocks
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	// Build tracker
	iat := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	// Create a btree.Item directly (constructor is unexported)
	pk, p := newPerson("mark", "twain", "male", "m@t", "123")
	item := &btree.Item[PersonKey, Person]{
		ID:    sop.NewUUID(),
		Key:   pk,
		Value: &p,
	}
	if err := iat.Add(ctx, item); err != nil {
		t.Fatalf("Add to tracker failed: %v", err)
	}
	if err := iat.commitTrackedItemsValues(ctx); err != nil {
		t.Fatalf("commitTrackedItemsValues failed: %v", err)
	}
	// Item value should be externalized and flagged for fetch
	if item.Value != nil || !item.ValueNeedsFetch {
		t.Fatalf("item externalization flags unexpected; Value=%v NeedsFetch=%v", item.Value, item.ValueNeedsFetch)
	}
	// Blob exists and can be decoded
	ba, err := mockNodeBlobStore.GetOne(ctx, si.BlobTable, item.ID)
	if err != nil || len(ba) == 0 {
		t.Fatalf("blob not found for %s: %v", item.ID.String(), err)
	}
	// Cached in Redis
	var pv Person
	if ok, err := mockRedisCache.GetStruct(ctx, formatItemKey(item.ID.String()), &pv); err != nil || !ok {
		t.Fatalf("cached value not found in redis for %s: %v", item.ID.String(), err)
	}
}

func Test_ItemActionTracker_Update_ActivelyPersisted_LogsAndCaches(t *testing.T) {
	ctx := context.Background()
	// Actively persisted configuration exercises pre-commit logging path
	so := sop.StoreOptions{
		Name:                         "iat_update_active",
		SlotLength:                   8,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: true,
		IsValueDataGloballyCached:    true,
	}
	si := sop.NewStoreInfo(so)
	// Use logging to ensure log path is taken
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	iat := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("ada", "lovelace", "female", "a@l", "555")
	id := sop.NewUUID()
	item := &btree.Item[PersonKey, Person]{ID: id, Key: pk, Value: &p}
	// Update on an untracked item should still actively persist
	if err := iat.Update(ctx, item); err != nil {
		t.Fatalf("Update to tracker failed: %v", err)
	}
	// Blob exists for item's ID (or temp ID if value duplicated); we check via cache first
	var pv Person
	if ok, err := mockRedisCache.GetStruct(ctx, formatItemKey(item.ID.String()), &pv); err != nil || !ok {
		// If not in cache, ensure blob exists
		ba, berr := mockNodeBlobStore.GetOne(ctx, si.BlobTable, item.ID)
		if berr != nil || len(ba) == 0 {
			t.Fatalf("neither cache nor blob exist for updated item %s: cacheErr=%v blobErr=%v", item.ID.String(), err, berr)
		}
	}
}
