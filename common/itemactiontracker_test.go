package common

import (
	"context"
	"testing"
	"time"

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
		Name:                      "iat_add",
		SlotLength:                8,
		IsValueDataInNodeSegment:  false,
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

func Test_ItemActionTracker_CommitTrackedItemsValues_Update_MarksObsolete(t *testing.T) {
	ctx := context.Background()
	// Value data in separate segment and globally cached
	so := sop.StoreOptions{
		Name:                      "iat_update_mark_obsolete",
		SlotLength:                8,
		IsValueDataInNodeSegment:  false,
		IsValueDataGloballyCached: true,
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	iat := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	// Manually seed a tracked item in "update" state with existing externalized value (ValueNeedsFetch)
	pk, p := newPerson("seed", "s", "z", "s@z", "9")
	originalID := sop.NewUUID()
	item := &btree.Item[PersonKey, Person]{ID: originalID, Key: pk, Value: &p, ValueNeedsFetch: true}
	iat.items[originalID] = cacheItem[PersonKey, Person]{
		lockRecord:  lockRecord{LockID: sop.NewUUID(), Action: updateAction},
		item:        item,
		versionInDB: item.Version,
	}

	// Commit tracked values; this should mark originalID obsolete and externalize a new blob under a new ID
	if err := iat.commitTrackedItemsValues(ctx); err != nil {
		t.Fatalf("commitTrackedItemsValues update failed: %v", err)
	}

	// Obsolete list should include originalID
	obs := iat.getObsoleteTrackedItemsValues()
	if obs == nil || len(obs.Blobs) != 1 || obs.Blobs[0] != originalID {
		t.Fatalf("expected obsolete to include originalID, got: %+v", obs)
	}
	// Item should be re-externalized with a new ID and cached
	if item.Value != nil || !item.ValueNeedsFetch {
		t.Fatalf("expected externalized value after update")
	}
	if item.ID == originalID {
		t.Fatalf("expected new ID to be assigned on update")
	}
	// Blob for new ID should exist
	if ba, _ := mockNodeBlobStore.GetOne(ctx, si.BlobTable, item.ID); len(ba) == 0 {
		t.Fatalf("expected blob for new ID after update")
	}
}

func Test_ItemActionTracker_GetForRollback_And_Obsolete_NilWhenInNodeSegment(t *testing.T) {
	// When value data is co-located in node segment, both getters should return nil.
	so := sop.StoreOptions{
		Name:                     "iat_nil_segment",
		SlotLength:               8,
		IsValueDataInNodeSegment: true,
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	iat := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	if got := iat.getForRollbackTrackedItemsValues(); got != nil {
		t.Fatalf("expected nil getForRollbackTrackedItemsValues when in node segment")
	}
	if got := iat.getObsoleteTrackedItemsValues(); got != nil {
		t.Fatalf("expected nil getObsoleteTrackedItemsValues when in node segment")
	}
}

func Test_ItemActionTracker_Remove_ActivelyPersisted_QueuesForDeletion(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{
		Name:                         "iat_remove_active",
		SlotLength:                   8,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: true,
	}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("rm", "a", "m", "e", "ph")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tr.Remove(ctx, it); err != nil {
		t.Fatalf("Remove err: %v", err)
	}
	if len(tr.forDeletionItems) != 1 || tr.forDeletionItems[0] != it.ID {
		t.Fatalf("expected item queued for deletion, got %#v", tr.forDeletionItems)
	}
	if it.ValueNeedsFetch {
		t.Fatalf("expected ValueNeedsFetch=false")
	}
}

func Test_ItemActionTracker_Remove_AfterAdd_DropsFromTracked(t *testing.T) {
	ctx := context.Background()
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_remove_after_add", SlotLength: 8})
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)

	pk, p := newPerson("rm2", "b", "m", "e", "ph")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tr.Add(ctx, it); err != nil {
		t.Fatalf("Add err: %v", err)
	}
	if _, ok := tr.items[it.ID]; !ok {
		t.Fatalf("expected item tracked after Add")
	}
	if err := tr.Remove(ctx, it); err != nil {
		t.Fatalf("Remove err: %v", err)
	}
	if _, ok := tr.items[it.ID]; ok {
		t.Fatalf("expected item removed from tracking after remove of new item")
	}
}

// Ensure unlock is a no-op for items where we are not the lock owner.
func Test_ItemActionTracker_Unlock_NonOwner_NoOp(t *testing.T) {
	ctx := context.Background()
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_unlock_noop", SlotLength: 8})
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tracker := newItemActionTracker[PersonKey, Person](si, redis, blobs, tl)

	// Track one item
	pk, p := newPerson("u", "n", "k", "e", "p")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tracker.Update(ctx, it); err != nil {
		t.Fatal(err)
	}

	// Manually set a different lock owner in cache, and do not set isLockOwner
	other := lockRecord{LockID: sop.NewUUID(), Action: updateAction}
	if err := redis.SetStruct(ctx, redis.FormatLockKey(it.ID.String()), &other, time.Minute); err != nil {
		t.Fatal(err)
	}

	// unlock should not delete the key since we are not owners; no error expected
	if err := tracker.unlock(ctx); err != nil {
		t.Fatalf("unexpected error on unlock: %v", err)
	}
	var lr lockRecord
	found, err := redis.GetStruct(ctx, redis.FormatLockKey(it.ID.String()), &lr)
	if err != nil {
		t.Fatalf("redis get err: %v", err)
	}
	if !found {
		t.Fatalf("lock key should remain when not owner")
	}
}

func Test_ItemActionTracker_LockUnlock_SetsOwnershipAndDeletes(t *testing.T) {
	ctx := context.Background()
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_lock", SlotLength: 8})
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tracker := newItemActionTracker[PersonKey, Person](si, redis, blobs, tl)

	// Two tracked items marked for update
	pk1, p1 := newPerson("a", "a", "m", "e", "ph")
	it1 := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk1, Value: &p1}
	pk2, p2 := newPerson("b", "b", "m", "e", "ph")
	it2 := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk2, Value: &p2}
	if err := tracker.Update(ctx, it1); err != nil { t.Fatal(err) }
	if err := tracker.Update(ctx, it2); err != nil { t.Fatal(err) }

	if err := tracker.lock(ctx, time.Minute); err != nil {
		t.Fatalf("lock returned error: %v", err)
	}
	// Verify ownership flags set and then unlock removes keys
	for id := range tracker.items {
		var lr lockRecord
		// Found in cache before unlock
		found, err := redis.GetStruct(ctx, redis.FormatLockKey(id.String()), &lr)
		if err != nil || !found { t.Fatalf("expected lock in cache for %s", id.String()) }
	}
	if err := tracker.unlock(ctx); err != nil {
		t.Fatalf("unlock returned error: %v", err)
	}
	for id := range tracker.items {
		var lr lockRecord
		found, err := redis.GetStruct(ctx, redis.FormatLockKey(id.String()), &lr)
		if err != nil { t.Fatalf("redis get err: %v", err) }
		if found { t.Fatalf("expected lock key deleted for %s", id.String()) }
	}
}

func Test_ItemActionTracker_Lock_ConflictDetected(t *testing.T) {
	ctx := context.Background()
	si := sop.NewStoreInfo(sop.StoreOptions{Name: "iat_conflict", SlotLength: 8})
	redis := mocks.NewMockClient()
	blobs := mocks.NewMockBlobStore()
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tracker := newItemActionTracker[PersonKey, Person](si, redis, blobs, tl)

	pk, p := newPerson("c", "c", "m", "e", "ph")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tracker.Update(ctx, it); err != nil { t.Fatal(err) }
	// Pre-populate a different lock owner in cache to force conflict
	var other lockRecord
	other.LockID = sop.NewUUID()
	other.Action = updateAction
	if err := redis.SetStruct(ctx, redis.FormatLockKey(it.ID.String()), &other, time.Minute); err != nil {
		t.Fatalf("pre-set lock err: %v", err)
	}
	if err := tracker.lock(ctx, time.Minute); err == nil {
		t.Fatalf("expected lock conflict error, got nil")
	}
	if err := tracker.checkTrackedItems(ctx); err == nil {
		t.Fatalf("expected checkTrackedItems to report conflict")
	}
}
