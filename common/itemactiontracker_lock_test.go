package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

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
