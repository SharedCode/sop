package common

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

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
