package common

// Consolidated extra scenarios for itemactiontracker:
// - lock compatibility vs conflict
// - checkTrackedItems outcomes
// - manage branches (persisted flag, update with fetch, add with/without value)

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

// buildTracker constructs a tracker with a fresh store info and mock deps.
func buildTracker(name string) (*itemActionTracker[PersonKey, Person], *sop.StoreInfo) {
	so := sop.StoreOptions{Name: name, SlotLength: 8, IsValueDataInNodeSegment: false, IsValueDataGloballyCached: true}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), true)
	tr := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, tl)
	return tr, si
}

func Test_ItemActionTracker_Lock_CompatibilityAndConflicts(t *testing.T) {
	ctx := context.Background()
	tracker, _ := buildTracker("iat_lock")

	// Prepare one item tracked with getAction
	idGet := sop.NewUUID()
	pk, p := newPerson("l", "get", "g", "e", "ph")
	itGet := &btree.Item[PersonKey, Person]{ID: idGet, Key: pk, Value: &p}
	if err := tracker.Get(ctx, itGet); err != nil {
		t.Fatalf("Get setup err: %v", err)
	}

	// In Redis, pre-set a GET lock with different LockID (compatible)
	lrGet := lockRecord{LockID: sop.NewUUID(), Action: getAction}
	if err := tracker.cache.SetStruct(ctx, tracker.cache.FormatLockKey(idGet.String()), &lrGet, time.Minute); err != nil {
		t.Fatalf("seed get lock err: %v", err)
	}

	// Another item tracked with updateAction
	idUpd := sop.NewUUID()
	itUpd := &btree.Item[PersonKey, Person]{ID: idUpd, Key: pk, Value: &p}
	if err := tracker.Update(ctx, itUpd); err != nil {
		t.Fatalf("Update setup err: %v", err)
	}
	// In Redis, pre-set a GET lock (incompatible with update)
	lrUpd := lockRecord{LockID: sop.NewUUID(), Action: getAction}
	if err := tracker.cache.SetStruct(ctx, tracker.cache.FormatLockKey(idUpd.String()), &lrUpd, time.Minute); err != nil {
		t.Fatalf("seed upd lock err: %v", err)
	}

	// And one more item without any Redis record (new lock path)
	idNew := sop.NewUUID()
	itNew := &btree.Item[PersonKey, Person]{ID: idNew, Key: pk, Value: &p}
	if err := tracker.Update(ctx, itNew); err != nil {
		t.Fatalf("Update(new) setup err: %v", err)
	}

	// Attempt to lock all tracked items
	err := tracker.lock(ctx, time.Second)
	if err == nil {
		t.Fatalf("expected conflict error for update-vs-get lock, got nil")
	}

	// Remove the conflicting one and ensure the remaining can be locked
	delete(tracker.items, idUpd)
	if err := tracker.lock(ctx, time.Second); err != nil {
		t.Fatalf("unexpected lock error after removing conflict: %v", err)
	}
	// New item should have ownership after creating lock
	if !tracker.items[idNew].isLockOwner {
		t.Fatalf("expected new item lock ownership")
	}
}

func Test_ItemActionTracker_CheckTrackedItems_ReportsConflictAndOK(t *testing.T) {
	ctx := context.Background()
	tracker, _ := buildTracker("iat_check")

	// Track one GET and one UPDATE
	idA := sop.NewUUID()
	idB := sop.NewUUID()
	pk, p := newPerson("c", "k", "g", "e", "ph")
	itA := &btree.Item[PersonKey, Person]{ID: idA, Key: pk, Value: &p}
	_ = tracker.Get(ctx, itA)
	itB := &btree.Item[PersonKey, Person]{ID: idB, Key: pk, Value: &p}
	_ = tracker.Update(ctx, itB)

	// Redis states: for A, another GET (compatible); for B, another UPDATE lock with different LockID (conflict)
	_ = tracker.cache.SetStruct(ctx, tracker.cache.FormatLockKey(idA.String()), &lockRecord{LockID: sop.NewUUID(), Action: getAction}, time.Minute)
	_ = tracker.cache.SetStruct(ctx, tracker.cache.FormatLockKey(idB.String()), &lockRecord{LockID: sop.NewUUID(), Action: updateAction}, time.Minute)

	err := tracker.checkTrackedItems(ctx)
	if err == nil {
		t.Fatalf("expected conflict reported for idB")
	}
}

func Test_ItemActionTracker_Manage_Branches(t *testing.T) {
	tracker, _ := buildTracker("iat_manage")

	// Case 1: persisted=true => no-op
	id := sop.NewUUID()
	item := &btree.Item[PersonKey, Person]{ID: id, Value: &Person{Email: "x"}}
	persisted := cacheItem[PersonKey, Person]{persisted: true, lockRecord: lockRecord{Action: addAction}, item: item}
	if kv, err := tracker.manage(id, persisted); err != nil || kv != nil {
		t.Fatalf("persisted branch failed, kv=%v err=%v", kv, err)
	}

	// Case 2: updateAction with ValueNeedsFetch set and value present -> mark old for deletion,
	// externalize new value and set ValueNeedsFetch
	id2 := sop.NewUUID()
	item2 := &btree.Item[PersonKey, Person]{ID: id2, Value: &Person{Email: "y"}, ValueNeedsFetch: true}
	upd := cacheItem[PersonKey, Person]{lockRecord: lockRecord{Action: updateAction}, item: item2}
	if kv, err := tracker.manage(id2, upd); err != nil || kv == nil {
		t.Fatalf("update branch expected kv add and no err; got kv=%v err=%v", kv, err)
	}
	if !item2.ValueNeedsFetch {
		t.Fatalf("expected ValueNeedsFetch set during manage(update)")
	}

	// Case 3: addAction with nil value => kv returned nil
	id3 := sop.NewUUID()
	item3 := &btree.Item[PersonKey, Person]{ID: id3}
	addNil := cacheItem[PersonKey, Person]{lockRecord: lockRecord{Action: addAction}, item: item3}
	if kv, err := tracker.manage(id3, addNil); err != nil || kv != nil {
		t.Fatalf("add(nil) branch expected no kv and no err; got kv=%v err=%v", kv, err)
	}

	// Case 4: addAction with value => kv returned and value externalized
	id4 := sop.NewUUID()
	pv := &Person{Email: "z"}
	item4 := &btree.Item[PersonKey, Person]{ID: id4, Value: pv}
	addVal := cacheItem[PersonKey, Person]{lockRecord: lockRecord{Action: addAction}, item: item4}
	if kv, err := tracker.manage(id4, addVal); err != nil || kv == nil || kv.Key.Compare(id4) != 0 {
		t.Fatalf("add(value) expected kv for id4; got kv=%v err=%v", kv, err)
	}
	if item4.Value != nil || !item4.ValueNeedsFetch {
		t.Fatalf("expected value externalized and ValueNeedsFetch set")
	}
}
