package common

// Consolidated from: itemactiontracker_test.go, itemactiontracker_add_test.go
import (
	"context"
	"errors"
	"time"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

// Basic sanity covering Add, Get, Update, Remove paths using public tracker API.
func Test_ItemActionTracker_BasicPaths(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{Name: "iat_basic", SlotLength: 8, IsValueDataInNodeSegment: false, IsValueDataGloballyCached: true}
	si := sop.NewStoreInfo(so)
	tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, newTransactionLogger(mocks.NewMockTransactionLog(), false))
	pk, p := newPerson("iat", "basic", "1", "e@x", "p")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tracker.Add(ctx, it); err != nil {
		t.Fatalf("Add err: %v", err)
	}
	if it.Value == nil {
		t.Fatalf("expected value retained before commitTrackedItemsValues")
	}
	if err := tracker.commitTrackedItemsValues(ctx); err != nil {
		t.Fatalf("commitTrackedItemsValues err: %v", err)
	}
	if it.Value != nil || !it.ValueNeedsFetch {
		t.Fatalf("expected externalized value after commit, got value=%v needsFetch=%v", it.Value, it.ValueNeedsFetch)
	}
	// Simulate update
	updated := Person{Gender: p.Gender, Email: "new@x", Phone: p.Phone, SSN: p.SSN}
	it.Value = &updated
	it.ValueNeedsFetch = false
	if err := tracker.Update(ctx, it); err != nil {
		t.Fatalf("Update err: %v", err)
	}
	if err := tracker.commitTrackedItemsValues(ctx); err != nil {
		t.Fatalf("2nd commitTrackedItemsValues err: %v", err)
	}
	// Remove path
	if err := tracker.Remove(ctx, it); err != nil {
		t.Fatalf("Remove err: %v", err)
	}
}

type errBlob struct{ e error }

func (e *errBlob) GetOne(ctx context.Context, blobTable string, blobID sop.UUID) ([]byte, error) {
	return nil, e.e
}
func (e *errBlob) Add(ctx context.Context, blobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return e.e
}
func (e *errBlob) Update(ctx context.Context, blobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return e.e
}
func (e *errBlob) Remove(ctx context.Context, blobsIDs []sop.BlobsPayload[sop.UUID]) error {
	return e.e
}

func Test_ItemActionTracker_Add_Paths(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		inNode bool
	}{{"add_in_node", true}, {"add_out_of_node", false}}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			so := sop.StoreOptions{Name: "iat_add_" + c.name, SlotLength: 8, IsValueDataInNodeSegment: c.inNode, IsValueDataGloballyCached: !c.inNode}
			si := sop.NewStoreInfo(so)
			tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, newTransactionLogger(mocks.NewMockTransactionLog(), false))
			pk, p := newPerson("iat", c.name, "1", "e@x", "p")
			it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
			if err := tracker.Add(ctx, it); err != nil {
				t.Fatalf("Add err: %v", err)
			}
			if err := tracker.commitTrackedItemsValues(ctx); err != nil {
				t.Fatalf("commitTrackedItemsValues err: %v", err)
			}
			if c.inNode { // value data stored inline so commitTrackedItemsValues is a no-op
				if it.Value == nil || it.ValueNeedsFetch {
					t.Fatalf("expected inline value retained; got value=%v needsFetch=%v", it.Value, it.ValueNeedsFetch)
				}
			} else { // value data externalized
				if it.Value != nil || !it.ValueNeedsFetch {
					t.Fatalf("expected externalized value; got value=%v needsFetch=%v", it.Value, it.ValueNeedsFetch)
				}
			}
		})
	}
}

func Test_ItemActionTracker_Add_ErrorPaths(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{Name: "iat_add_err", SlotLength: 8, IsValueDataInNodeSegment: false, IsValueDataActivelyPersisted: true}
	si := sop.NewStoreInfo(so)
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, &errBlob{e: errors.New("boom")}, tl)
	pk, p := newPerson("err", "add", "1", "e@x", "p")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	err := tracker.Add(ctx, it)
	if err == nil {
		t.Fatalf("expected add error due to blob store failure")
	}
}

// No direct reset method; rely on creating a new tracker (stateless between instances).
func Test_ItemActionTracker_NewInstance_ResetsState(t *testing.T) {
	so := sop.StoreOptions{Name: "iat_reset", SlotLength: 8}
	si := sop.NewStoreInfo(so)
	tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
	pk, p := newPerson("r", "s", "g", "e", "p")
	it := &btree.Item[PersonKey, Person]{ID: sop.NewUUID(), Key: pk, Value: &p}
	if err := tracker.Add(context.Background(), it); err != nil {
		t.Fatalf("Add err: %v", err)
	}
	if len(tracker.items) == 0 {
		t.Fatalf("expected tracked items")
	}
	tracker2 := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mocks.NewMockBlobStore(), newTransactionLogger(mocks.NewMockTransactionLog(), false))
	if len(tracker2.items) != 0 {
		t.Fatalf("expected fresh tracker state")
	}
}

func Test_ItemActionTracker_Get_CacheHit_TTL(t *testing.T) {
	ctx := context.Background()
	so := sop.StoreOptions{Name: "iat_cache_ttl", SlotLength: 8, IsValueDataInNodeSegment: false, IsValueDataGloballyCached: true}
	si := sop.NewStoreInfo(so)
	si.CacheConfig.IsValueDataCacheTTL = true
	tracker := newItemActionTracker[PersonKey, Person](si, mockRedisCache, mockNodeBlobStore, newTransactionLogger(mocks.NewMockTransactionLog(), false))
	pk, p := newPerson("ttl", "add", "1", "e@x", "p")
	id := sop.NewUUID()
	_ = mockRedisCache.SetStruct(ctx, formatItemKey(id.String()), &p, si.CacheConfig.ValueDataCacheDuration)
	it := &btree.Item[PersonKey, Person]{ID: id, Key: pk, ValueNeedsFetch: true}
	if err := tracker.Get(ctx, it); err != nil {
		t.Fatalf("Get err: %v", err)
	}
	if it.Value == nil || it.ValueNeedsFetch {
		t.Fatalf("expected hydrated value from cache")
	}
}



// Consolidated extra scenarios for itemactiontracker:
// - lock compatibility vs conflict
// - checkTrackedItems outcomes
// - manage branches (persisted flag, update with fetch, add with/without value)

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
