package common

// Consolidated from: itemactiontracker_test.go, itemactiontracker_add_test.go
import (
	"context"
	"errors"
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
