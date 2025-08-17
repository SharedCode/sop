package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common/mocks"
)

// errOnAddStoreRepo errors on Add and tracks Remove calls.
type errOnAddStoreRepo struct{ removed []string }

func (e *errOnAddStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error {
	return errors.New("add failed")
}
func (e *errOnAddStoreRepo) Update(ctx context.Context, stores []sop.StoreInfo) ([]sop.StoreInfo, error) {
	return nil, nil
}
func (e *errOnAddStoreRepo) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	// Return empty to force NewBtree to try Add
	return []sop.StoreInfo{}, nil
}
func (e *errOnAddStoreRepo) GetAll(ctx context.Context) ([]string, error) { return nil, nil }
func (e *errOnAddStoreRepo) GetWithTTL(ctx context.Context, isTTL bool, d time.Duration, names ...string) ([]sop.StoreInfo, error) {
	return e.Get(ctx, names...)
}
func (e *errOnAddStoreRepo) Remove(ctx context.Context, names ...string) error {
	e.removed = append(e.removed, names...)
	return nil
}
func (e *errOnAddStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error {
	return nil
}

// errOnGetStoreRepo errors on Get for OpenBtree path.
type errOnGetStoreRepo struct{ err error }

func (e *errOnGetStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error { return nil }
func (e *errOnGetStoreRepo) Update(ctx context.Context, stores []sop.StoreInfo) ([]sop.StoreInfo, error) {
	return nil, nil
}
func (e *errOnGetStoreRepo) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	return nil, e.err
}
func (e *errOnGetStoreRepo) GetAll(ctx context.Context) ([]string, error) { return nil, nil }
func (e *errOnGetStoreRepo) GetWithTTL(ctx context.Context, isTTL bool, d time.Duration, names ...string) ([]sop.StoreInfo, error) {
	return nil, e.err
}
func (e *errOnGetStoreRepo) Remove(ctx context.Context, names ...string) error { return nil }
func (e *errOnGetStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error {
	return nil
}

// blob store that errors on GetOne to drive itemactiontracker.Get error branch.
type errGetBlobStore struct{}

func (e errGetBlobStore) GetOne(ctx context.Context, blobName string, blobID sop.UUID) ([]byte, error) {
	return nil, errors.New("blob get error")
}
func (e errGetBlobStore) Add(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return nil
}
func (e errGetBlobStore) Update(ctx context.Context, storesblobs []sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return nil
}
func (e errGetBlobStore) Remove(ctx context.Context, storesBlobsIDs []sop.BlobsPayload[sop.UUID]) error {
	return nil
}

func Test_NewBtree_AddFails_CleansUpAndRollsBack(t *testing.T) {
	ctx := context.Background()
	trans, _ := newMockTransaction(t, sop.ForWriting, -1)
	if err := trans.Begin(); err != nil {
		t.Fatalf("begin err: %v", err)
	}
	// Swap repository with erroring one
	t2 := trans.GetPhasedTransaction().(*Transaction)
	ers := &errOnAddStoreRepo{}
	t2.StoreRepository = ers
	// Attempt to create new store; expect error and rollback (transaction ended)
	cmp := func(a, b int) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}
	_, err := NewBtree[int, int](ctx, sop.StoreOptions{Name: "add_fail", SlotLength: 2}, trans, cmp)
	if err == nil {
		t.Fatalf("expected error from NewBtree with Add failure")
	}
	if trans.HasBegun() {
		t.Fatalf("expected transaction ended after rollback")
	}
	// Remove should be called for attempted add
	if len(ers.removed) == 0 || ers.removed[0] != "add_fail" {
		t.Fatalf("expected Remove called for add_fail, got %v", ers.removed)
	}
}

func Test_OpenBtree_StoreRepositoryError_RollsBack(t *testing.T) {
	ctx := context.Background()
	trans, _ := newMockTransaction(t, sop.ForWriting, -1)
	_ = trans.Begin()
	t2 := trans.GetPhasedTransaction().(*Transaction)
	t2.StoreRepository = &errOnGetStoreRepo{err: errors.New("get error")}
	cmp := func(a, b int) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		}
		return 0
	}
	_, err := OpenBtree[int, int](ctx, "any", trans, cmp)
	if err == nil {
		t.Fatalf("expected error from OpenBtree when Get errors")
	}
	if trans.HasBegun() {
		t.Fatalf("expected transaction ended after OpenBtree failure")
	}
}

func Test_ItemActionTracker_Get_TTL_And_BlobError(t *testing.T) {
	ctx := context.Background()
	// TTL=true path with cache miss should fall back to blob store; here blob store errors.
	so := sop.StoreOptions{
		Name:                      "iat_get_ttl",
		SlotLength:                4,
		IsValueDataInNodeSegment:  false,
		IsValueDataGloballyCached: true,
		CacheConfig: &sop.StoreCacheConfig{
			IsValueDataCacheTTL:    true,
			ValueDataCacheDuration: time.Minute,
		},
	}
	si := sop.NewStoreInfo(so)
	// Use mock transaction log
	tl := newTransactionLogger(mocks.NewMockTransactionLog(), false)
	// Construct tracker with blob error store to force error path
	tr := newItemActionTracker[int, int](si, mockRedisCache, errGetBlobStore{}, tl)

	item := &btree.Item[int, int]{ID: sop.NewUUID(), Key: 1, Value: nil, ValueNeedsFetch: true}
	if err := tr.Get(ctx, item); err == nil {
		t.Fatalf("expected error when blob store GetOne fails")
	}
}
