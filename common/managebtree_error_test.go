package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/common/mocks"
)

type errStoreRepo struct{ e error }

func (e *errStoreRepo) Add(ctx context.Context, stores ...sop.StoreInfo) error { return nil }
func (e *errStoreRepo) Update(ctx context.Context, stores []sop.StoreInfo) ([]sop.StoreInfo, error) {
	return nil, nil
}
func (e *errStoreRepo) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	return nil, nil
}
func (e *errStoreRepo) GetAll(ctx context.Context) ([]string, error) { return nil, nil }
func (e *errStoreRepo) GetWithTTL(ctx context.Context, isTTL bool, d time.Duration, names ...string) ([]sop.StoreInfo, error) {
	return nil, e.e
}
func (e *errStoreRepo) Remove(ctx context.Context, names ...string) error               { return nil }
func (e *errStoreRepo) Replicate(ctx context.Context, storesInfo []sop.StoreInfo) error { return nil }

func Test_RefetchAndMerge_StoreInfoFetchError(t *testing.T) {
	ctx := context.Background()
	// Construct a minimal B-Tree and StoreInterface similar to newBtree(), but inject a StoreRepository that errors
	so := sop.StoreOptions{Name: "rfm_storeinfo_err", SlotLength: 8, IsValueDataInNodeSegment: true}
	ns := sop.NewStoreInfo(so)
	si := StoreInterface[PersonKey, Person]{}
	// Minimal transaction for backends
	tr := &Transaction{
		registry:        mocks.NewMockRegistry(false),
		l2Cache:         mocks.NewMockClient(),
		l1Cache:         cache.GetGlobalCache(),
		blobStore:       mocks.NewMockBlobStore(),
		logger:          newTransactionLogger(mocks.NewMockTransactionLog(), false),
		StoreRepository: &errStoreRepo{e: errors.New("boom")},
	}
	// Wire IAT and NodeRepository like newBtree() BEFORE creating B-Tree
	si.ItemActionTracker = newItemActionTracker[PersonKey, Person](ns, tr.l2Cache, tr.blobStore, tr.logger)
	nrw := newNodeRepository[PersonKey, Person](tr, ns)
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.nodeRepositoryBackend
	// Build B-Tree with the StoreInterface hook
	b3, err := btree.New(ns, &si.StoreInterface, Compare)
	if err != nil {
		t.Fatal(err)
	}

	closure := refetchAndMergeClosure(&si, b3, tr.StoreRepository)
	if err := closure(ctx); err == nil {
		t.Fatalf("expected error from GetWithTTL, got nil")
	}
}
