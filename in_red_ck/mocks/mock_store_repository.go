package mocks

import (
	"context"

	"github.com/SharedCode/sop"
)

// mockStoreRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type mockStoreRepository struct {
	lookup map[string]sop.StoreInfo
}

// NewMockStoreRepository manages the StoreInfo in Cassandra table.
func NewMockStoreRepository() sop.StoreRepository {
	return &mockStoreRepository{
		lookup: make(map[string]sop.StoreInfo),
	}
}

func (sr *mockStoreRepository) Add(ctx context.Context, stores ...sop.StoreInfo) error {
	for _, store := range stores {
		sr.lookup[store.Name] = store
	}
	return nil
}

func (sr *mockStoreRepository) Update(ctx context.Context, stores ...sop.StoreInfo) error {
	for _, store := range stores {
		cs := sr.lookup[store.Name]
		// Merge or apply the "count delta".
		store.Count = cs.Count + store.CountDelta
		store.CountDelta = 0
		sr.lookup[store.Name] = store
	}
	return nil
}

func (sr *mockStoreRepository) Get(ctx context.Context, names ...string) ([]sop.StoreInfo, error) {
	stores := make([]sop.StoreInfo, len(names))
	for i, name := range names {
		v := sr.lookup[name]
		stores[i] = v
	}
	return stores, nil
}

func (sr *mockStoreRepository) Remove(ctx context.Context, names ...string) error {
	for _, name := range names {
		delete(sr.lookup, name)
	}
	return nil
}
