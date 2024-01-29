package cassandra

import (
	"context"

	"github.com/SharedCode/sop/btree"
)

// mockStoreRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type mockStoreRepository struct {
	lookup map[string]btree.StoreInfo
}

// NewMockStoreRepository manages the StoreInfo in Cassandra table.
func NewMockStoreRepository() StoreRepository {
	return &mockStoreRepository{
		lookup: make(map[string]btree.StoreInfo),
	}
}

func (sr *mockStoreRepository) Add(ctx context.Context, stores ...btree.StoreInfo) error {
	for _, store := range stores {
		sr.lookup[store.Name] = store
	}
	return nil
}

func (sr *mockStoreRepository) Update(ctx context.Context, stores ...btree.StoreInfo) error {
	for _, store := range stores {
		s2 := store
		// Merge or apply the "count delta".
		s2.Count = s2.Count + s2.CountDelta
		sr.lookup[store.Name] = s2
	}
	return nil
}

func (sr *mockStoreRepository) Get(ctx context.Context, names ...string) ([]btree.StoreInfo, error) {
	stores := make([]btree.StoreInfo, len(names))
	for i, name := range names {
		v, _ := sr.lookup[name]
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
