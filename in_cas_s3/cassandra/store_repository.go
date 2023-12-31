package cassandra

import (
	"context"

	"github.com/SharedCode/sop/btree"
)

// TODO: when need arise, move these interfaces to a common package, but keep them for now
// in package where they are implemented, 'just because we wanted to keep changes minimal,
// and driven by needs.

// StoreRepository interface specifies the store repository. Stores are readonly after creation, thus, no update method.
type StoreRepository interface {
	// Fetch store info with name.
	Get(context.Context, string) (btree.StoreInfo, error)
	// Add store info. Add all or nothing.
	Add(context.Context, ...btree.StoreInfo) error
	// Update store info. Update all or nothing.
	Update(context.Context, ...btree.StoreInfo) error
	// Remove store info with name. Remove all or nothing.
	Remove(context.Context, ...string) error
}

// storeRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type storeRepository struct {
	lookup map[string]btree.StoreInfo
}

// NewStoreRepository manages the StoreInfo in Cassandra table.
func NewStoreRepository() StoreRepository {
	return &storeRepository{
		lookup: make(map[string]btree.StoreInfo),
	}
}

func (sr *storeRepository) Add(ctx context.Context, stores ...btree.StoreInfo) error {
	for _, store := range stores {
		sr.lookup[store.Name] = store
	}
	return nil
}

func (sr *storeRepository) Update(ctx context.Context, stores ...btree.StoreInfo) error {
	for _, store := range stores {
		sr.lookup[store.Name] = store
	}
	return nil
}

func (sr *storeRepository) Get(ctx context.Context, name string) (btree.StoreInfo, error) {
	v, _ := sr.lookup[name]
	return v, nil
}

func (sr *storeRepository) Remove(ctx context.Context, names ...string) error {
	for _, name := range names {
		delete(sr.lookup, name)
	}
	return nil
}
