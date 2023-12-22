package in_cas_s3

import (
	"context"

	"github.com/SharedCode/sop/btree"
)

// StoreRepository interface specifies the store repository.
type StoreRepository interface {
	Get(ctx context.Context, name string) (btree.StoreInfo, error)
	Add(btree.StoreInfo) error
	Remove(name string) error
	CommitChanges(ctx context.Context) error
}

// storeRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type storeRepository struct {
	lookup map[string]btree.StoreInfo
}

func newStoreRepository() StoreRepository {
	return &storeRepository{
		lookup: make(map[string]btree.StoreInfo),
	}
}

func (sr *storeRepository) Add(store btree.StoreInfo) error {
	sr.lookup[store.Name] = store
	return nil
}

func (sr *storeRepository) Get(ctx context.Context, name string) (btree.StoreInfo, error) {
	v, _ := sr.lookup[name]
	return v, nil
}

func (sr *storeRepository) Remove(name string) error {
	delete(sr.lookup, name)
	return nil
}

func (sr *storeRepository) CommitChanges(ctx context.Context) error {
	return nil
}
