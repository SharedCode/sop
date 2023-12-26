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
	// Fetch from backend if not yet in the (local) cache list a given store info with name.
	Get(ctx context.Context, name string) (btree.StoreInfo, error)
	// Add store info to the (local) cache list.
	Add(btree.StoreInfo) error
	// Remove a store info with name from the (local) cache list. This should also remove all the
	// data of the store(i.e. - B-Tree) with such name.
	Remove(name string) error
	// Commit(i.e. - merge) to the backend the changes done to the (local) cache list.
	CommitChanges(ctx context.Context) error
}

// TODO: implement a real Store Repository, for now, mock it up using a map like below.

// storeRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type storeRepository struct {
	lookup map[string]btree.StoreInfo
}

// TODO: NewStoreRepository manages the StoreInfo in Cassandra table.
func NewStoreRepository() StoreRepository {
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
