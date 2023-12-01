package in_aws

import "github.com/SharedCode/sop/btree"

// StoreRepository interface specifies the store repository.
type StoreRepository interface {
	Get(name string) (btree.Store, error)
	Add(btree.Store) error
	Remove(name string) error
}

// storeRepository is a simple in-memory implementation of store repository to demonstrate
// or mockup the structure composition, so we can define it in preparation of v2.
type storeRepository struct {
	lookup map[string]btree.Store
}

func newStoreRepository() StoreRepository {
	return &storeRepository{
		lookup: make(map[string]btree.Store),
	}
}

func (sr *storeRepository) Add(store btree.Store) error {
	sr.lookup[store.Name] = store
	return nil
}

func (sr *storeRepository) Get(name string) (btree.Store, error) {
	v, _ := sr.lookup[name]
	return v, nil
}

func (sr *storeRepository) Remove(name string) error {
	delete(sr.lookup, name)
	return nil
}
