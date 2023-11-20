package btree

import "github.com/SharedCode/sop/btree"

type Store btree.Store

func NewStoreRepository() btree.StoreRepository {
	return Store{}
}

func (Store) Get(name string) (btree.Store, error) {
	// todo
	return btree.Store{}, nil
}

func (Store) Add(source btree.Store) error {
	return nil
}

func (Store) Remove(name string) error {
	return nil
}
