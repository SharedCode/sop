package store;

import "github.com/SharedCode/sop/btree"

type sc Connection

func (conn *sc) Get(name string) btree.Store{
	return btree.Store{};
}

func  (conn *sc) Add(source btree.Store) error{
	return nil;
}

func  (conn *sc) Remove(name string) error{
	return nil;
}
