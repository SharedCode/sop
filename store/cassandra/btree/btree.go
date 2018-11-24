package btree;

import "../../../btree"

type Recyclable struct{
	Year int
	Month int
	Day int
	Hour int
	btree.Recyclable
}

type Store btree.Store;

func NewStoreRepository() btree.StoreRepository{
	return Store{};
}

func (Store) Get(name string) *btree.Store{
	return nil;
}

func  (Store) Add(source *btree.Store) error{
	return nil;
}

func  (Store) Remove(name string) error{
	return nil;
}

func NewRecycler() btree.Recycler{
	return Recyclable{};
}

func (Recyclable) Get(objectID btree.UUID) *btree.Recyclable{
	return nil;
}
func (Recyclable) Add(recyclable *btree.Recyclable) error{
	//var iface interface{} = recyclable
	//item := iface.(Recyclable)
	return nil;
}
func (Recyclable) Update(*btree.Recyclable) error{
	return nil;
}
func (Recyclable) Remove(objectID btree.UUID) error{
	return nil;
}
