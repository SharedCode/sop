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
type Node btree.Node;

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

func NewNodeRepository() btree.NodeRepository{
	return Node{};
}

func (Node) Add(n *btree.Node) error {
	return nil;
}
func (Node) Update(n *btree.Node) error {
	return nil;
}
func (Node) Get(nodeID btree.UUID) (*btree.Node, error) {
	return &btree.Node{}, nil;
}
func (Node) Remove(nodeID btree.UUID) error {
	return nil;
}

func NewRecycler() btree.Recycler{
	return Recyclable{};
}

func (Recyclable) Get(batch int, objectType int) []*btree.Recyclable{
	return nil;
}
func (Recyclable) Add(recyclable []*btree.Recyclable) error{
	//var iface interface{} = recyclable
	//item := iface.(Recyclable)
	return nil;
}
// func (Recyclable) Update(*btree.Recyclable) error{
// 	return nil;
// }
func (Recyclable) Remove(items []*btree.Recyclable) error{
	return nil;
}
