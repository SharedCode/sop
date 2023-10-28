package in_memory

import "github.com/SharedCode/sop/btree"

// NewStore will create an in-memory Store for B-Tree. You can use this similar to how you use a Map.
// Implemented in SOP so we can mockup the B-Tree and write some unit tests on it, but feel free to
// use it in your discretion if you have a use for it.
func NewStore[TKey btree.Comparable, TValue any]() (btree.BtreeInterface[TKey, TValue], error) {
	s := btree.NewStore("", 10, false, true)
	si := btree.StoreInterface[TKey, TValue]{
		NodeRepository:      NewNodeRepository[TKey, TValue](),
		RecyclerRepository:  NewRecycler(),
		VirtualIdRepository: NewVirtualIdRepository(),
		Transaction:         NewTransaction(),
	}
	return btree.NewBtree[TKey, TValue](s, si), nil
}
