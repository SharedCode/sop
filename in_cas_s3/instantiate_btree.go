package in_cas_s3

import "github.com/SharedCode/sop/btree"

// NewBtree will create B-Tree with data persisted in backend store, e.g. - AWS storage services.
func NewBtree[TK btree.Comparable, TV any](name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, t Transaction) btree.BtreeInterface[TK, TV] {
	si := StoreInterface[TK, TV]{
		RecyclerRepository:  newRecycler(), // shared globally.
		VirtualIdRepository: newVirtualIdRepository(),
		StoreRepository:     newStoreRepository(), // shared globally.
	}
	si.NodeRepository = newNodeRepository[TK, TV]()
	s := btree.NewStore(name, slotLength, isUnique, true)
	si.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, &si.StoreInterface)
}
