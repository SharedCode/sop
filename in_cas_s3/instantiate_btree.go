package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
)

// NewBtree will create B-Tree with data persisted in backend store, e.g. - AWS storage services.
func NewBtree[TK btree.Comparable, TV any](name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, t Transaction) btree.BtreeInterface[TK, TV] {
	si := StoreInterface[TK, TV]{
		recyclerRepository: newRecycler(), // shared globally.
		virtualIdRegistry:  newVirtualIdRegistry(),
		storeRepository:    newStoreRepository(), // shared globally.
	}
	si.ItemActionTracker = newItemActionTracker[TK, TV]()
	nrw := newNodeRepository[TK, TV]()
	// Assign the frontend facing NodeRepository that uses generics.
	si.NodeRepository = nrw
	// Assign the backend transaction nodeRepository that we can process in transaction commit.
	si.nodeRepository = nrw.realNodeRepository
	s := btree.NewStoreInfo(name, slotLength, isUnique, true)
	si.storeRepository.Add(s)
	return btree.NewBtree[TK, TV](s, &si.StoreInterface)
}
