package in_cas_s3

import (
	"github.com/SharedCode/sop/in_cas_s3/redis"

	"github.com/SharedCode/sop/btree"
)

// NewBtree will create B-Tree with data persisted in backend store,
// e.g. - AWS storage services.
func NewBtree[TK btree.Comparable, TV any](name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, t Transaction) btree.BtreeInterface[TK, TV] {
	si := StoreInterface[interface{}, interface{}]{
		recyclerRepository: newRecycler(), // shared globally.
		virtualIdRegistry:  newVirtualIdRegistry(),
		storeRepository:    newStoreRepository(), // shared globally.
	}

	// Assign the item action tracker frontend and backend bits.
	iatw := newItemActionTracker[interface{}, interface{}]()
	si.ItemActionTracker = iatw
	si.backendItemActionTracker = iatw.realItemActionTracker

	// Assign the node repository frontend and backend bits.
	nrw := newNodeRepository[interface{}, interface{}]()
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.realNodeRepository
	si.itemRedisCache = redis.NewClient(redis.DefaultOptions())

	// Create, populate & assign the Store Info.
	s := btree.NewStoreInfo(name, slotLength, isUnique, true)
	si.storeRepository.Add(s)

	// Wire up the B-tree & its backend store interface of the transaction.
	b3 := btree.NewBtree[interface{}, interface{}](s, &si.StoreInterface)
	var t2 interface{} = t
	trans := t2.(*transaction)
	trans.btreesBackend = append(trans.btreesBackend, si)
	trans.btrees = append(trans.btrees, b3)

	return newBtreeWithTransaction[TK, TV](t, b3)
}
