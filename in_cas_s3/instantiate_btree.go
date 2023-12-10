package in_cas_s3

import (
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_cas_s3/redis"
	"github.com/SharedCode/sop/in_memory"
)

// NewBtree will create B-Tree with data persisted in backend store, e.g. - AWS storage services.
func NewBtree[TK btree.Comparable, TV any](name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, t Transaction) btree.BtreeInterface[TK, TV] {
	si := StoreInterface[TK, TV]{
		nodeLocalCache: in_memory.NewBtree[btree.UUID, interface{}](true),
		nodeRedisCache: redis.NewClient(redis.DefaultOptions()),
		// TODO: replace with real S3 or file system persisting repository.
		nodeBlobStore: in_memory.NewBtreeWithNoWrapper[btree.UUID, interface{}](true),
		recyclerRepository:  newRecycler(), // shared globally.
		virtualIdRepository: newVirtualIdRepository(),
		storeRepository:     newStoreRepository(), // shared globally.
	}
	si.ItemActionTracker = newItemActionTracker[TK, TV](&si)
	si.NodeRepository = newNodeRepository[TK, TV](&si)
	s := btree.NewStoreInfo(name, slotLength, isUnique, true)
	si.storeRepository.Add(s)
	return btree.NewBtree[TK, TV](s, &si.StoreInterface)
}
