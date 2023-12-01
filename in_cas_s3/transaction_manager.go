package in_cas_s3

import "github.com/SharedCode/sop/btree"

// in-memory transaction manager just relays CRUD actions to the actual in-memory NodeRepository.
type transaction_manager[TK btree.Comparable, TV any] struct {
	storeInterface *StoreInterface[TK, TV]
}

// Transaction Manager surfaces a StoreInterface that which, knows how to reconcile/merge
// changes in a transaction session with the actual destination storage.
// Idea here is, to let the algorithms be abstracted with the backend store. To the b-tree
// it feels like it only uses normal repositories but in actuality, these repositories
// are facade for the transaction manager, so it can reconcile/merge with the backend
// during transaction commit. A very simple model, but powerful/complete control on
// the data changes & necessary merging with backend storage.
func newTransactionManager[TK btree.Comparable, TV any]() *transaction_manager[TK, TV] {
	si := StoreInterface[TK, TV]{
		RecyclerRepository:  newRecycler(), // shared globally.
		VirtualIdRepository: newVirtualIdRepository(),
		StoreRepository:     newStoreRepository(), // shared globally.
	}
	si.NodeRepository = newNodeRepository[TK, TV]()

	return &transaction_manager[TK, TV]{
		storeInterface: &si,
	}
}
