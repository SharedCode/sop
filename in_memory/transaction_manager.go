package in_memory

import "github.com/SharedCode/sop/btree"

// in-memory transaction manager just relays CRUD actions to the actual in-memory NodeRepository.
type transaction_manager[TK btree.Comparable, TV any] struct {
	storeInterface *btree.StoreInterface[TK, TV]
}

// Transaction Manager surfaces a StoreInterface that which, knows how to reconcile/merge
// changes in a transaction session with the actual destination storage.
// Idea here is, to let the algorithms be abstracted with the backend store. To the b-tree
// it feels like it only uses normal repositories but in actuality, these repositories
// are facade for the transaction manager, so it can reconcile/merge with the backend
// during transaction commit. A very simple model, but powerful/complete control on
// the data changes & necessary merging with backend storage.
//
// newTransactionManager assembles together an in-memory set of StoreInterface repositories
// that simply stores/manages items in-memory.
func newTransactionManager[TK btree.Comparable, TV any]() *transaction_manager[TK, TV] {
	si := btree.StoreInterface[TK, TV]{
		NodeRepository:      btree.NewInMemoryNodeRepository[TK, TV](),
		RecyclerRepository:  newRecycler(),
		VirtualIdRepository: newVirtualIdRepository(),
		Transaction:         newTransaction[TK, TV](),
		StoreRepository:     newStoreRepository(),
	}
	return &transaction_manager[TK, TV]{
		storeInterface: &si,
	}
}
