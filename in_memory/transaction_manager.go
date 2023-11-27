package in_memory

import "github.com/SharedCode/sop/btree"

// in-memory transaction manager just relays CRUD actions to the actual in-memory NodeRepository.
type transaction_manager[TK btree.Comparable, TV any] struct {
	storeInterface *btree.StoreInterface[TK, TV]
}

// newTransactionManager assembles together an in-memory set of StoreInterface repositories
// that simply stores/manages items in-memory.
func newTransactionManager[TK btree.Comparable, TV any]() *transaction_manager[TK, TV] {
	si := btree.StoreInterface[TK, TV]{
		NodeRepository:  newNodeRepository[TK, TV](),
		StoreRepository: newStoreRepository(),
	}
	return &transaction_manager[TK, TV]{
		storeInterface: &si,
	}
}
