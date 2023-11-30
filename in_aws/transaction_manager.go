package in_aws

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
func newTransactionManager[TK btree.Comparable, TV any]() *transaction_manager[TK, TV] {
	si := btree.StoreInterface[TK, TV]{
		NodeRepository:      newNodeRepository[TK, TV](),
		RecyclerRepository:  newRecycler(), // shared globally.
		VirtualIdRepository: newVirtualIdRepository(),
		StoreRepository:     newStoreRepository(), // shared globally.
	}
	return &transaction_manager[TK, TV]{
		storeInterface: &si,
	}
}

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
// This will return btree instance that has no wrapper, thus, methods have error in return where appropriate.
// Handy for using in-memory b-tree for writing unit tests to mock the "Enterprise" V2 version.
func NewBtreeWithNoWrapper[TK btree.Comparable, TV any](isUnique bool) btree.BtreeInterface[TK, TV] {
	transactionManager := newTransactionManager[TK, TV]()
	s := btree.NewStore("", itemsPerNode, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, transactionManager.storeInterface)
}
