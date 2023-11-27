package in_aws

import "github.com/SharedCode/sop/btree"

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
// This will return btree instance that has no wrapper, thus, methods have error in return where appropriate.
// Handy for using in-memory b-tree for writing unit tests to mock the "Enterprise" V2 version.
func NewBtree[TK btree.Comparable, TV any](name string, slotLength int, isUnique bool, isValueDataInNodeSegment bool) btree.BtreeInterface[TK, TV] {
	transactionManager := newTransactionManager[TK, TV]()
	s := btree.NewStore(name, slotLength, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, transactionManager.storeInterface)
}
