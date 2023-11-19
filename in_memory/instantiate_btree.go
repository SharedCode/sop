package in_memory

import "github.com/SharedCode/sop/btree"

// For in-memory b-tree, hardcode to 8 items per node. We don't need wide array for in-memory.
const itemsPerNode = 8

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
func NewBtree[TK btree.Comparable, TV any](isUnique bool) (btree.BtreeInterface[TK, TV], error) {
	transactionManager := newTransactionManager[TK,TV]()
	s := btree.NewStore("", itemsPerNode, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, transactionManager.storeInterface), nil
}
