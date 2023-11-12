package in_memory

import "github.com/SharedCode/sop/btree"

// For in-memory b-tree, hardcode to 8 items per node. We don't need wide array for in-memory.
const itemsPerNode = 4

// NewBtree will create an in-memory B-Tree & its required data stores. You can use this similar to
// how you use a Map. Implemented in SOP so we can mockup the (structural composition & interfaces of)
// B-Tree and write some unit tests on it, but feel free to use it in your discretion if you have a use for it.
func NewBtree[TK btree.Comparable, TV any](isUnique bool) (btree.BtreeInterface[TK, TV], error) {
	transactionManager := newTransactionManager[TK,TV]()
	s := btree.NewStore("", itemsPerNode, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, transactionManager.storeInterface), nil
}
