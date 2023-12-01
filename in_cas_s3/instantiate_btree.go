package in_cas_s3

import "github.com/SharedCode/sop/btree"

// NewBtree will create B-Tree with data persisted in backend store, e.g. - AWS storage services.
func NewBtree[TK btree.Comparable, TV any](name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, t Transaction) btree.BtreeInterface[TK, TV] {
	transactionManager := newTransactionManager[TK, TV]()
	s := btree.NewStore(name, slotLength, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, &transactionManager.storeInterface.StoreInterface)
}
