package in_cas_s3

import (
	"fmt"

	"github.com/SharedCode/sop/btree"
)

// OpenBtree will open an existing B-Tree instance it for use in a transaction.
func OpenBtree[TK btree.Comparable, TV any](name string, t Transaction) (btree.BtreeInterface[TK, TV], error) {
	var t2 interface{} = t
	trans := t2.(*transaction)
	if s, err := trans.storeRepository.Get(name); s.IsEmpty() || err != nil {
		if s.IsEmpty() {
			return nil, fmt.Errorf("B-Tree '%s' does not exist, please use NewBtree to create an instance of it.", name)
		}
		return nil, err
	} else {
		return newBtree[TK, TV](s, trans)
	}
}

// NewBtree will create a new B-Tree instance with data persisted in backend store,
// e.g. - AWS storage services.
func NewBtree[TK btree.Comparable, TV any](name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, t Transaction) (btree.BtreeInterface[TK, TV], error) {

	var t2 interface{} = t
	trans := t2.(*transaction)

	if s, err := trans.storeRepository.Get(name); !s.IsEmpty() || err != nil {
		if !s.IsEmpty() {
			return nil, fmt.Errorf("B-Tree '%s' exists, please use OpenBtree to open & create an instance of it.", name)
		}
		return nil, err
	}
	return newBtree[TK, TV](btree.NewStoreInfo(name, slotLength, isUnique, true), trans)
}

func newBtree[TK btree.Comparable, TV any](s btree.StoreInfo, trans *transaction) (btree.BtreeInterface[TK, TV], error) {
	si := StoreInterface[interface{}, interface{}]{}

	// Assign the item action tracker frontend and backend bits.
	iatw := newItemActionTracker()
	si.ItemActionTracker = iatw
	si.backendItemActionTracker = iatw

	// Assign the node repository frontend and backend bits.
	nrw := newNodeRepository[interface{}, interface{}]()
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.realNodeRepository

	// Wire up the B-tree & its backend store interface of the transaction.
	b3,_ := btree.New[interface{}, interface{}](s, &si.StoreInterface)
	trans.btreesBackend = append(trans.btreesBackend, si)
	trans.btrees = append(trans.btrees, b3)
	trans.storeRepository.Add(s)

	return newBtreeWithTransaction[TK, TV](trans, b3), nil
}
