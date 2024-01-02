package in_cas_s3

import (
	"context"
	"fmt"

	"github/sharedcode/sop/btree"
)

// OpenBtree will open an existing B-Tree instance it for use in a transaction.
func OpenBtree[TK btree.Comparable, TV any](ctx context.Context, name string, t Transaction) (btree.BtreeInterface[TK, TV], error) {
	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*transaction)
	stores, err := trans.storeRepository.Get(ctx, name)
	if len(stores) == 0 || stores[0].IsEmpty() || err != nil {
		if err == nil {
			return nil, fmt.Errorf("B-Tree '%s' does not exist, please use NewBtree to create an instance of it.", name)
		}
		return nil, err
	}
	return newBtree[TK, TV](&stores[0], trans)
}

// NewBtree will create a new B-Tree instance with data persisted in backend store,
// e.g. - AWS storage services. The created B-Tree is a permanent action, it is outside
// of the transaction, thus, even if the passed in transaction rolls back, the created tree sticks.
func NewBtree[TK btree.Comparable, TV any](ctx context.Context, name string, slotLength int, isUnique bool,
	isValueDataInNodeSegment bool, t Transaction) (btree.BtreeInterface[TK, TV], error) {

	var t2 interface{} = t.GetPhasedTransaction()
	trans := t2.(*transaction)

	stores, err := trans.storeRepository.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	ns := btree.NewStoreInfo(name, slotLength, isUnique, true)
	if len(stores) == 0 || stores[0].IsEmpty() {
		// Add to store repository if store not found.
		if ns.RootNodeId.IsNil() {
			// Pre-assign root node Id so B-Trees can merge newly created root nodes on commit.
			ns.RootNodeId = btree.NewUUID()
			ns.Timestamp = Now()
		}
		if err := trans.storeRepository.Add(ctx, *ns); err != nil {
			return nil, err
		}
		stores = []btree.StoreInfo{*ns}
	}
	// Check if store retrieved is empty or of non-compatible specification.
	if !ns.IsCompatible(stores[0]) {
		// Recommend to use the OpenBtree function to open it.
		return nil, fmt.Errorf("B-Tree '%s' exists, please use OpenBtree to open & create an instance of it.", name)
	}
	return newBtree[TK, TV](ns, trans)
}

func newBtree[TK btree.Comparable, TV any](s *btree.StoreInfo, trans *transaction) (btree.BtreeInterface[TK, TV], error) {
	si := StoreInterface[interface{}, interface{}]{}

	// Assign the item action tracker frontend and backend bits.
	iatw := newItemActionTracker()
	si.ItemActionTracker = iatw
	si.backendItemActionTracker = iatw

	// Assign the node repository frontend and backend bits.
	nrw := newNodeRepository[interface{}, interface{}](trans)
	nrw.realNodeRepository.count = s.Count
	si.NodeRepository = nrw
	si.backendNodeRepository = nrw.realNodeRepository

	// Wire up the B-tree & add its backend interface to the transaction.
	b3, _ := btree.New[interface{}, interface{}](s, &si.StoreInterface)
	trans.btreesBackend = append(trans.btreesBackend, si)
	trans.btrees = append(trans.btrees, b3)

	return newBtreeWithTransaction[TK, TV](trans, b3), nil
}
