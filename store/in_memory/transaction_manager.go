package in_memory

import "github.com/SharedCode/sop/btree"

// in-memory transaction manager just relays CRUD actions to the actual NodeRepository.
type transaction_manager[TK btree.Comparable, TV any] struct {
	storeInterface   *btree.StoreInterface[TK, TV] `json:"-"`
}

func NewTransactionManager[TK btree.Comparable, TV any]() *transaction_manager[TK,TV] {
	si := btree.StoreInterface[TK, TV]{
		NodeRepository:      newNodeRepository[TK, TV](),
		RecyclerRepository:  newRecycler(),
		VirtualIdRepository: newVirtualIdRepository(),
		Transaction:         newTransaction[TK, TV](),
		StoreRepository: newStoreRepository(),
	}
	return &transaction_manager[TK, TV]{
		storeInterface: &si,
	}
}

func (tm *transaction_manager[TK, TV])Get(nodeId btree.UUID) (*btree.Node[TK, TV], error) {
	return tm.storeInterface.NodeRepository.Get(nodeId)
}
func (tm *transaction_manager[TK, TV])Add(node *btree.Node[TK, TV]) error {
	return tm.storeInterface.NodeRepository.Add(node)
}

func (tm *transaction_manager[TK, TV])Update(node *btree.Node[TK, TV]) error {
	return tm.storeInterface.NodeRepository.Update(node)
}

func (tm *transaction_manager[TK, TV])Remove(nodeId btree.UUID) error {
	return tm.storeInterface.NodeRepository.Remove(nodeId)
}
