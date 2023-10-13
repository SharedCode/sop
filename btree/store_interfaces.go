package btree

// BtreeInterface defines publicly callable methods of Btree.
type BtreeInterface[TKey comparable, TValue any] interface{
	Add(key TKey, value TValue) (bool, error)
	Update(key TKey, value TValue) (bool, error)
	UpdateCurrentItem(newValue TValue) (bool, error)
	Remove(key TKey) (bool, error)
	RemoveCurrentItem() (bool, error)

	// MoveTo will search Btree for an item with a given key. Return true if found, 
	// otherwise false. firstItemWithKey is useful when there are items with same key. 
	// true will position pointer to the first item, according to key ordering sequence, 
	// with the given key.
	MoveTo(key TKey, firstItemWithKey bool) (bool, error)
	MoveToFirst() (bool, error)
	MoveToLast() (bool, error)
	MoveToNext() (bool, error)
	MoveToPrevious()( bool, error)
}

// backend store persistence interfaces

type StoreInterface struct{
	StoreType uint
	StoreRepository StoreRepository
	NodeRepository NodeRepository
	VirtualIDRepository VirtualIDRepository
	RecyclerRepository RecyclerRepository
	TransactionRepository TransactionRepository
}

type StoreRepository interface{
	Get(name string) Store
	Add(Store) error
	Remove(name string) error
}

type NodeRepository interface{
	Get(nodeID Handle) (*Node, error)
	Add(*Node) error
	Update(*Node) error
	Remove(nodeID Handle) error
}

type VirtualIDRepository interface{
	Get(logicalID UUID) (VirtualID, error)
	Add(VirtualID) error
	Update(VirtualID) error
	Remove(logicalID UUID) error
	// NewUUID will generate new UUID that is unique globally.
	NewUUID() UUID
}

type RecyclerRepository interface{
	Get(batch int, objectType int) []Recyclable
	Add(recyclables []Recyclable) error
	Remove(items []Recyclable) error
}

type TransactionRepository interface{
	Get(transactionID UUID) ([]TransactionEntry, error)
	GetByStore(transactionID UUID, storeName string) ([]TransactionEntry, error)
	Add([]TransactionEntry) error
	//Update([]TransactionEntry) error
	MarkDone([]TransactionEntryKeys) error
}
