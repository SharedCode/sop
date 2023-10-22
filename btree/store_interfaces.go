package btree

// store_interfaces contains interface definitions of different repository that are
// required by Btree. It is needed so we can support different backend storage.
// NOTE: may be moved to directly under sop.

// BtreeInterface defines publicly callable methods of Btree.
type BtreeInterface[TKey Comparable, TValue any] interface{
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

type StoreRepository interface{
	Get(name string) Store
	Add(Store) error
	Remove(name string) error
}

type NodeRepository[TKey Comparable, TValue any] interface{
	Get(nodeId Handle) (*Node[TKey, TValue], error)
	Add(*Node[TKey, TValue]) error
	Update(*Node[TKey, TValue]) error
	Remove(nodeId Handle) error
}

type VirtualIdRepository interface{
	Get(logicalId UUID) (VirtualId, error)
	Add(VirtualId) error
	Update(VirtualId) error
	Remove(logicalId UUID) error
	// NewUUId will generate new UUId that is unique globally.
	NewUUID() UUID
}

type RecyclerRepository interface{
	Get(batch int, objectType int) []Recyclable
	Add(recyclables []Recyclable) error
	Remove(items []Recyclable) error
}

type TransactionRepository interface{
	Get(transactionId UUID) ([]TransactionEntry, error)
	GetByStore(transactionId UUID, storeName string) ([]TransactionEntry, error)
	Add([]TransactionEntry) error
	//Update([]TransactionEntry) error
	MarkDone([]TransactionEntry) error
}
