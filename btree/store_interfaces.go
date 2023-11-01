package btree

// store_interfaces contains interface definitions of different repository that are
// required by Btree. It is needed so we can support different backend storage.

// BtreeInterface defines publicly callable methods of Btree.
type BtreeInterface[TKey Comparable, TValue any] interface {
	Add(key TKey, value TValue) (bool, error)
	AddIfNotExist(key TKey, value TValue) (bool, error)
	// FindOne will search Btree for an item with a given key. Return true if found,
	// otherwise false. firstItemWithKey is useful when there are items with same key.
	// true will position pointer to the first item with the given key,
	// according to key ordering sequence.
	FindOne(key TKey, firstItemWithKey bool) (bool, error)
	// GetCurrentKey returns the current item's key.
	GetCurrentKey() TKey
	// GetCurrentValue returns the current item's value.
	GetCurrentValue() TValue
	// Update finds the item with key and update its value to the value argument.
	Update(key TKey, value TValue) (bool, error)
	// UpdateCurrentItem will update the Value of the current item.
	// Key is read-only, thus, no argument for the key.
	UpdateCurrentItem(newValue TValue) (bool, error)
	// Remove will find the item with a given key then remove that item.
	Remove(key TKey) (bool, error)
	// RemoveCurrentItem will remove the current key/value pair from the store.
	RemoveCurrentItem() (bool, error)

	MoveToFirst() (bool, error)
	MoveToLast() (bool, error)
	MoveToNext() (bool, error)
	MoveToPrevious() (bool, error)
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's data segment.
	// Otherwise is false.
	IsValueDataInNodeSegment() bool
}

// StoreRepository interface specifies the store repository.
type StoreRepository interface {
	Get(name string) Store
	Add(Store) error
	Remove(name string) error
}

// NodeRepository interface specifies the node repository.
type NodeRepository[TKey Comparable, TValue any] interface {
	Get(nodeId UUID) (*Node[TKey, TValue], error)
	Add(*Node[TKey, TValue]) error
	Update(*Node[TKey, TValue]) error
	Remove(nodeId UUID) error
}

type VirtualIdRepository interface {
	Get(lid UUID) (Handle, error)
	Add(Handle) error
	Update(Handle) error
	Remove(lid UUID) error
}

// RecyclerRepository provides capability to recycle storage areas for storing data such as Node, etc...
// There are backends where this is not needed at all, e.g. Cassandra backend will not need this.
type RecyclerRepository interface {
	Get(itemCount int, objectType int) []Recyclable
	Add(recyclables []Recyclable) error
	Remove(items []Recyclable) error
}

type TransactionRepository interface {
	Get(transactionId UUID) ([]TransactionEntry, error)
	GetByStore(transactionId UUID, storeName string) ([]TransactionEntry, error)
	Add([]TransactionEntry) error
	//Update([]TransactionEntry) error
	MarkDone([]TransactionEntry) error
}

// PhasedTransaction defines the "SOP internal" transaction methods.
type PhasedTransaction interface {
	Begin() error
	CommitPhase1() error
	CommitPhase2() error
	Rollback() error
}

// Transaction interface defines the "enduser facing" transaction methods.
type Transaction interface {
	Begin() error
	Commit() error
	Rollback() error
	HasBegun() bool
}
