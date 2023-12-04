package btree

// store_interfaces contains interface definitions of different repository that are
// required by Btree. It is needed so we can support different backend storage.

// BtreeInterface defines publicly callable methods of Btree.
type BtreeInterface[TK Comparable, TV any] interface {
	// Add adds an item to the b-tree and does not check for duplicates.
	Add(key TK, value TV) (bool, error)
	// AddIfNotExist adds an item if there is no item matching the key yet.
	// Otherwise, it will do nothing and return false, for not adding the item.
	// This is useful for cases one wants to add an item without creating a duplicate entry.
	AddIfNotExist(key TK, value TV) (bool, error)
	// FindOne will search Btree for an item with a given key. Return true if found,
	// otherwise false. firstItemWithKey is useful when there are items with same key.
	// true will position pointer to the first item with the given key,
	// according to key ordering sequence.
	FindOne(key TK, firstItemWithKey bool) (bool, error)
	// GetCurrentKey returns the current item's key.
	GetCurrentKey() TK
	// GetCurrentValue returns the current item's value.
	GetCurrentValue() (TV, error)
	// Update finds the item with key and update its value to the value argument.
	Update(key TK, value TV) (bool, error)
	// UpdateCurrentItem will update the Value of the current item.
	// Key is read-only, thus, no argument for the key.
	UpdateCurrentItem(newValue TV) (bool, error)
	// Remove will find the item with a given key then remove that item.
	Remove(key TK) (bool, error)
	// RemoveCurrentItem will remove the current key/value pair from the store.
	RemoveCurrentItem() (bool, error)

	// Cursor like "move" functions. Use the CurrentKey/CurrentValue to retrieve the
	// "current item" details(key &/or value).
	MoveToFirst() (bool, error)
	MoveToLast() (bool, error)
	MoveToNext() (bool, error)
	MoveToPrevious() (bool, error)
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's segment.
	// Otherwise is false.
	IsValueDataInNodeSegment() bool

	// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
	// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
	// unique check during Add of an item, then you can use AddIfNotExist method for that.
	IsUnique() bool
}

// NodeRepository interface specifies the node repository.
type NodeRepository[TK Comparable, TV any] interface {
	Get(nodeId UUID) (*Node[TK, TV], error)
	Upsert(*Node[TK, TV]) error
	Remove(nodeId UUID) error
}

// ItemCacheRepository specifies the methods for CRUD operations on items
// stored/managed in host server's memory. These are short-lived items,
// e.g. - lived only for the duration of the Transaction session that B-Tree is in.
type ItemCacheRepository[TK Comparable, TV any] interface {
	Add(item *Item[TK, TV])
	Get(itemId UUID) (*Item[TK, TV], error)
	Update(item *Item[TK, TV]) error
	Remove(itemId UUID) error
}

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its
// data/objects from a backend.
type StoreInterface[TK Comparable, TV any] struct {
	// NodeRepository is used to manage/access B-Tree nodes.
	NodeRepository NodeRepository[TK, TV]
	// ItemCacheRepository is used to manage/access items in cache.
	ItemCacheRepository ItemCacheRepository[TK, TV]
}
