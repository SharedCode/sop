package btree

import "context"

// store_interfaces contains interface definitions of different repository that are
// required by Btree. It is needed so we can support different backend storage.

// BtreeInterface defines publicly callable methods of Btree.
type BtreeInterface[TK Comparable, TV any] interface {
	// Add adds an item to the b-tree and does not check for duplicates.
	Add(ctx context.Context, key TK, value TV) (bool, error)
	// AddIfNotExist adds an item if there is no item matching the key yet.
	// Otherwise, it will do nothing and return false, for not adding the item.
	// This is useful for cases one wants to add an item without creating a duplicate entry.
	AddIfNotExist(ctx context.Context, key TK, value TV) (bool, error)
	// FindOne will search Btree for an item with a given key. Return true if found,
	// otherwise false. firstItemWithKey is useful when there are items with same key.
	// true will position pointer to the first item with the given key,
	// according to key ordering sequence.
	FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error)
	// GetCurrentKey returns the current item's key.
	GetCurrentKey(ctx context.Context) (TK, error)
	// GetCurrentValue returns the current item's value.
	GetCurrentValue(ctx context.Context) (TV, error)
	// Update finds the item with key and update its value to the value argument.
	Update(ctx context.Context, key TK, value TV) (bool, error)
	// UpdateCurrentItem will update the Value of the current item.
	// Key is read-only, thus, no argument for the key.
	UpdateCurrentItem(ctx context.Context, newValue TV) (bool, error)
	// Remove will find the item with a given key then remove that item.
	Remove(ctx context.Context, key TK) (bool, error)
	// RemoveCurrentItem will remove the current key/value pair from the store.
	RemoveCurrentItem(ctx context.Context) (bool, error)

	// Cursor like "move" functions. Use the CurrentKey/CurrentValue to retrieve the
	// "current item" details(key &/or value).
	MoveToFirst(ctx context.Context) (bool, error)
	MoveToLast(ctx context.Context) (bool, error)
	MoveToNext(ctx context.Context) (bool, error)
	MoveToPrevious(ctx context.Context) (bool, error)
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
	Get(ctx context.Context, nodeId UUID) (*Node[TK, TV], error)
	Upsert(ctx context.Context, node *Node[TK, TV]) error
	Remove(ctx context.Context, nodeId UUID) error
}

// ItemActionTracker specifies the CRUD action methods that can be done to manage Items.
// These action methods can be implemented to allow the backend to resolve and submit 
// these changes to the backend storage during transaction commit.
type ItemActionTracker[TK Comparable, TV any] interface {
	// Add will just cache the item for submit on transction commit.
	Add(item *Item[TK, TV])
	// Get will fetch data from Redis if it is not yet then mark item as appropriate.
	Get(ctx context.Context, itemId UUID)
	// Update will fetch data from Redis if it is not yet then mark item as appropriate. 
	Update(ctx context.Context, item *Item[TK, TV])
	// Remove will fetch data from Redis if it is not yet then mark item as appropriate.
	Remove(ctx context.Context, itemId UUID)
}

// StoreInterface contains different repositories needed/used by B-Tree to manage/access its
// data/objects from a backend.
type StoreInterface[TK Comparable, TV any] struct {
	// NodeRepository is used to manage/access B-Tree nodes.
	NodeRepository NodeRepository[TK, TV]
	// ItemActionTracker is used to track management actions to Items which
	// are geared for resolution & submit on the backend during transaction commit.
	ItemActionTracker ItemActionTracker[TK, TV]
}
