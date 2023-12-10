// Package btree contains the code artifacts implementing the M-Way Trie data structures and algorithms.
// It also contains different interfaces necessary for btree to support different storage backends. In one
// implementation, btree can be in-memory, in another, it can be using other backend storage systems like
// Cassandra and AWS S3.
//
// A b-tree that can distribute items added on a given "leaf" sub-branch so it will tend to fill in the
// nodes of the sub-branch. Instead of achieving half full on average load(typical), each node can then achieve
// higher load average, perhaps up to 62% on average.
// This logic is cut, limited within a given sub-branch so as not to affect performance. If it is found
// to affect performance on a given backend, it may get turned off(TODO).
//
// "leaf" sub-branch is the outermost node of the trie that only has 1 level children, that is, its
// children has no children.
package btree

import "context"

// BtreeInterface defines publicly callable methods of Btree.
type BtreeInterface[TK Comparable, TV any] interface {
	// Add adds an item to the b-tree and does not check for duplicates.
	Add(ctx context.Context, key TK, value TV) (bool, error)
	// AddIfNotExist adds an item if there is no item matching the key yet.
	// Otherwise, it will do nothing and return false, for not adding the item.
	// This is useful for cases one wants to add an item without creating a duplicate entry.
	AddIfNotExist(ctx context.Context, key TK, value TV) (bool, error)

	// Update finds the item with key and update its value to the value argument.
	Update(ctx context.Context, key TK, value TV) (bool, error)
	// UpdateCurrentItem will update the Value of the current item.
	// Key is read-only, thus, no argument for the key.
	UpdateCurrentItem(ctx context.Context, newValue TV) (bool, error)
	// Remove will find the item with a given key then remove that item.
	Remove(ctx context.Context, key TK) (bool, error)
	// RemoveCurrentItem will remove the current key/value pair from the store.
	RemoveCurrentItem(ctx context.Context) (bool, error)

	// FindOne will search Btree for an item with a given key. Return true if found,
	// otherwise false. firstItemWithKey is useful when there are items with same key.
	// true will position pointer to the first item with the given key,
	// according to key ordering sequence.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error)
	// GetCurrentKey returns the current item's key.
	GetCurrentKey(ctx context.Context) (TK, error)
	// GetCurrentValue returns the current item's value.
	GetCurrentValue(ctx context.Context) (TV, error)
	// First positions the "cursor" to the first item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	First(ctx context.Context) (bool, error)
	// Last positionts the "cursor" to the last item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	Last(ctx context.Context) (bool, error)
	// Next positions the "cursor" to the next item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	Next(ctx context.Context) (bool, error)
	// Previous positions the "cursor" to the previous item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	Previous(ctx context.Context) (bool, error)

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
	Get(ctx context.Context, itemId UUID) error
	// Update will fetch data from Redis if it is not yet then mark item as appropriate. 
	Update(ctx context.Context, item *Item[TK, TV]) error
	// Remove will fetch data from Redis if it is not yet then mark item as appropriate.
	Remove(ctx context.Context, itemId UUID) error
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
