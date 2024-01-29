package btree

import (
	"context"

	"github.com/SharedCode/sop"
)

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
	// FindOneWithId is synonymous to FindOne but allows code to supply the Item's Id to identify it.
	// This is useful for B-Tree that allows duplicate keys(IsUnique = false) as it provides a way to
	// differentiate duplicated keys via the unique Id(sop.UUID).
	FindOneWithId(ctx context.Context, key TK, id sop.UUID) (bool, error)
	// GetCurrentKey returns the current item's key.
	GetCurrentKey() TK
	// GetCurrentValue returns the current item's value.
	GetCurrentValue(ctx context.Context) (TV, error)
	// GetCurrentItem returns the current item.
	GetCurrentItem(ctx context.Context) (Item[TK, TV], error)

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

	// Returns the number of items in this B-Tree.
	Count() int64
}

// NodeRepository interface specifies the node repository.
type NodeRepository[TK Comparable, TV any] interface {
	// Add will just cache the item, "add" action for submit on transaction commit as appropriate.
	Add(node *Node[TK, TV])
	// Get fetches from backend(or from cache if exists) & returns the Node with a given nodeId.
	Get(ctx context.Context, nodeId sop.UUID) (*Node[TK, TV], error)
	// Mark Node with nodeId as fetched, so, it will get checked for version conflict during commit.
	Fetched(nodeId sop.UUID)
	// Update will just cache the item, "update" action for resolve on transaction commit as appropriate.
	Update(node *Node[TK, TV])
	// Remove will just cache the item, "remove" action for resolve on transaction commit as appropriate.
	Remove(nodeId sop.UUID)
}

// ItemActionTracker specifies the CRUD action methods that can be done to manage Items.
// These action methods can be implemented to allow the backend to resolve and submit
// these changes to the backend storage during transaction commit.
type ItemActionTracker[TK Comparable, TV any] interface {
	// Add will just cache the item, "add" action for submit on transaction commit as appropriate.
	Add(item *Item[TK, TV])
	// Get will just cache the item, "get" action then resolve on transaction commit, compare version
	// with backend copy, error out if version shows another transaction modified/deleted this item on the back.
	Get(item *Item[TK, TV])
	// Update will just cache the item, "update" action for submit on transaction commit as appropriate.
	Update(item *Item[TK, TV])
	// Remove will just cache the item, "remove" action for submit on transaction commit as appropriate.
	Remove(item *Item[TK, TV])
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
