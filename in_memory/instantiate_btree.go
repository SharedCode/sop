package in_memory

import (
	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// BtreeInterface struct defines publicly callable methods of Btree in-memory.
// NOTE: this is synonymous to the btree.BtreeInterface but with methods removed of error
// in return. Because in-memory will not produce any error during access,
// thus, it can be simplified so code will not need to bother with the 2nd (error) return.
type BtreeInterface[TK btree.Ordered, TV any] struct {
	// Inherit from Btree.
	*btree.Btree[TK, TV]
}

// For in-memory b-tree, hardcode to 8 items per node. We don't need wide array for in-memory.
const itemsPerNode = 8

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
func NewBtree[TK btree.Ordered, TV any](isUnique bool) BtreeInterface[TK, TV] {
	so := sop.StoreOptions{
		Name:                         "",
		SlotLength:                   itemsPerNode,
		IsUnique:                     isUnique,
		IsValueDataInNodeSegment:     true,
		IsValueDataActivelyPersisted: true,
	}
	s := sop.NewStoreInfo(so)
	si := btree.StoreInterface[TK, TV]{
		NodeRepository:    newNodeRepository[TK, TV](),
		ItemActionTracker: newDumbItemActionTracker[TK, TV](),
	}
	b3, _ := btree.New[TK, TV](s, &si, nil)
	return BtreeInterface[TK, TV]{
		Btree: b3,
	}
}

// Returns the Count of items in the B-Tree.
func (b3 BtreeInterface[TK, TV]) Count() int {
	return int(b3.Btree.Count())
}

// Add adds an item to the b-tree and does not check for duplicates.
func (b3 BtreeInterface[TK, TV]) Add(key TK, value TV) bool {
	ok, _ := b3.Btree.Add(nil, key, value)
	return ok
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (b3 BtreeInterface[TK, TV]) AddIfNotExist(key TK, value TV) bool {
	ok, _ := b3.Btree.AddIfNotExist(nil, key, value)
	return ok
}

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false. firstItemWithKey is useful when there are items with same key.
// true will position pointer to the first item with the given key,
// according to key ordering sequence.
func (b3 BtreeInterface[TK, TV]) Find(key TK, firstItemWithKey bool) bool {
	ok, _ := b3.Btree.Find(nil, key, firstItemWithKey)
	return ok
}

// GetCurrentKey returns the current item's value.
func (b3 BtreeInterface[TK, TV]) GetCurrentKey() TK {
	return b3.Btree.GetCurrentKey().Key
}

// GetCurrentValue returns the current item's value.
func (b3 BtreeInterface[TK, TV]) GetCurrentValue() TV {
	v, _ := b3.Btree.GetCurrentValue(nil)
	return v
}

// Update finds the item with key and update its value to the value argument.
func (b3 BtreeInterface[TK, TV]) Update(key TK, value TV) bool {
	ok, _ := b3.Btree.Update(nil, key, value)
	return ok
}

// Upsert will add the item if not found or update it if it exists.
func (b3 BtreeInterface[TK, TV]) Upsert(key TK, value TV) bool {
	ok, _ := b3.Btree.Upsert(nil, key, value)
	return ok
}

// UpdateCurrentItem will update the Value of the current item.
// Key is read-only, thus, no argument for the key.
func (b3 BtreeInterface[TK, TV]) UpdateCurrentItem(newValue TV) bool {
	ok, _ := b3.Btree.UpdateCurrentItem(nil, newValue)
	return ok
}

// Remove will find the item with a given key then remove that item.
func (b3 BtreeInterface[TK, TV]) Remove(key TK) bool {
	ok, _ := b3.Btree.Remove(nil, key)
	return ok
}

// RemoveCurrentItem will remove the current key/value pair from the store.
func (b3 BtreeInterface[TK, TV]) RemoveCurrentItem() bool {
	ok, _ := b3.Btree.RemoveCurrentItem(nil)
	return ok
}

// First positions the "cursor" to the first item as per key ordering.
func (b3 BtreeInterface[TK, TV]) First() bool {
	ok, _ := b3.Btree.First(nil)
	return ok
}

// Last positionts the "cursor" to the last item as per key ordering.
func (b3 BtreeInterface[TK, TV]) Last() bool {
	ok, _ := b3.Btree.Last(nil)
	return ok
}

// Next positions the "cursor" to the next item as per key ordering.
func (b3 BtreeInterface[TK, TV]) Next() bool {
	ok, _ := b3.Btree.Next(nil)
	return ok
}

// Previous positions the "cursor" to the previous item as per key ordering.
func (b3 BtreeInterface[TK, TV]) Previous() bool {
	ok, _ := b3.Btree.Previous(nil)
	return ok
}
