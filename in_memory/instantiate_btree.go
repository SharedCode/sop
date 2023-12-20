package in_memory

import "github.com/SharedCode/sop/btree"

// BtreeInterface struct defines publicly callable methods of Btree in-memory.
// NOTE: this is synonymous to the btree.BtreeInterface but with methods removed of error
// in return. Because in-memory will not produce any error during access,
// thus, it can be simplified so code will not need to bother with the 2nd (error) return.
type btreeInterface[TK btree.Comparable, TV any] struct {
	btree *btree.Btree[TK, TV]
}

// BtreeInterface defines publicly callable methods of in-memory Btree.
type BtreeInterface[TK btree.Comparable, TV any] interface {
	// Add adds an item to the b-tree and does not check for duplicates.
	Add(key TK, value TV) bool
	// AddIfNotExist adds an item if there is no item matching the key yet.
	// Otherwise, it will do nothing and return false, for not adding the item.
	// This is useful for cases one wants to add an item without creating a duplicate entry.
	AddIfNotExist(key TK, value TV) bool

	// Update finds the item with key and update its value to the value argument.
	Update(key TK, value TV) bool
	// UpdateCurrentItem will update the Value of the current item.
	// Key is read-only, thus, no argument for the key.
	UpdateCurrentItem(newValue TV) bool
	// Remove will find the item with a given key then remove that item.
	Remove(key TK) bool
	// RemoveCurrentItem will remove the current key/value pair from the store.
	RemoveCurrentItem() bool

	// FindOne will search Btree for an item with a given key. Return true if found,
	// otherwise false. firstItemWithKey is useful when there are items with same key.
	// true will position pointer to the first item with the given key,
	// according to key ordering sequence.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	FindOne(key TK, firstItemWithKey bool) bool
	// GetCurrentKey returns the current item's key.
	GetCurrentKey() TK
	// GetCurrentValue returns the current item's value.
	GetCurrentValue() TV

	// First positions the "cursor" to the first item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	First() bool
	// Last positionts the "cursor" to the last item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	Last() bool
	// Next positions the "cursor" to the next item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	Next() bool
	// Previous positions the "cursor" to the previous item as per key ordering.
	// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
	Previous() (bool)

	// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
	// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
	// unique check during Add of an item, then you can use AddIfNotExist method for that.
	IsUnique() bool
}


// For in-memory b-tree, hardcode to 8 items per node. We don't need wide array for in-memory.
const itemsPerNode = 8

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
func NewBtree[TK btree.Comparable, TV any](isUnique bool) BtreeInterface[TK, TV] {
	s := btree.NewStoreInfo("", itemsPerNode, isUnique, true)
	si := btree.StoreInterface[TK, TV]{
		NodeRepository: newNodeRepository[TK, TV](),
	}
	b3 := btree.NewBtree[TK, TV](s, &si)
	return btreeInterface[TK, TV]{
		btree: b3,
	}
}

// Add adds an item to the b-tree and does not check for duplicates.
func (b3 btreeInterface[TK, TV]) Add(key TK, value TV) bool {
	ok, _ := b3.btree.Add(nil, key, value)
	return ok
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (b3 btreeInterface[TK, TV]) AddIfNotExist(key TK, value TV) bool {
	ok, _ := b3.btree.AddIfNotExist(nil, key, value)
	return ok
}

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false. firstItemWithKey is useful when there are items with same key.
// true will position pointer to the first item with the given key,
// according to key ordering sequence.
func (b3 btreeInterface[TK, TV]) FindOne(key TK, firstItemWithKey bool) bool {
	ok, _ := b3.btree.FindOne(nil, key, firstItemWithKey)
	return ok
}

// GetCurrentKey returns the current item's key.
func (b3 btreeInterface[TK, TV]) GetCurrentKey() TK {
	k, _ := b3.btree.GetCurrentKey(nil)
	return k
}

// GetCurrentValue returns the current item's value.
func (b3 btreeInterface[TK, TV]) GetCurrentValue() TV {
	v, _ := b3.btree.GetCurrentValue(nil)
	return v
}

// Update finds the item with key and update its value to the value argument.
func (b3 btreeInterface[TK, TV]) Update(key TK, value TV) bool {
	ok, _ := b3.btree.Update(nil, key, value)
	return ok
}

// UpdateCurrentItem will update the Value of the current item.
// Key is read-only, thus, no argument for the key.
func (b3 btreeInterface[TK, TV]) UpdateCurrentItem(newValue TV) bool {
	ok, _ := b3.btree.UpdateCurrentItem(nil, newValue)
	return ok
}

// Remove will find the item with a given key then remove that item.
func (b3 btreeInterface[TK, TV]) Remove(key TK) bool {
	ok, _ := b3.btree.Remove(nil, key)
	return ok
}

// RemoveCurrentItem will remove the current key/value pair from the store.
func (b3 btreeInterface[TK, TV]) RemoveCurrentItem() bool {
	ok, _ := b3.btree.RemoveCurrentItem(nil)
	return ok
}

// First positions the "cursor" to the first item as per key ordering.
func (b3 btreeInterface[TK, TV]) First() bool {
	ok, _ := b3.btree.First(nil)
	return ok
}

// Last positionts the "cursor" to the last item as per key ordering.
func (b3 btreeInterface[TK, TV]) Last() bool {
	ok, _ := b3.btree.Last(nil)
	return ok
}

// Next positions the "cursor" to the next item as per key ordering.
func (b3 btreeInterface[TK, TV]) Next() bool {
	ok, _ := b3.btree.Next(nil)
	return ok
}

// Previous positions the "cursor" to the previous item as per key ordering.
func (b3 btreeInterface[TK, TV]) Previous() bool {
	ok, _ := b3.btree.Previous(nil)
	return ok
}

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// unique check during Add of an item, then you can use AddIfNotExist method for that.
func (b3 btreeInterface[TK, TV]) IsUnique() bool {
	return b3.btree.IsUnique()
}
