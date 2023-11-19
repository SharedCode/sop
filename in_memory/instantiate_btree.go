package in_memory

import "github.com/SharedCode/sop/btree"

// BtreeInterface defines publicly callable methods of Btree in-memory.
// NOTE: this is synonymous to the btree.BtreeInterface but with methods removed of error in return.
// Because in-memory will not produce any error during access, thus, it can be simplified so code will not need
// to bother with the 2nd (error) return.
type BtreeInterface[TK btree.Comparable, TV any] interface {
	// Add adds an item to the b-tree and does not check for duplicates.
	Add(key TK, value TV) bool
	// AddIfNotExist adds an item if there is no item matching the key yet.
	// Otherwise, it will do nothing and return false, for not adding the item.
	// This is useful for cases one wants to add an item without creating a duplicate entry.
	AddIfNotExist(key TK, value TV) bool
	// FindOne will search Btree for an item with a given key. Return true if found,
	// otherwise false. firstItemWithKey is useful when there are items with same key.
	// true will position pointer to the first item with the given key,
	// according to key ordering sequence.
	FindOne(key TK, firstItemWithKey bool) bool
	// GetCurrentKey returns the current item's key.
	GetCurrentKey() TK
	// GetCurrentValue returns the current item's value.
	GetCurrentValue() TV
	// Update finds the item with key and update its value to the value argument.
	Update(key TK, value TV) bool
	// UpdateCurrentItem will update the Value of the current item.
	// Key is read-only, thus, no argument for the key.
	UpdateCurrentItem(newValue TV) bool
	// Remove will find the item with a given key then remove that item.
	Remove(key TK) bool
	// RemoveCurrentItem will remove the current key/value pair from the store.
	RemoveCurrentItem() bool

	// Cursor like "move" functions. Use the CurrentKey/CurrentValue to retrieve the
	// "current item" details(key &/or value).
	MoveToFirst() bool
	MoveToLast() bool
	MoveToNext() bool
	MoveToPrevious() bool
	// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's segment.
	// Otherwise is false.
	IsValueDataInNodeSegment() bool

	// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
	// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
	// unique check during Add of an item, then you can use AddIfNotExist method for that.
	IsUnique() bool
}

type inmemoryBtree[TK btree.Comparable, TV any] struct {
	btree btree.BtreeInterface[TK, TV]
}

// For in-memory b-tree, hardcode to 8 items per node. We don't need wide array for in-memory.
const itemsPerNode = 8

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
func NewBtree[TK btree.Comparable, TV any](isUnique bool) (BtreeInterface[TK, TV]) {
	transactionManager := newTransactionManager[TK,TV]()
	s := btree.NewStore("", itemsPerNode, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	b3 := btree.NewBtree[TK, TV](s, transactionManager.storeInterface)
	return inmemoryBtree[TK, TV] {
		btree: b3,
	}
}

// NewBtree will create an in-memory B-Tree & its required data stores. You can use it to store
// and access key/value pairs similar to a map but which, sorts items & allows "range queries".
// This will return btree instance that has no wrapper, thus, methods have error in return where appropriate.
// Handy for using in-memory b-tree for writing unit tests to mock the "Enterprise" V2 version.
func NewBtreeWithNoWrapper[TK btree.Comparable, TV any](isUnique bool) (btree.BtreeInterface[TK, TV]) {
	transactionManager := newTransactionManager[TK,TV]()
	s := btree.NewStore("", itemsPerNode, isUnique, true)
	transactionManager.storeInterface.StoreRepository.Add(s)
	return btree.NewBtree[TK, TV](s, transactionManager.storeInterface)
}

// Add adds an item to the b-tree and does not check for duplicates.
func (b3 inmemoryBtree[TK, TV]) Add(key TK, value TV) bool {
	ok,_ := b3.btree.Add(key, value)
	return ok
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (b3 inmemoryBtree[TK, TV]) AddIfNotExist(key TK, value TV) bool {
	ok,_ := b3.btree.AddIfNotExist(key, value)
	return ok
}

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false. firstItemWithKey is useful when there are items with same key.
// true will position pointer to the first item with the given key,
// according to key ordering sequence.
func (b3 inmemoryBtree[TK, TV]) FindOne(key TK, firstItemWithKey bool) bool {
	ok,_ := b3.btree.FindOne(key, firstItemWithKey)
	return ok
}

// GetCurrentKey returns the current item's key.
func (b3 inmemoryBtree[TK, TV]) GetCurrentKey() TK {
	return b3.btree.GetCurrentKey()
}

// GetCurrentValue returns the current item's value.
func (b3 inmemoryBtree[TK, TV]) GetCurrentValue() TV {
	v,_ := b3.btree.GetCurrentValue()
	return v
}

// Update finds the item with key and update its value to the value argument.
func (b3 inmemoryBtree[TK, TV]) Update(key TK, value TV) bool {
	ok,_ := b3.btree.Update(key, value)
	return ok
}

// UpdateCurrentItem will update the Value of the current item.
// Key is read-only, thus, no argument for the key.
func (b3 inmemoryBtree[TK, TV]) UpdateCurrentItem(newValue TV) bool {
	ok,_ := b3.btree.UpdateCurrentItem(newValue)
	return ok
}

// Remove will find the item with a given key then remove that item.
func (b3 inmemoryBtree[TK, TV]) Remove(key TK) bool {
	ok,_ := b3.btree.Remove(key)
	return ok
}

// RemoveCurrentItem will remove the current key/value pair from the store.
func (b3 inmemoryBtree[TK, TV]) RemoveCurrentItem() bool {
	ok,_ := b3.btree.RemoveCurrentItem()
	return ok
}

// Cursor like "move" functions. Use the CurrentKey/CurrentValue to retrieve the
// "current item" details(key &/or value).
func (b3 inmemoryBtree[TK, TV]) MoveToFirst() bool {
	ok,_ := b3.btree.MoveToFirst()
	return ok
}

func (b3 inmemoryBtree[TK, TV]) MoveToLast() bool {
	ok,_ := b3.btree.MoveToLast()
	return ok
}

func (b3 inmemoryBtree[TK, TV]) MoveToNext() bool {
	ok,_ := b3.btree.MoveToNext()
	return ok
}

func (b3 inmemoryBtree[TK, TV]) MoveToPrevious() bool {
	ok,_ := b3.btree.MoveToPrevious()
	return ok
}

// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's segment.
// Otherwise is false.
func (b3 inmemoryBtree[TK, TV]) IsValueDataInNodeSegment() bool {
	return b3.btree.IsValueDataInNodeSegment()
}

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// unique check during Add of an item, then you can use AddIfNotExist method for that.
func (b3 inmemoryBtree[TK, TV]) IsUnique() bool {
	return b3.btree.IsUnique()
}
