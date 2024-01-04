package in_red_ck

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
)

type btreeWithTransaction[TK btree.Comparable, TV any] struct {
	transaction *transaction
	btree       *btree.Btree[interface{}, interface{}]
}

// Instantiate a B-Tree wrapper that enforces transaction session on each method(a.k.a. operation).
func newBtreeWithTransaction[TK btree.Comparable, TV any](t *transaction, btree *btree.Btree[interface{}, interface{}]) *btreeWithTransaction[TK, TV] {
	return &btreeWithTransaction[TK, TV]{
		transaction: t,
		btree:       btree,
	}
}

// Add adds an item to the b-tree and does not check for duplicates.
func (b3 *btreeWithTransaction[TK, TV]) Add(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	if !b3.transaction.forWriting {
		return false, fmt.Errorf("Can't add item, transaction is for reading.")
	}
	return b3.btree.Add(ctx, key, value)
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (b3 *btreeWithTransaction[TK, TV]) AddIfNotExist(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	if !b3.transaction.forWriting {
		return false, fmt.Errorf("Can't add item, transaction is for reading.")
	}
	return b3.btree.AddIfNotExist(ctx, key, value)
}

// Update finds the item with key and update its value to the value argument.
func (b3 *btreeWithTransaction[TK, TV]) Update(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	if !b3.transaction.forWriting {
		return false, fmt.Errorf("Can't update item, transaction is for reading.")
	}
	return b3.btree.Update(ctx, key, value)
}

// UpdateCurrentItem will update the Value of the current item.
// Key is read-only, thus, no argument for the key.
func (b3 *btreeWithTransaction[TK, TV]) UpdateCurrentItem(ctx context.Context, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	if !b3.transaction.forWriting {
		return false, fmt.Errorf("Can't update item, transaction is for reading.")
	}
	return b3.btree.UpdateCurrentItem(ctx, value)
}

// Remove will find the item with a given key then remove that item.
func (b3 *btreeWithTransaction[TK, TV]) Remove(ctx context.Context, key TK) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	if !b3.transaction.forWriting {
		return false, fmt.Errorf("Can't remove item, transaction is for reading.")
	}
	return b3.btree.Remove(ctx, key)
}

// RemoveCurrentItem will remove the current key/value pair from the store.
func (b3 *btreeWithTransaction[TK, TV]) RemoveCurrentItem(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	if !b3.transaction.forWriting {
		return false, fmt.Errorf("Can't remove item, transaction is for reading.")
	}
	return b3.btree.RemoveCurrentItem(ctx)
}

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false. firstItemWithKey is useful when there are items with same key.
// true will position pointer to the first item with the given key,
// according to key ordering sequence.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (b3 *btreeWithTransaction[TK, TV]) FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	return b3.btree.FindOne(ctx, key, firstItemWithKey)
}
func (b3 *btreeWithTransaction[TK, TV]) FindOneWithId(ctx context.Context, key TK, id btree.UUID) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	return b3.btree.FindOneWithId(ctx, key, id)
}

// GetCurrentKey returns the current item's key.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentKey(ctx context.Context) (TK, error) {
	var zero TK
	if !b3.transaction.HasBegun() {
		return zero, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	k, err := b3.btree.GetCurrentKey(ctx)
	return k.(TK), err
}

// GetCurrentValue returns the current item's value.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentValue(ctx context.Context) (TV, error) {
	var zero TV
	if !b3.transaction.HasBegun() {
		return zero, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	v, err := b3.btree.GetCurrentValue(ctx)
	return v.(TV), err
}

// GetCurrentItem returns the current item.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentItem(ctx context.Context) (btree.Item[TK, TV], error) {
	var zero btree.Item[TK, TV]
	if !b3.transaction.HasBegun() {
		return zero, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	return b3.GetCurrentItem(ctx)
}

// First positions the "cursor" to the first item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (b3 *btreeWithTransaction[TK, TV]) First(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	return b3.btree.First(ctx)
}

// Last positionts the "cursor" to the last item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (b3 *btreeWithTransaction[TK, TV]) Last(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	return b3.btree.Last(ctx)
}

// Next positions the "cursor" to the next item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (b3 *btreeWithTransaction[TK, TV]) Next(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	return b3.btree.Next(ctx)
}

// Previous positions the "cursor" to the previous item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (b3 *btreeWithTransaction[TK, TV]) Previous(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, fmt.Errorf("Can't do operation on b-tree if transaction has not begun.")
	}
	return b3.btree.Previous(ctx)
}

// IsValueDataInNodeSegment is true if "Value" data is stored in the B-Tree node's segment.
// Otherwise is false.
func (b3 *btreeWithTransaction[TK, TV]) IsValueDataInNodeSegment() bool {
	return b3.btree.IsValueDataInNodeSegment()
}

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// unique check during Add of an item, then you can use AddIfNotExist method for that.
func (b3 *btreeWithTransaction[TK, TV]) IsUnique() bool {
	return b3.btree.IsUnique()
}
