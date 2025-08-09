package btree

import (
	"context"
	"errors"
	"fmt"

	"github.com/sharedcode/sop"
)

type btreeWithTransaction[TK Ordered, TV any] struct {
	// Inherit from B
	BtreeInterface[TK, TV]
	transaction sop.TwoPhaseCommitTransaction
}

var errTransHasNotBegunMsg = errors.New("can't do operation on b-tree if transaction has not begun")

// NewBtreeWithTransaction wraps a B-tree with a transaction session, enforcing that
// each operation occurs within a begun transaction and in the correct mode.
func NewBtreeWithTransaction[TK Ordered, TV any](t sop.TwoPhaseCommitTransaction, btree BtreeInterface[TK, TV]) *btreeWithTransaction[TK, TV] {
	return &btreeWithTransaction[TK, TV]{
		transaction:    t,
		BtreeInterface: btree,
	}
}

/*
	- Implement Lock & unlock on commit.
	- Implement Node early persist.
	- Implement MRU caching.
*/

func (b3 *btreeWithTransaction[TK, TV]) Lock(ctx context.Context, forWriting bool) error {
	// TODO
	return nil
}

// Add adds an item to the B-tree without checking for duplicates.
func (b3 *btreeWithTransaction[TK, TV]) Add(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't add item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.Add(ctx, key, value)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// AddIfNotExist adds an item if there is no item matching the key; otherwise it
// returns false and leaves the B-tree unchanged.
func (b3 *btreeWithTransaction[TK, TV]) AddIfNotExist(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't add item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.AddIfNotExist(ctx, key, value)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Upsert adds the item if it does not exist or updates it if it does.
func (b3 *btreeWithTransaction[TK, TV]) Upsert(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.Upsert(ctx, key, value)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Update finds the item with key and updates its value to the provided value.
func (b3 *btreeWithTransaction[TK, TV]) Update(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.Update(ctx, key, value)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// UpdateCurrentItem updates the Value of the current item. Key is read-only.
func (b3 *btreeWithTransaction[TK, TV]) UpdateCurrentItem(ctx context.Context, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.UpdateCurrentItem(ctx, value)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Remove finds the item with the given key and removes it.
func (b3 *btreeWithTransaction[TK, TV]) Remove(ctx context.Context, key TK) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.Remove(ctx, key)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// RemoveCurrentItem removes the current key/value pair from the store.
func (b3 *btreeWithTransaction[TK, TV]) RemoveCurrentItem(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't remove item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.RemoveCurrentItem(ctx)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Find searches the B-tree for an item with the given key. It returns true if found.
// When firstItemWithKey is true and duplicates exist, it positions the cursor to the first match.
// Use GetCurrentKey/GetCurrentValue to retrieve the current item.
func (b3 *btreeWithTransaction[TK, TV]) Find(ctx context.Context, key TK, firstItemWithKey bool) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.Find(ctx, key, firstItemWithKey)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// FindWithID searches the B-tree for an item with the given key and ID.
func (b3 *btreeWithTransaction[TK, TV]) FindWithID(ctx context.Context, key TK, id sop.UUID) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.FindWithID(ctx, key, id)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// GetCurrentKey returns the current item's key and ID.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentKey() Item[TK, TV] {
	var item Item[TK, TV]
	if !b3.transaction.HasBegun() {
		return item
	}
	return b3.BtreeInterface.GetCurrentKey()
}

// GetCurrentValue returns the current item's value.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentValue(ctx context.Context) (TV, error) {
	var zero TV
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return zero, errTransHasNotBegunMsg
	}
	v, err := b3.BtreeInterface.GetCurrentValue(ctx)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return v, err
}

// GetCurrentItem returns the current item.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentItem(ctx context.Context) (Item[TK, TV], error) {
	var zero Item[TK, TV]
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return zero, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.GetCurrentItem(ctx)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// First positions the cursor to the first item as per key ordering.
// Use GetCurrentKey/GetCurrentValue to retrieve the current item.
func (b3 *btreeWithTransaction[TK, TV]) First(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.First(ctx)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Last positions the cursor to the last item as per key ordering.
// Use GetCurrentKey/GetCurrentValue to retrieve the current item.
func (b3 *btreeWithTransaction[TK, TV]) Last(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.Last(ctx)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Next positions the cursor to the next item as per key ordering.
// Use GetCurrentKey/GetCurrentValue to retrieve the current item.
func (b3 *btreeWithTransaction[TK, TV]) Next(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.Next(ctx)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Previous positions the cursor to the previous item as per key ordering.
// Use GetCurrentKey/GetCurrentValue to retrieve the current item.
func (b3 *btreeWithTransaction[TK, TV]) Previous(ctx context.Context) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.Previous(ctx)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}
