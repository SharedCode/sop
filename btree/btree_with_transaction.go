package btree

import (
	"context"
	"errors"
	"fmt"

	"github.com/SharedCode/sop"
)

type btreeWithTransaction[TK Ordered, TV any] struct {
	// Inherit from B
	BtreeInterface[TK, TV]
	transaction sop.TwoPhaseCommitTransaction
}

var errTransHasNotBegunMsg = errors.New("can't do operation on b-tree if transaction has not begun")

// Instantiate a B-Tree wrapper that enforces transaction session on each method(a.k.a. operation).
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

// Add adds an item to the b-tree and does not check for duplicates.
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

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
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

// Upsert will add item if it does not exist or update it if it does.
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

// Update finds the item with key and update its value to the value argument.
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

// UpdateCurrentItem will update the Value of the current item.
// Key is read-only, thus, no argument for the key.
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

// Remove will find the item with a given key then remove that item.
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

// RemoveCurrentItem will remove the current key/value pair from the store.
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

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false. firstItemWithKey is useful when there are items with same key.
// true will position pointer to the first item with the given key,
// according to key ordering sequence.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (b3 *btreeWithTransaction[TK, TV]) FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.FindOne(ctx, key, firstItemWithKey)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}
func (b3 *btreeWithTransaction[TK, TV]) FindOneWithID(ctx context.Context, key TK, id sop.UUID) (bool, error) {
	if !b3.transaction.HasBegun() {
		b3.transaction.Rollback(ctx, nil)
		return false, errTransHasNotBegunMsg
	}
	r, err := b3.BtreeInterface.FindOneWithID(ctx, key, id)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// GetCurrentKey returns the current item's key.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentKey() TK {
	var zero TK
	if !b3.transaction.HasBegun() {
		return zero
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

// First positions the "cursor" to the first item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
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

// Last positionts the "cursor" to the last item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
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

// Next positions the "cursor" to the next item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
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

// Previous positions the "cursor" to the previous item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
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
