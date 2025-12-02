package btree

import (
	"context"
	"errors"
	"fmt"

	"github.com/sharedcode/sop"
)

// btreeWithTransaction wraps a B-tree with a TwoPhaseCommitTransaction and enforces:
//   - A transaction must have begun before any operation.
//   - Write operations require a writer-mode transaction; otherwise the tx is rolled back.
//   - On any delegated operation error, the wrapper triggers Rollback to keep state consistent.
//
// It does not implement locking yet (see Lock). All methods simply delegate after precondition checks.
type btreeWithTransaction[TK Ordered, TV any] struct {
	// Inherit from B
	BtreeInterface[TK, TV]
	transaction sop.TwoPhaseCommitTransaction
}

// errTransHasNotBegunMsg is returned when an operation is attempted without a begun transaction.
// For read operations below, we proactively call Rollback to ensure no partial state lingers.
var errTransHasNotBegunMsg = errors.New("can't do operation on b-tree if transaction has not begun")

// NewBtreeWithTransaction wraps a B-tree with a transaction session, enforcing that
// operations are run only when a transaction has begun and in the correct mode.
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

// Lock is reserved for coordinating store-level locking; currently a no-op.
func (b3 *btreeWithTransaction[TK, TV]) Lock(ctx context.Context, forWriting bool) error {
	// TODO
	return nil
}

// Write operations: the following methods require a writer-mode transaction.
// They all follow the same pattern:
//   1) Ensure transaction HasBegun and mode == ForWriting, else Rollback and return error
//   2) Delegate to the underlying BtreeInterface
//   3) If the delegate returns error, Rollback with that error

// Add adds a key/value; requires a begun writer transaction.
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

// AddIfNotExist adds only when no duplicate key exists; requires writer transaction.
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

// Upsert inserts or updates depending on existence; requires writer transaction.
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

// Update finds by key and updates value; requires writer transaction.
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

// UpdateKey finds by key and updates key; requires writer transaction.
func (b3 *btreeWithTransaction[TK, TV]) UpdateKey(ctx context.Context, key TK) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.UpdateKey(ctx, key)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// UpdateCurrentValue updates the current item; requires writer transaction.
func (b3 *btreeWithTransaction[TK, TV]) UpdateCurrentValue(ctx context.Context, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.UpdateCurrentValue(ctx, value)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// UpdateCurrentItem updates the current item; requires writer transaction.
func (b3 *btreeWithTransaction[TK, TV]) UpdateCurrentItem(ctx context.Context, key TK, value TV) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.UpdateCurrentItem(ctx, key, value)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// UpdateCurrentKey updates the current item's key; requires writer transaction.
func (b3 *btreeWithTransaction[TK, TV]) UpdateCurrentKey(ctx context.Context, key TK) (bool, error) {
	if !b3.transaction.HasBegun() {
		return false, errTransHasNotBegunMsg
	}
	if b3.transaction.GetMode() != sop.ForWriting {
		b3.transaction.Rollback(ctx, nil)
		return false, fmt.Errorf("can't update item, transaction is not for writing")
	}
	r, err := b3.BtreeInterface.UpdateCurrentKey(ctx, key)
	if err != nil {
		b3.transaction.Rollback(ctx, err)
	}
	return r, err
}

// Remove finds by key and deletes; requires writer transaction.
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

// RemoveCurrentItem deletes the current item; requires writer transaction.
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

// Read-only operations: the following methods only require a begun transaction (any mode).
// On failure or when a transaction has not begun, we call Rollback to terminate the session.

// Find positions the cursor on an exact/first match; requires begun transaction.
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

// FindWithID positions the cursor on a match with specific ID; requires begun transaction.
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

// GetCurrentKey returns the current key/ID; returns zero value if no transaction.
func (b3 *btreeWithTransaction[TK, TV]) GetCurrentKey() Item[TK, TV] {
	var item Item[TK, TV]
	if !b3.transaction.HasBegun() {
		return item
	}
	return b3.BtreeInterface.GetCurrentKey()
}

// GetCurrentValue returns the current value; requires begun transaction.
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

// GetCurrentItem returns the current item; requires begun transaction.
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

// First positions the cursor at the smallest key; requires begun transaction.
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

// Last positions the cursor at the largest key; requires begun transaction.
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

// Next advances the cursor forward; requires begun transaction.
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

// Previous moves the cursor backward; requires begun transaction.
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
