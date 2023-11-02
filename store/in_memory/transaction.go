package in_memory

import (
	"fmt"

	"github.com/SharedCode/sop/btree"
)

type transaction[TK btree.Comparable, TV any] struct {
	id            btree.UUID
	nodeRepository btree.NodeRepository[TK, TV]
}

func newTransaction[TK btree.Comparable, TV any]() btree.Transaction {
	return &transaction[TK, TV]{}
}

func (trans *transaction[TK, TV]) Begin() error {
	if trans.HasBegun() {
		return fmt.Errorf("Transaction already begun.")
	}
	trans.id = btree.NewUUID()
	return nil
}

// CommitPhase1 commits all changes to each Btree modified during transaction.
func (trans *transaction[TK, TV]) Commit() error {
	if !trans.HasBegun() {
		return fmt.Errorf("Transaction has not began, nothing to commit.")
	}
	trans.id = btree.NilUUID
	return nil
}

// Rollback undoes any changes done to each Btree modified during transaction.
func (trans *transaction[TK, TV]) Rollback() error {
	if !trans.HasBegun() {
		return fmt.Errorf("Transaction has not began, nothing to rollback.")
	}
	trans.id = btree.NilUUID
	return nil
}

// HasBegun returns true if this tranaction is "open", false otherwise.
func (trans *transaction[TK, TV]) HasBegun() bool {
	return trans.id != btree.NilUUID
}
