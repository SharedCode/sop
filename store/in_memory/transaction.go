package in_memory

import (
	"fmt"
	"github.com/SharedCode/sop/btree"
)

type TransactionSession struct {
	hasBegun bool
}

func NewTransaction() btree.Transaction {
	return &TransactionSession{}
}

func (trans *TransactionSession) Begin() error {
	if trans.hasBegun {
		return fmt.Errorf("Transaction already begun.")
	}
	trans.hasBegun = true
	return nil
}

// CommitPhase1 commits all changes to each Btree modified during transaction.
func (trans *TransactionSession) Commit() error {
	if !trans.hasBegun {
		return fmt.Errorf("Transaction has not began, nothing to commit.")
	}
	trans.hasBegun = false
	return nil
}

// Rollback undoes any changes done to each Btree modified during transaction.
func (trans *TransactionSession) Rollback() error {
	if !trans.hasBegun {
		return fmt.Errorf("Transaction has not began, nothing to rollback.")
	}
	trans.hasBegun = false
	return nil
}

// HasBegun returns true if this tranaction is "open", false otherwise.
func (trans *TransactionSession) HasBegun() bool {
	return trans.hasBegun
}
