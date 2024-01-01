package in_cas_s3

import (
	"context"
	"time"
)

// Transaction interface defines the "enduser facing" transaction methods.
type Transaction interface {
	// Begin the transaction.
	Begin() error
	// Commit the transaction.
	Commit(ctx context.Context) error
	// Rollback the transaction.
	Rollback(ctx context.Context) error
	// Returns true if transaction has begun, false otherwise.
	HasBegun() bool

	// Returns the two phased commit transaction object. Useful for integration with your application
	// "other" database transactions. Returned transaction object will allow your code to call the
	// two phases commit of SOP.
	GetPhasedTransaction() TwoPhaseCommitTransaction
	// Add your two phases commit implementation for managing your/3rd party database transaction.
	AddPhasedTransaction(otherTransaction ...TwoPhaseCommitTransaction)
}

type singlePhaseTransaction struct {
	sopPhaseCommitTransaction TwoPhaseCommitTransaction
	otherTransactions         []TwoPhaseCommitTransaction
}

// NewTransaction creates an enduser facing transaction object.
func NewTransaction(forWriting bool, maxTime time.Duration) Transaction {
	return &singlePhaseTransaction{
		sopPhaseCommitTransaction: NewTwoPhaseCommitTransaction(forWriting, maxTime),
	}
}

// Begin the transaction.
func (t *singlePhaseTransaction) Begin() error {
	if err := t.sopPhaseCommitTransaction.Begin(); err != nil {
		return err
	}
	for _, t := range t.otherTransactions {
		if err := t.Begin(); err != nil {
			return err
		}
	}
	return nil
}

// Commit the transaction. If multiple phase 1 commit erors are returned,
// this will return the sop phase 1 commit error or
// your other transactions phase 1 commits' last error.
func (t *singlePhaseTransaction) Commit(ctx context.Context) error {
	var lastErr error
	// Phase 1 commit.
	if err := t.sopPhaseCommitTransaction.Phase1Commit(ctx); err != nil {
		t.Rollback(ctx)
		return err
	}
	for _, ot := range t.otherTransactions {
		if err := ot.Phase1Commit(ctx); err != nil {
			lastErr = err
		}
	}
	if lastErr != nil {
		t.Rollback(ctx)
		return lastErr
	}

	// Phase 2 commit.
	if err := t.sopPhaseCommitTransaction.Phase2Commit(ctx); err != nil {
		t.Rollback(ctx)
		return err
	}
	// If SOP phase 2 commit succeeds, then all other transactions phase 2 commit are
	// expected to succeed, returned error will be ignored.
	for _, ot := range t.otherTransactions {
		ot.Phase2Commit(ctx)
	}
	return nil
}

// Rollback the transaction. If multiple transaction rollbacks errored,
// this will return the last error.
func (t *singlePhaseTransaction) Rollback(ctx context.Context) error {
	t.sopPhaseCommitTransaction.Rollback(ctx)
	var lastErr error
	for _, ot := range t.otherTransactions {
		if err := ot.Rollback(ctx); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Returns true if transaction has begun, false otherwise.
func (t *singlePhaseTransaction) HasBegun() bool {
	return t.sopPhaseCommitTransaction.HasBegun()
}

// Returns the two phased commit transaction object. Useful for integration with your application
// "other" database transactions. Returned transaction object will allow your code to call the
// two phases commit of SOP.
func (t *singlePhaseTransaction) GetPhasedTransaction() TwoPhaseCommitTransaction {
	return t.sopPhaseCommitTransaction
}

// Add your two phases commit implementation for managing your/3rd party database transaction.
func (t *singlePhaseTransaction) AddPhasedTransaction(otherTransaction ...TwoPhaseCommitTransaction) {
	t.otherTransactions = append(t.otherTransactions, otherTransaction...)
}
