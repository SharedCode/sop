// Package sop contains SOP integration code for Redis, Cassandra & Kafka (in_red_c).
package sop

import (
	"context"
	"fmt"
	log "log/slog"
	"time"
)

// TransactionMode enumerates the supported transaction behaviors.
type TransactionMode int

const (
	// NoCheck disallows any changes and skips read-version checks during commit.
	NoCheck TransactionMode = iota
	// ForWriting allows modifications to B-Tree stores within the transaction.
	ForWriting
	// ForReading disallows modifications; read-only.
	ForReading
)

// Transaction defines end-user-facing transactional operations.
type Transaction interface {
	// Begin starts the transaction.
	Begin() error
	// Commit finalizes the transaction.
	Commit(ctx context.Context) error
	// Rollback aborts the transaction.
	Rollback(ctx context.Context) error
	// HasBegun reports whether the transaction has started.
	HasBegun() bool

	// GetPhasedTransaction returns the underlying two-phase commit transaction for orchestration with other systems.
	GetPhasedTransaction() TwoPhaseCommitTransaction
	// AddPhasedTransaction registers external two-phase commit participants.
	AddPhasedTransaction(otherTransaction ...TwoPhaseCommitTransaction)

	// GetStores lists all available B-Tree stores from the backend.
	GetStores(ctx context.Context) ([]string, error)

	// Close releases any resources associated with the transaction.
	Close() error

	// GetID returns the transaction ID.
	GetID() UUID
}

// TwoPhaseCommitTransaction defines infrastructure-facing two-phase commit operations.
type TwoPhaseCommitTransaction interface {
	// Begin starts the transaction.
	Begin() error
	// Phase1Commit performs the first phase (prepare) of the commit.
	Phase1Commit(ctx context.Context) error
	// Phase2Commit performs the second phase (finalize) of the commit.
	Phase2Commit(ctx context.Context) error
	// Rollback aborts the transaction and may be provided an error cause.
	Rollback(ctx context.Context, err error) error
	// HasBegun reports whether the transaction has started.
	HasBegun() bool
	// GetMode returns the configured TransactionMode.
	GetMode() TransactionMode

	// GetStores lists all available B-Tree stores from the backend.
	GetStores(ctx context.Context) ([]string, error)

	// Close releases any resources associated with the transaction implementation.
	Close() error

	// GetID returns the transaction ID.
	GetID() UUID
}

// SinglePhaseTransaction wraps a TwoPhaseCommitTransaction providing an end-user friendly API
// and optional participation of other two-phase commit transactions.
type SinglePhaseTransaction struct {
	SopPhaseCommitTransaction TwoPhaseCommitTransaction
	otherTransactions         []TwoPhaseCommitTransaction
}

// NewTransaction constructs a Transaction wrapper around a TwoPhaseCommitTransaction.
// mode controls permissions; maxTime caps the allowed commit duration (-1 uses 15m). When logging is true
// SOP records commit steps to aid recovery and cleanup of expired resources.
func NewTransaction(mode TransactionMode,
	twoPhaseCommitTrans TwoPhaseCommitTransaction,
	maxTime time.Duration, logging bool) (Transaction, error) {
	twoPhase := twoPhaseCommitTrans // NewTwoPhaseCommitTransaction(mode, maxTime, logging)
	return &SinglePhaseTransaction{
		SopPhaseCommitTransaction: twoPhase,
	}, nil
}

// Begin starts the wrapped transaction and any registered participants.
func (t *SinglePhaseTransaction) Begin() error {
	if err := t.SopPhaseCommitTransaction.Begin(); err != nil {
		return err
	}
	for _, t := range t.otherTransactions {
		if err := t.Begin(); err != nil {
			return err
		}
	}

	return nil
}

// Close calls Close on the wrapped transaction implementation.
func (t *SinglePhaseTransaction) Close() error {
	return t.SopPhaseCommitTransaction.Close()
}

// Commit executes phase 1 on all participants and then phase 2; on error, Rollback is invoked.
func (t *SinglePhaseTransaction) Commit(ctx context.Context) error {
	// Phase 1 commit.
	if err := t.SopPhaseCommitTransaction.Phase1Commit(ctx); err != nil {
		t.Rollback(ctx)
		return err
	}
	// Call phase 1 commit of other non-SOP transactions.
	for _, ot := range t.otherTransactions {
		if err := ot.Phase1Commit(ctx); err != nil {
			t.Rollback(ctx)
			return err
		}
	}

	// Phase 2 commit.
	if err := t.SopPhaseCommitTransaction.Phase2Commit(ctx); err != nil {
		log.Debug(fmt.Sprintf("Phase2Commit error: %v", err))
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

// Rollback aborts the transaction and attempts to rollback all participants, returning the last error if any.
func (t *SinglePhaseTransaction) Rollback(ctx context.Context) error {
	t.SopPhaseCommitTransaction.Rollback(ctx, nil)
	var lastErr error
	for _, ot := range t.otherTransactions {
		if err := ot.Rollback(ctx, nil); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// GetMode returns the transaction mode.
func (t *SinglePhaseTransaction) GetMode() TransactionMode {
	return t.SopPhaseCommitTransaction.GetMode()
}

// HasBegun reports whether the transaction has started.
func (t *SinglePhaseTransaction) HasBegun() bool {
	return t.SopPhaseCommitTransaction.HasBegun()
}

// GetPhasedTransaction returns the wrapped two-phase commit transaction.
func (t *SinglePhaseTransaction) GetPhasedTransaction() TwoPhaseCommitTransaction {
	return t.SopPhaseCommitTransaction
}

// AddPhasedTransaction registers additional two-phase commit participants.
func (t *SinglePhaseTransaction) AddPhasedTransaction(otherTransaction ...TwoPhaseCommitTransaction) {
	t.otherTransactions = append(t.otherTransactions, otherTransaction...)
}

// GetStores delegates to the wrapped transaction to list available stores.
func (t *SinglePhaseTransaction) GetStores(ctx context.Context) ([]string, error) {
	return t.SopPhaseCommitTransaction.GetStores(ctx)
}

// GetID returns the transaction ID.
func (t *SinglePhaseTransaction) GetID() UUID {
	return t.SopPhaseCommitTransaction.GetID()
}
