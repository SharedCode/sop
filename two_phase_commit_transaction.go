package sop

import (
	"context"
)

// TwoPhaseCommitTransaction interface defines the "infrastructure facing" transaction methods.
type TwoPhaseCommitTransaction interface {
	// Begin the transaction.
	Begin() error
	// Phase1Commit of the transaction.
	Phase1Commit(ctx context.Context) error
	// Phase2Commit of the transaction.
	Phase2Commit(ctx context.Context) error
	// Rollback the transaction.
	Rollback(ctx context.Context) error
	// Returns true if transaction has begun, false otherwise.
	HasBegun() bool
}
