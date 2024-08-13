// Package contains SOP in Redis, Cassandra & Kafka(in_red_c) integration code.
package sop

import (
	"context"
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

// Transaction modes enumeration.
type TransactionMode int
const(
	// No check does not allow any change to the Btree stores and does not check
	// read items' versions (for consistency) during commit.
	NoCheck = iota
	// For writing mode allows changes to be done to the Btree stores.
	ForWriting
	// For reading mode does not allow any change to the Btree stores.
	ForReading
)
