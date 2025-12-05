package inredck

import (
	"fmt"
	"time"

	"github.com/sharedcode/sop"
	cas "github.com/sharedcode/sop/adapters/cassandra"
	"github.com/sharedcode/sop/common"
)

// NewTransaction is a convenience function to create an end-user facing transaction object that wraps the two-phase commit transaction.
func NewTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransaction(mode, maxTime, logging, cas.NewBlobStore(), cas.NewStoreRepository(nil))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, logging)
}

// NewTwoPhaseCommitTransaction instantiates a transaction for the given mode.
// Pass -1 for maxTime to default to 15 minutes. If logging is on, changes are logged for recovery at the cost of performance.
func NewTwoPhaseCommitTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool,
	blobStore sop.BlobStore, storeRepository sop.StoreRepository) (sop.TwoPhaseCommitTransaction, error) {
	if !IsInitialized() {
		return nil, fmt.Errorf("redis and/or cassandra bits were not initialized")
	}
	return common.NewTwoPhaseCommitTransaction(mode, maxTime, logging, blobStore, storeRepository, cas.NewRegistry(), sop.NewCacheClient(), cas.NewTransactionLog())
}
