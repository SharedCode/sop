package in_red_fs

import (
	"fmt"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/common"
	repo "github.com/SharedCode/sop/in_red_fs/repositories"
	"github.com/SharedCode/sop/redis"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransaction(mode, maxTime, logging)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
// If logging is on, 'will log changes so it can get rolledback if transaction got left unfinished, e.g. crash or power reboot.
// However, without logging, the transaction commit can execute faster because there is no data getting logged.
func NewTwoPhaseCommitTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.TwoPhaseCommitTransaction, error) {
	if !IsInitialized() {
		return nil, fmt.Errorf("Redis was not initialized")
	}
	return common.NewTwoPhaseCommitTransaction(mode, maxTime, logging, repo.NewBlobStore(), repo.NewStoreRepository(), repo.NewRegistry(), redis.NewClient(), repo.NewTransactionLog())
}
