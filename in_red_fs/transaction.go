package in_red_fs

import (
	"fmt"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/common"
	"github.com/SharedCode/sop/fs"
	"github.com/SharedCode/sop/redis"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(storesBaseFolder string, mode sop.TransactionMode, maxTime time.Duration) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransaction(storesBaseFolder, mode, maxTime, nil)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, true)
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
// If logging is on, 'will log changes so it can get rolledback if transaction got left unfinished, e.g. crash or power reboot.
// However, without logging, the transaction commit can execute faster because there is no data getting logged.
func NewTwoPhaseCommitTransaction(storesBaseFolder string, mode sop.TransactionMode, maxTime time.Duration, cache sop.Cache) (sop.TwoPhaseCommitTransaction, error) {
	if !IsInitialized() {
		return nil, fmt.Errorf("Redis was not initialized")
	}
	if cache == nil {
		cache = redis.NewClient()
	}
	replicationTracker := fs.NewReplicationTracker([]string{storesBaseFolder}, false)
	sr, err := fs.NewStoreRepository(replicationTracker, nil, cache)
	if err != nil {
		return nil, err
	}
	return common.NewTwoPhaseCommitTransaction(mode, maxTime, true, fs.NewBlobStore(nil), sr, fs.NewRegistry(replicationTracker), cache, fs.NewTransactionLog())
}

// Create a transaction that supports replication, via custom SOP replicaiton on StoreRepository & Registry and then Erasure Coding on Blob Store.
func NewTransactionWithReplication(storesBaseFolders []string, mode sop.TransactionMode, maxTime time.Duration, cache sop.Cache, erasureConfig map[string]fs.ErasureCodingConfig) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransactionWithReplication(storesBaseFolders, mode, maxTime, cache, erasureConfig)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, true)
}

// Create a transaction that supports replication, via custom SOP replicaiton on StoreRepository & Registry and then Erasure Coding on Blob Store.
// Returns sop.TwoPhaseCommitTransaction type useful for integration with your custom application transaction where code would like to get access to SOP's two phase commit transaction API.
func NewTwoPhaseCommitTransactionWithReplication(storesBaseFolders []string, mode sop.TransactionMode, maxTime time.Duration, cache sop.Cache, erasureConfig map[string]fs.ErasureCodingConfig) (sop.TwoPhaseCommitTransaction, error) {
	if erasureConfig == nil {
		erasureConfig = fs.GetGlobalErasureConfig()
		if erasureConfig == nil {
			return nil, fmt.Errorf("erasureConfig can't be nil")
		}
	}
	fio := fs.NewDefaultFileIO(fs.DefaultToFilePath)
	replicationTracker := fs.NewReplicationTracker(storesBaseFolders, true)
	bs, err := fs.NewBlobStoreWithEC(fio, erasureConfig)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)
	if cache == nil {
		cache = redis.NewClient()
	}

	if !IsInitialized() {
		return nil, fmt.Errorf("Redis was not initialized")
	}
	sr, err := fs.NewStoreRepository(replicationTracker, mbsf, cache)
	if err != nil {
		return nil, err
	}
	return common.NewTwoPhaseCommitTransaction(mode, maxTime, true, bs, sr, fs.NewRegistry(replicationTracker), cache, fs.NewTransactionLog())
}
