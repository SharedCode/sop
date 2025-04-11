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
	return common.NewTwoPhaseCommitTransaction(mode, maxTime, true, fs.NewBlobStore(nil),
		fs.NewStoreRepository(storesBaseFolder, nil, cache), fs.NewRegistry(), cache, fs.NewTransactionLog())
}

// Create a transaction that supports replication, via custom SOP replicaiton on StoreRepository & Registry and then Erasure Coding on Blob Store.
func NewTransactionWithReplication(storesBaseFolders []string, mode sop.TransactionMode, maxTime time.Duration, cache sop.Cache, erasureConfig map[string]fs.ErasureCodingConfig) (sop.Transaction, error) {
	if erasureConfig == nil {
		erasureConfig = fs.GetGlobalErasureConfig()
		if erasureConfig == nil {
			return nil, fmt.Errorf("erasureConfig can't be nil")
		}
	}
	fio := fs.NewDefaultFileIO(fs.DefaultToFilePath)
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
	twoPT, err := common.NewTwoPhaseCommitTransaction(mode, maxTime, true, bs,
		fs.NewStoreRepositoryWithReplication(storesBaseFolders, mbsf, cache), fs.NewRegistry(), cache, fs.NewTransactionLog())
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, true)
}
