package in_red_fs

import (
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/common"
	"github.com/SharedCode/sop/fs"
	"github.com/SharedCode/sop/redis"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(to TransationOptions) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransaction(to)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(to.Mode, twoPT, to.MaxTime, true)
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
func NewTwoPhaseCommitTransaction(to TransationOptions) (sop.TwoPhaseCommitTransaction, error) {
	if !IsInitialized() {
		return nil, fmt.Errorf("Redis was not initialized")
	}
	if to.Cache == nil {
		to.Cache = redis.NewClient()
	}
	fio := fs.NewDefaultFileIO(fs.DefaultToFilePath)
	replicationTracker := fs.NewReplicationTracker([]string{to.StoresBaseFolder}, false)
	mbsf := fs.NewManageStoreFolder(fio)
	sr, err := fs.NewStoreRepository(replicationTracker, mbsf, to.Cache)
	if err != nil {
		return nil, err
	}
	tl := fs.NewTransactionLog(to.Cache, replicationTracker)
	return common.NewTwoPhaseCommitTransaction(to.Mode, to.MaxTime, true,
		fs.NewBlobStore(nil), sr, fs.NewRegistry(to.Mode == sop.ForWriting,
			to.RegistryHashModValue, replicationTracker, to.Cache, to.UseCacheForFileRegionLocks), to.Cache, tl)
} 

// Create a transaction that supports replication, via custom SOP replicaiton on StoreRepository & Registry and then Erasure Coding on Blob Store.
func NewTransactionWithReplication(towr TransationOptionsWithReplication) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransactionWithReplication(towr)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(towr.Mode, twoPT, towr.MaxTime, true)
}

// Create a transaction that supports replication, via custom SOP replicaiton on StoreRepository & Registry and then Erasure Coding on Blob Store.
// Returns sop.TwoPhaseCommitTransaction type useful for integration with your custom application transaction where code would like to get access to SOP's two phase commit transaction API.
func NewTwoPhaseCommitTransactionWithReplication(towr TransationOptionsWithReplication) (sop.TwoPhaseCommitTransaction, error) {
	if towr.ErasureConfig == nil {
		towr.ErasureConfig = fs.GetGlobalErasureConfig()
		if towr.ErasureConfig == nil {
			return nil, fmt.Errorf("erasureConfig can't be nil")
		}
	}
	fio := fs.NewDefaultFileIO(fs.DefaultToFilePath)
	replicationTracker := fs.NewReplicationTracker(towr.StoresBaseFolders, true)
	bs, err := fs.NewBlobStoreWithEC(fio, towr.ErasureConfig)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)
	if towr.Cache == nil {
		towr.Cache = redis.NewClient()
	}

	if !IsInitialized() {
		return nil, fmt.Errorf("Redis was not initialized")
	}
	sr, err := fs.NewStoreRepository(replicationTracker, mbsf, towr.Cache)
	if err != nil {
		return nil, err
	}

	tl := fs.NewTransactionLog(towr.Cache, replicationTracker)

	return common.NewTwoPhaseCommitTransaction(towr.Mode, towr.MaxTime, true, bs, sr,
		fs.NewRegistry(towr.Mode == sop.ForWriting, towr.RegistryHashModValue, replicationTracker, towr.Cache, towr.UseCacheForFileRegionLocks), towr.Cache, tl)
}
