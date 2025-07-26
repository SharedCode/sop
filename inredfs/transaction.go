package inredfs

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/redis"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(ctx context.Context, to TransationOptions) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransaction(ctx, to)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(to.Mode, twoPT, to.MaxTime, true)
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
func NewTwoPhaseCommitTransaction(ctx context.Context, to TransationOptions) (sop.TwoPhaseCommitTransaction, error) {
	if !IsInitialized() {
		return nil, fmt.Errorf("Redis was not initialized")
	}
	if to.Cache == nil {
		to.Cache = redis.NewClient()
	}
	fio := fs.NewFileIO()
	replicationTracker, err := fs.NewReplicationTracker(ctx, []string{to.StoresBaseFolder}, false, to.Cache)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)
	sr, err := fs.NewStoreRepository(ctx, replicationTracker, mbsf, to.Cache, to.RegistryHashModValue)
	if err != nil {
		return nil, err
	}

	// Override with the read registry hash mod value (if there is).
	if i, err := sr.GetRegistryHashModValue(ctx); err != nil {
		return nil, err
	} else if i > 0 {
		to.RegistryHashModValue = i
	}

	tl := fs.NewTransactionLog(to.Cache, replicationTracker)
	t, err := common.NewTwoPhaseCommitTransaction(to.Mode, to.MaxTime, true,
		fs.NewBlobStore(fs.DefaultToFilePath, nil), sr, fs.NewRegistry(to.Mode == sop.ForWriting,
			to.RegistryHashModValue, replicationTracker, to.Cache), to.Cache, tl)

	// Tell Replication Tracker to use transaction ID as ID when locking registry handle record's file sector during writes.
	replicationTracker.SetTransactionID(t.GetID())
	return t, err
}

// Create a transaction that supports replication, via custom SOP replicaiton on StoreRepository & Registry and then Erasure Coding on Blob Store.
func NewTransactionWithReplication(ctx context.Context, towr TransationOptionsWithReplication) (sop.Transaction, error) {
	twoPT, err := NewTwoPhaseCommitTransactionWithReplication(ctx, towr)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(towr.Mode, twoPT, towr.MaxTime, true)
}

// Create a transaction that supports replication, via custom SOP replicaiton on StoreRepository & Registry and then Erasure Coding on Blob Store.
// Returns sop.TwoPhaseCommitTransaction type useful for integration with your custom application transaction where code would like to get access to SOP's two phase commit transaction API.
func NewTwoPhaseCommitTransactionWithReplication(ctx context.Context, towr TransationOptionsWithReplication) (sop.TwoPhaseCommitTransaction, error) {
	if towr.IsEmpty() {
		return nil, fmt.Errorf("towr can't be empty")
	}
	if towr.ErasureConfig == nil {
		towr.ErasureConfig = fs.GetGlobalErasureConfig()
		if towr.ErasureConfig == nil {
			return nil, fmt.Errorf("towr.ErasureConfig can't be nil")
		}
	}
	fio := fs.NewFileIO()
	replicationTracker, err := fs.NewReplicationTracker(ctx, towr.StoresBaseFolders, true, towr.Cache)
	if err != nil {
		return nil, err
	}
	bs, err := fs.NewBlobStoreWithEC(fs.DefaultToFilePath, fio, towr.ErasureConfig)
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
	sr, err := fs.NewStoreRepository(ctx, replicationTracker, mbsf, towr.Cache, towr.RegistryHashModValue)
	if err != nil {
		return nil, err
	}

	// Override with the read registry hash mod value (if there is).
	// Override with the read registry hash mod value (if there is).
	if i, err := sr.GetRegistryHashModValue(ctx); err != nil {
		return nil, err
	} else if i > 0 {
		towr.RegistryHashModValue = i
	}

	tl := fs.NewTransactionLog(towr.Cache, replicationTracker)

	t, err := common.NewTwoPhaseCommitTransaction(towr.Mode, towr.MaxTime, true, bs, sr,
		fs.NewRegistry(towr.Mode == sop.ForWriting, towr.RegistryHashModValue, replicationTracker, towr.Cache), towr.Cache, tl)
	t.HandleReplicationRelatedError = replicationTracker.HandleReplicationRelatedError

	// Tell Replication Tracker to use transaction ID as ID when locking registry handle record's file sector during writes.
	replicationTracker.SetTransactionID(t.GetID())
	return t, err
}
