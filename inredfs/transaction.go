package inredfs

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/fs"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(ctx context.Context, to TransationOptions) (sop.Transaction, error) {
	log.Debug("NewTransaction called")
	twoPT, err := NewTwoPhaseCommitTransaction(ctx, to)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(to.Mode, twoPT, true)
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
//
// Timeout semantics: the commit ends when the earlier of ctx deadline or maxTime is reached.
// Locks use maxTime as TTL so they are bounded even if ctx is canceled. If you want replication/log
// cleanup to finish under the same budget, set ctx.Deadline to at least maxTime plus a small grace period.
func NewTwoPhaseCommitTransaction(ctx context.Context, to TransationOptions) (sop.TwoPhaseCommitTransaction, error) {
	log.Debug("NewTwoPhaseCommitTransaction called")
	if to.Cache == nil {
		to.Cache = sop.NewCacheClient()
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

// NewTransactionWithReplication creates a transaction that supports replication via SOP's replication on StoreRepository & Registry,
// and Erasure Coding on the Blob Store.
func NewTransactionWithReplication(ctx context.Context, towr TransationOptionsWithReplication) (sop.Transaction, error) {
	log.Debug("NewTransactionWithReplication called")
	twoPT, err := NewTwoPhaseCommitTransactionWithReplication(ctx, towr)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(towr.Mode, twoPT, true)
}

// NewTwoPhaseCommitTransactionWithReplication creates a two-phase commit transaction with replication enabled.
//
// Timeout semantics: see NewTwoPhaseCommitTransaction for guidance on ctx.Deadline vs maxTime and lock TTLs.
// Returns sop.TwoPhaseCommitTransaction to allow integration with custom application transactions requiring direct access to SOP's API.
func NewTwoPhaseCommitTransactionWithReplication(ctx context.Context, towr TransationOptionsWithReplication) (sop.TwoPhaseCommitTransaction, error) {
	log.Debug("NewTwoPhaseCommitTransactionWithReplication called")
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
	bs, err := fs.NewBlobStoreWithEC(fs.DefaultToFilePath, nil, towr.ErasureConfig)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)
	if towr.Cache == nil {
		towr.Cache = sop.NewCacheClient()
	}
	sr, err := fs.NewStoreRepository(ctx, replicationTracker, mbsf, towr.Cache, towr.RegistryHashModValue)
	if err != nil {
		return nil, err
	}

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
