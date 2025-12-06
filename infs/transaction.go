package infs

import (
	"context"
	"fmt"
	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/fs"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(ctx context.Context, config sop.TransactionOptions) (sop.Transaction, error) {
	log.Debug("NewTransaction called")
	twoPT, err := NewTwoPhaseCommitTransaction(ctx, config)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(config.Mode, twoPT, true)
}

// NewTwoPhaseCommitTransaction will instantiate a transaction object for writing(forWriting=true)
// or for reading(forWriting=false). Pass in -1 on maxTime to default to 15 minutes of max "commit" duration.
//
// Timeout semantics: the commit ends when the earlier of ctx deadline or maxTime is reached.
// Locks use maxTime as TTL so they are bounded even if ctx is canceled. If you want replication/log
// cleanup to finish under the same budget, set ctx.Deadline to at least maxTime plus a small grace period.
func NewTwoPhaseCommitTransaction(ctx context.Context, config sop.TransactionOptions) (sop.TwoPhaseCommitTransaction, error) {
	log.Debug("NewTwoPhaseCommitTransaction called")
	cache := sop.NewCacheClientByType(config.CacheType)
	if cache == nil {
		cache = sop.NewCacheClient()
	}
	fio := fs.NewFileIO()
	var folder string
	if len(config.StoresFolders) > 0 {
		folder = config.StoresFolders[0]
	}
	replicationTracker, err := fs.NewReplicationTracker(ctx, []string{folder}, false, cache)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)
	sr, err := fs.NewStoreRepository(ctx, replicationTracker, mbsf, cache, config.RegistryHashModValue)
	if err != nil {
		return nil, err
	}

	// Override with the read registry hash mod value (if there is).
	if i, err := sr.GetRegistryHashModValue(ctx); err != nil {
		return nil, err
	} else if i > 0 {
		config.RegistryHashModValue = i
	}

	tl := fs.NewTransactionLog(cache, replicationTracker)
	t, err := common.NewTwoPhaseCommitTransaction(config.Mode, config.MaxTime, true,
		fs.NewBlobStore(fs.DefaultToFilePath, nil), sr, fs.NewRegistry(config.Mode == sop.ForWriting,
			config.RegistryHashModValue, replicationTracker, cache), cache, tl)

	// Tell Replication Tracker to use transaction ID as ID when locking registry handle record's file sector during writes.
	replicationTracker.SetTransactionID(t.GetID())
	return t, err
}

// NewTransactionWithReplication creates a transaction that supports replication via SOP's replication on StoreRepository & Registry,
// and Erasure Coding on the Blob Store.
func NewTransactionWithReplication(ctx context.Context, config sop.TransactionOptions) (sop.Transaction, error) {
	log.Debug("NewTransactionWithReplication called")
	twoPT, err := NewTwoPhaseCommitTransactionWithReplication(ctx, config)
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(config.Mode, twoPT, true)
}

// NewTwoPhaseCommitTransactionWithReplication creates a two-phase commit transaction with replication enabled.
//
// Timeout semantics: see NewTwoPhaseCommitTransaction for guidance on ctx.Deadline vs maxTime and lock TTLs.
// Returns sop.TwoPhaseCommitTransaction to allow integration with custom application transactions requiring direct access to SOP's API.
func NewTwoPhaseCommitTransactionWithReplication(ctx context.Context, config sop.TransactionOptions) (sop.TwoPhaseCommitTransaction, error) {
	log.Debug("NewTwoPhaseCommitTransactionWithReplication called")
	if len(config.StoresFolders) == 0 {
		return nil, fmt.Errorf("config.StoresFolders can't be empty")
	}
	if config.ErasureConfig == nil {
		config.ErasureConfig = fs.GetGlobalErasureConfig()
		if config.ErasureConfig == nil {
			return nil, fmt.Errorf("config.ErasureConfig can't be nil")
		}
	}
	fio := fs.NewFileIO()
	cache := sop.NewCacheClientByType(config.CacheType)
	if cache == nil {
		cache = sop.NewCacheClient()
	}
	replicationTracker, err := fs.NewReplicationTracker(ctx, config.StoresFolders, true, cache)
	if err != nil {
		return nil, err
	}
	bs, err := fs.NewBlobStoreWithEC(fs.DefaultToFilePath, nil, config.ErasureConfig)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)
	sr, err := fs.NewStoreRepository(ctx, replicationTracker, mbsf, cache, config.RegistryHashModValue)
	if err != nil {
		return nil, err
	}

	// Override with the read registry hash mod value (if there is).
	if i, err := sr.GetRegistryHashModValue(ctx); err != nil {
		return nil, err
	} else if i > 0 {
		config.RegistryHashModValue = i
	}

	tl := fs.NewTransactionLog(cache, replicationTracker)

	t, err := common.NewTwoPhaseCommitTransaction(config.Mode, config.MaxTime, true, bs, sr,
		fs.NewRegistry(config.Mode == sop.ForWriting, config.RegistryHashModValue, replicationTracker, cache), cache, tl)
	t.HandleReplicationRelatedError = replicationTracker.HandleReplicationRelatedError

	// Tell Replication Tracker to use transaction ID as ID when locking registry handle record's file sector during writes.
	replicationTracker.SetTransactionID(t.GetID())
	return t, err
}
