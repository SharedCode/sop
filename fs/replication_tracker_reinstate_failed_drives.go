package fs

import (
	"context"
	"fmt"
)

// ReinstateFailedDrives can be invoked after replacing the failed drives so they can be synchronized with
// the current Active targets' Stores' contents.
//
// Details:
//   - Tell transactions to start logging commit changes
//   - Copy the registries & storeRepositories files to the new drives
//   - Once done, switch over the entire cluster into "InDeltaSync" mode
//   - Using all the transactions' commit logs' generated while doing the 2nd step above,
//     fast forward registries & storeRepositories files
//   - Update the L2 cache copy of global replication status to turn back on, the replication
//     to the passive targets
//   - Turn off the "InDeltaSync" mode to switch over to normal mode
func (r *replicationTracker) ReinstateFailedDrives(ctx context.Context) error {
	if !r.replicate {
		return fmt.Errorf("replicationTracker.replicate flag is off, ReinstateFaileDrives is valid only if this is on")
	}
	if !r.replicationTrackedDetails.FailedToReplicate {
		return fmt.Errorf("replicationTracker.FailedToReplicate is false, ReinstateFaileDrives is valid only if this is true")
	}

	if err := r.startLoggingCommitChanges(ctx); err != nil {
		return err
	}
	if err := r.copyStores(ctx); err != nil {
		return err
	}
	if err := r.fastForward(ctx); err != nil {
		return err
	}
	if err := r.turnOnReplication(ctx); err != nil {
		return err
	}
	// Check fast forward log one last time.
	return r.fastForward(ctx)
}

func (r *replicationTracker) startLoggingCommitChanges(ctx context.Context) error {
	globalReplicationDetails.LogCommitChanges = true
	r.LogCommitChanges = true

	// Update the replication status details.
	r.writeReplicationStatus(r.formatActiveFolderEntity(replicationStatusFilename))
	return r.syncWithL2Cache(ctx, true)
}

func (r *replicationTracker) copyStores(ctx context.Context) error {
	if sr, err := NewStoreRepository(r, nil, r.l2Cache); err != nil {
		return err
	} else {
		// Copy Store Repositories & Registries to passive folders being reinstated for replication.
		return sr.CopyToPassiveFolders(ctx)
	}
}

func (r *replicationTracker) fastForward(ctx context.Context) error {
	// TODO: apply commit logs, since commit logging got started, one after another until caught up.

	// Read the transaction commit logs then sync the passive stores/registries w/ the values from active stores/regs.
	//   - In case StoreRepository exists in target, use the StoreRepository timestamp to determine if target needs to get updated.
	//     If missing in passive target then add one from active.
	//   - In case Handle exists in target, use Handle version to determine if target needs to get updated.
	//     If missing in passive target then add one from active.
	// Delete after the log file is completely processed.
	// Repeat this until all commit logs files are done processing (all deleted).

	return nil
}

func (r *replicationTracker) turnOnReplication(ctx context.Context) error {
	globalReplicationDetails.FailedToReplicate = false
	globalReplicationDetails.ActiveFolderToggler = !globalReplicationDetails.ActiveFolderToggler

	r.replicationTrackedDetails = *globalReplicationDetails
	// Update the replication status details.
	r.writeReplicationStatus(r.formatActiveFolderEntity(replicationStatusFilename))
	return r.syncWithL2Cache(ctx, true)
}
