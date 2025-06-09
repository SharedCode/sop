package fs

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

// ReinstateFailedDrives can be invoked after replacing the failed drives so they can be synchronized with
// the current Active targets' Stores' contents.
//
// Details:
//   - Tell transactions to start logging commit changes
//   - Copy the registries & storeRepositories files to the new drives
//   - Using all the transactions' commit logs' generated while doing the 2nd step above,
//     fast forward registries & storeRepositories files
//   - Update the L2 cache copy of global replication status to turn back on, the replication
//     to the passive targets
//   - Run Fast Forward one more time to ensure there are no "remnants" commit log file(s), race condition case
func (r *replicationTracker) ReinstateFailedDrives(ctx context.Context, registryHashModValue int) error {
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
	if err := r.fastForward(ctx, registryHashModValue); err != nil {
		return err
	}
	if err := r.turnOnReplication(ctx); err != nil {
		return err
	}
	// Check fast forward log one last time.
	return r.fastForward(ctx, registryHashModValue)
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

func (r *replicationTracker) fastForward(ctx context.Context, registryHashModValue int) error {
	// Read the transaction commit logs then sync the passive stores/registries w/ the values from active stores/regs.
	//   - In case StoreRepository exists in target, use the StoreRepository timestamp to determine if target needs to get updated.
	//     If missing in passive target then add one from active.
	//   - In case Handle exists in target, use Handle version to determine if target needs to get updated.
	//     If missing in passive target then add one from active.
	// Delete after the log file is completely processed.
	// Repeat this until all commit logs files are done processing (all deleted).

	fn := r.formatActiveFolderEntity(commitChangesLogFolder)
	files, err := getFilesSortedByModifiedTime(fn)
	if err != nil {
		return err
	}

	fio := NewDefaultFileIO()
	ms := NewManageStoreFolder(fio)

	// Set to false the FailedToReplicate so we can issue a successful Replicate call on StoreRepository & Registry.
	r.replicationTrackedDetails.FailedToReplicate = false
	sr, err := NewStoreRepository(r, ms, r.l2Cache)
	if err != nil {
		return err
	}
	reg := NewRegistry(true, registryHashModValue, r, r.l2Cache)
	// Get the oldest first.
	for i := 0; i < len(files); i++ {
		filename := files[i].Name()

		ba, err := fio.ReadFile(filename)
		if err != nil {
			return err
		}

		var logData sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]

		err = encoding.DefaultMarshaler.Unmarshal(ba, &logData)
		if err != nil {
			return err
		}

		sr.Replicate(ctx, logData.First)
		reg.Replicate(ctx, logData.Second[0], logData.Second[1], logData.Second[2], logData.Second[3])

		if err := fio.Remove(filename); err != nil {
			return err
		}
	}

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
