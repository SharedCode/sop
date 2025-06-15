package fs

import (
	"context"
	"fmt"
	"os"

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
		return fmt.Errorf("replicationTracker.replicate flag is off, ReinstateFailedDrives is valid only if this is on")
	}
	if !r.replicationTrackedDetails.FailedToReplicate {
		return fmt.Errorf("replicationTracker.FailedToReplicate is false, ReinstateFailedDrives is valid only if this is true")
	}

	if err := r.startLoggingCommitChanges(ctx); err != nil {
		return err
	}
	if err := r.copyStores(ctx); err != nil {
		return err
	}
	// Consume all Commit logs.
	for {
		if fileFound, err := r.fastForward(ctx, registryHashModValue); err != nil {
			return err
		} else if !fileFound {
			break
		}
	}
	// Turn on Replication.
	if err := r.turnOnReplication(ctx); err != nil {
		return err
	}
	// Check fast forward log one last time.
	for {
		if fileFound, err := r.fastForward(ctx, registryHashModValue); err != nil {
			return err
		} else if !fileFound {
			return nil
		}
	}
}

func (r *replicationTracker) startLoggingCommitChanges(ctx context.Context) error {
	globalReplicationDetails.LogCommitChanges = true
	r.LogCommitChanges = true

	// Update the replication status details.
	if err := r.writeReplicationStatus(ctx, r.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
		return err
	}
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

func (r *replicationTracker) fastForward(ctx context.Context, registryHashModValue int) (bool, error) {
	// Read the transaction commit logs then sync the passive stores/registries w/ the values from active stores/regs.
	//   - In case StoreRepository exists in target, use the StoreRepository timestamp to determine if target needs to get updated.
	//     If missing in passive target then add one from active.
	//   - In case Handle exists in target, use Handle version to determine if target needs to get updated.
	//     If missing in passive target then add one from active.
	// Delete after the log file is completely processed.
	// Repeat this until all commit logs files are done processing (all deleted).

	fn := r.formatActiveFolderEntity(commitChangesLogFolder)

	files, err := getFilesSortedDescByModifiedTime(ctx, fn, logFileExtension, nil)
	if err != nil {
		return false, err
	}
	if len(files) == 0 {
		return false, nil
	}

	// Set to false the FailedToReplicate so we can issue a successful Replicate call on StoreRepository & Registry.
	r.replicationTrackedDetails.FailedToReplicate = false
	fio := NewFileIO()
	ms := NewManageStoreFolder(fio)
	sr, err := NewStoreRepository(r, ms, r.l2Cache)
	if err != nil {
		return false, err
	}
	reg := NewRegistry(true, registryHashModValue, r, r.l2Cache)
	var foundAndProcessed bool
	// Get the oldest first.
	for i := range files {
		filename := files[i].Name()
		foundAndProcessed = true

		ffn := r.formatActiveFolderEntity(fmt.Sprintf("%s%c%s", commitChangesLogFolder, os.PathSeparator, filename))
		ba, err := fio.ReadFile(ctx, ffn)
		if err != nil {
			return false, err
		}

		var logData sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]

		err = encoding.DefaultMarshaler.Unmarshal(ba, &logData)
		if err != nil {
			return false, err
		}

		if logData.First != nil {
			// Ensure Store Repo has the latest Count so we don't need to worry about potential race condition
			// between transaction doing commit logs vs. replication to the reinstated drives.
			for i := range logData.First {
				if sis, _ := sr.getFromCache(ctx, logData.First[i].Name); len(sis) == 1 {
					logData.First[i].Count = sis[0].Count
				}
			}
			if err := sr.Replicate(ctx, logData.First); err != nil {
				return false, err
			}
		}
		if err := reg.Replicate(ctx, logData.Second[0], logData.Second[1], logData.Second[2], logData.Second[3]); err != nil {
			return false, err
		}

		if err := fio.Remove(ctx, ffn); err != nil {
			return false, err
		}
	}

	return foundAndProcessed, nil
}

func (r *replicationTracker) turnOnReplication(ctx context.Context) error {
	globalReplicationDetails.FailedToReplicate = false
	globalReplicationDetails.LogCommitChanges = false

	r.replicationTrackedDetails = *globalReplicationDetails
	// Update the replication status details.
	if err := r.writeReplicationStatus(ctx, r.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
		return err
	}
	return r.syncWithL2Cache(ctx, true)
}
