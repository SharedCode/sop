package fs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/redis"
)

type ReplicationTrackedDetails struct {
	FailedToReplicate bool
	// If true, folder as specified in storesBaseFolders[0] will be the active folder,
	// otherwise the 2nd folder, as specified in storesBaseFolders[1].
	ActiveFolderToggler bool

	// If true, Transactions should log commit changes so they can be used for "fast forward" functionality
	// of ReinstateFailedDrives functionality.
	LogCommitChanges bool
}

func (a ReplicationTrackedDetails) isEqual(b ReplicationTrackedDetails) bool {
	return a.ActiveFolderToggler == b.ActiveFolderToggler && a.FailedToReplicate == b.FailedToReplicate
}

type replicationTracker struct {
	ReplicationTrackedDetails
	// Array so we can use in replication across two folders, if in replication mode.
	storesBaseFolders []string
	replicate         bool
	l2Cache           sop.Cache
	tid               sop.UUID
}

const (
	replicationStatusFilename         = "replstat.txt"
	replicationStatusCacheKey         = "Rreplstat"
	replicationStatusCacheTTLDuration = 5 * time.Minute
	commitChangesLogFolder            = "commitlogs"
)

var GlobalReplicationDetails *ReplicationTrackedDetails
var globalReplicationDetailsLocker sync.Mutex = sync.Mutex{}

// Instantiates a replication tracker.
func NewReplicationTracker(ctx context.Context, storesBaseFolders []string, replicate bool, l2Cache sop.Cache) (*replicationTracker, error) {
	if l2Cache == nil {
		l2Cache = redis.NewClient()
	}
	isFirstFolderActive := true
	rt := replicationTracker{
		storesBaseFolders: storesBaseFolders,
		replicate:         replicate,
		l2Cache:           l2Cache,
	}
	rt.ActiveFolderToggler = isFirstFolderActive
	if replicate {
		if err := rt.syncWithL2Cache(ctx, false); err != nil {
			log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
		}
		// Minimize reading the replication "status" if we have read it and is tracking it globally.
		if GlobalReplicationDetails != nil {
			globalReplicationDetailsLocker.Lock()
			rt.ReplicationTrackedDetails = *GlobalReplicationDetails
			globalReplicationDetailsLocker.Unlock()
		} else {
			if err := rt.readStatusFromHomeFolder(ctx); err != nil {
				return nil, fmt.Errorf("failed reading replication status (%sº file, details: %v", replicationStatusFilename, err)
			}
			globalReplicationDetailsLocker.Lock()
			copy := rt.ReplicationTrackedDetails
			GlobalReplicationDetails = &copy
			// Sync l2 cache.
			if err := rt.syncWithL2Cache(ctx, true); err != nil {
				log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
			}

			globalReplicationDetailsLocker.Unlock()
		}
	}
	return &rt, nil
}

func (r *replicationTracker) SetTransactionID(tid sop.UUID) {
	r.tid = tid
}

// Handle replication related error is invoked from a transaction when an IO error is encountered.
// This function should handle the act of failing over to the passive destinations making them as active and the active to be passive.
func (r *replicationTracker) HandleReplicationRelatedError(ctx context.Context, ioError error, rollbackError error, rollbackSucceeded bool) {
	if !r.replicate {
		return
	}
	rootErr := errors.Unwrap(ioError)
	err1, ok1 := rootErr.(sop.Error)
	err2, ok2 := ioError.(sop.Error)
	if ok2 {
		err1 = err2
	}
	if ok1 || ok2 {
		log.Error(fmt.Sprintf("a replication related error detected (rollback: %v), details: %v", rollbackSucceeded, err1))

		failoverError := err1.Code == sop.RestoreRegistryFileSectorFailure || err2.Code == sop.RestoreRegistryFileSectorFailure

		// Generally, no need to failover if rollback succeeded. It means that the IO error itself is temporary.
		// BUT if error is known to need failover then it should get failover.
		if !failoverError && rollbackSucceeded {
			return
		}
		if err1.Code >= sop.FailoverQualifiedError || err2.Code >= sop.FailoverQualifiedError {
			// Cause a failover switch to passive destinations on succeeding transactions.
			if err := r.failover(ctx); err != nil {
				log.Error(fmt.Sprintf("failover to folder %s failed, details: %v", r.getPassiveBaseFolder(), err.Error()))
			}
		}
	}
}

func (r *replicationTracker) handleFailedToReplicate(ctx context.Context) {
	if !r.replicate || r.FailedToReplicate {
		return
	}

	if err := r.syncWithL2Cache(ctx, false); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	// L2 cache "knows" of failure, just return.
	if GlobalReplicationDetails.FailedToReplicate {
		r.FailedToReplicate = true
		return
	}

	globalReplicationDetailsLocker.Lock()

	if r.FailedToReplicate {
		globalReplicationDetailsLocker.Unlock()
		return
	}

	r.FailedToReplicate = true
	GlobalReplicationDetails.FailedToReplicate = true
	if err := r.writeReplicationStatus(ctx, r.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
		log.Warn(fmt.Sprintf("handleFailedToReplicate writeReplicationStatus failed, details: %v", err))
	}

	// Sync l2 cache.
	if err := r.syncWithL2Cache(ctx, true); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	globalReplicationDetailsLocker.Unlock()
}

func (r *replicationTracker) failover(ctx context.Context) error {
	if GlobalReplicationDetails.ActiveFolderToggler == !r.ActiveFolderToggler ||
		r.FailedToReplicate {
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	if err := r.syncWithL2Cache(ctx, false); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	if GlobalReplicationDetails.ActiveFolderToggler == !r.ActiveFolderToggler {
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	globalReplicationDetailsLocker.Lock()
	if GlobalReplicationDetails.ActiveFolderToggler == !r.ActiveFolderToggler {
		globalReplicationDetailsLocker.Unlock()
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	// Set to failed to replicate because when we flip passive to active, then yes, we should not
	// replicate on the previously active drive because it failed.
	r.FailedToReplicate = true

	if err := r.writeReplicationStatus(ctx, r.formatPassiveFolderEntity(replicationStatusFilename)); err != nil {
		globalReplicationDetailsLocker.Unlock()
		return err
	}

	// Switch the passive into active & vice versa.
	r.ActiveFolderToggler = !r.ActiveFolderToggler
	copy := r.ReplicationTrackedDetails
	GlobalReplicationDetails = &copy

	// Sync l2 cache.
	if err := r.syncWithL2Cache(ctx, true); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	globalReplicationDetailsLocker.Unlock()

	log.Info(fmt.Sprintf("failover event occurred, newly active folder is, %s", r.getActiveBaseFolder()))
	return nil
}

func (r *replicationTracker) logCommitChanges(ctx context.Context, tid sop.UUID, stores []sop.StoreInfo, newRootNodesHandles, addedNodesHandles, updatedNodesHandles, removedNodesHandles []sop.RegistryPayload[sop.Handle]) error {
	if !r.LogCommitChanges {
		return nil
	}
	fn := r.formatActiveFolderEntity(fmt.Sprintf("%s%c%s%s", commitChangesLogFolder, os.PathSeparator, tid.String(), logFileExtension))

	fio := NewFileIO()

	// Payload conversion has to happen here so the payload unpack and operation on it can be done on this file as well.
	payload, err := encoding.DefaultMarshaler.Marshal(sop.Tuple[[]sop.StoreInfo, [][]sop.RegistryPayload[sop.Handle]]{
		First: stores,
		Second: [][]sop.RegistryPayload[sop.Handle]{
			newRootNodesHandles, addedNodesHandles,
			updatedNodesHandles, removedNodesHandles,
		},
	})
	if err != nil {
		return err
	}

	return fio.WriteFile(ctx, fn, payload, permission)
}

func (r *replicationTracker) getActiveBaseFolder() string {
	if r.ActiveFolderToggler {
		return r.storesBaseFolders[0]
	}
	return r.storesBaseFolders[1]
}
func (r *replicationTracker) getPassiveBaseFolder() string {
	if r.ActiveFolderToggler {
		return r.storesBaseFolders[1]
	}
	return r.storesBaseFolders[0]
}

func (r *replicationTracker) formatActiveFolderEntity(entityName string) string {
	bf := r.getActiveBaseFolder()

	if strings.HasSuffix(bf, string(os.PathSeparator)) {
		return fmt.Sprintf("%s%s", bf, entityName)
	} else {
		return fmt.Sprintf("%s%c%s", bf, os.PathSeparator, entityName)
	}
}

func (r *replicationTracker) formatPassiveFolderEntity(entityName string) string {
	bf := r.getPassiveBaseFolder()

	if strings.HasSuffix(bf, string(os.PathSeparator)) {
		return fmt.Sprintf("%s%s", bf, entityName)
	} else {
		return fmt.Sprintf("%s%c%s", bf, os.PathSeparator, entityName)
	}
}

func (r *replicationTracker) readStatusFromHomeFolder(ctx context.Context) error {
	fio := NewFileIO()
	// Detect the active folder based on time stamp of the file.
	if !fio.Exists(ctx, r.formatActiveFolderEntity(replicationStatusFilename)) {
		if fio.Exists(ctx, r.formatPassiveFolderEntity(replicationStatusFilename)) {
			if err := r.readReplicationStatus(ctx, r.formatPassiveFolderEntity(replicationStatusFilename)); err == nil {
				// Switch passive to active if we are able to read the delta sync status file.
				r.ActiveFolderToggler = !r.ActiveFolderToggler
			}
		}
		// No Replication status file in both active & passive folders, we're good with default.
		return nil
	}
	stat, err := os.Stat(r.formatActiveFolderEntity(replicationStatusFilename))
	if err != nil {
		_, err = os.Stat(r.formatPassiveFolderEntity(replicationStatusFilename))
		if err != nil {
			return err
		}
		r.ActiveFolderToggler = !r.ActiveFolderToggler
	} else {
		stat2, err := os.Stat(r.formatPassiveFolderEntity(replicationStatusFilename))
		if err == nil {
			if stat2.ModTime().After(stat.ModTime()) {
				r.ActiveFolderToggler = !r.ActiveFolderToggler
			}
		}
	}

	return r.readReplicationStatus(ctx, r.formatActiveFolderEntity(replicationStatusFilename))
}

func (r *replicationTracker) writeReplicationStatus(ctx context.Context, filename string) error {
	fio := NewFileIO()
	ba, _ := encoding.DefaultMarshaler.Marshal(r.ReplicationTrackedDetails)
	if err := fio.WriteFile(ctx, filename, ba, permission); err != nil {
		return err
	}
	return nil
}

func (r *replicationTracker) readReplicationStatus(ctx context.Context, filename string) error {
	fio := NewFileIO()
	// Read the delta sync status.
	ba, err := fio.ReadFile(ctx, filename)
	if err != nil {
		return err
	}
	if err = encoding.DefaultMarshaler.Unmarshal(ba, &r.ReplicationTrackedDetails); err != nil {
		return err
	}
	return nil
}

// Sync global and this replication trackers with the L2 cache record.
func (r *replicationTracker) syncWithL2Cache(ctx context.Context, pushValue bool) error {

	var rtd ReplicationTrackedDetails
	// Update L2 cache of new value in global status.
	if pushValue {
		// When stable, perhaps we just issue a SetStruct here to sync L2 cache.
		if found, err := r.l2Cache.GetStructEx(ctx, replicationStatusCacheKey, &rtd, replicationStatusCacheTTLDuration); err != nil {
			return err
		} else if !found {
			if err := r.l2Cache.SetStruct(ctx, replicationStatusCacheKey, *GlobalReplicationDetails, replicationStatusCacheTTLDuration); err != nil {
				return err
			}
			return nil
		}
		// Found in L2 cache, sync it if needed.
		if rtd.isEqual(*GlobalReplicationDetails) {
			log.Debug("global replication details & l2 Cache copy is found to be in sync")
			return nil
		}
		if err := r.l2Cache.SetStruct(ctx, replicationStatusCacheKey, *GlobalReplicationDetails, replicationStatusCacheTTLDuration); err != nil {
			return err
		}
		log.Debug(fmt.Sprintf("l2 cache had been updated with global replication details value: %v", GlobalReplicationDetails))
		return nil
	}
	// pull or update global replication details with l2 cache copy.
	if found, err := r.l2Cache.GetStructEx(ctx, replicationStatusCacheKey, &rtd, replicationStatusCacheTTLDuration); err != nil {
		return err
	} else if !found {
		log.Debug("replication details not found in l2 cache")
		return nil
	}

	log.Debug(fmt.Sprintf("global replication details & this repl object are being updated w/ l2 cache copy: %v", rtd))

	// Just assign to global replication details the read rtd value from l2 cache.
	GlobalReplicationDetails = &rtd

	return nil
}
