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

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/redis"
)

type replicationStatus struct {
	IsInDeltaSync     bool
	FailedToReplicate bool
}

type replicationTrackedDetails struct {
	replicationStatus
	// If true, folder as specified in storesBaseFolders[0] will be the active folder,
	// otherwise the 2nd folder, as specified in storesBaseFolders[1].
	IsFirstFolderActive bool
}
func (a replicationTrackedDetails)isEqual(b replicationTrackedDetails) bool {
	return a.IsInDeltaSync == b.IsInDeltaSync && a.IsFirstFolderActive == b.IsFirstFolderActive && a.FailedToReplicate == b.FailedToReplicate
}

type replicationTracker struct {
	replicationTrackedDetails
	// Array so we can use in replication across two folders, if in replication mode.
	storesBaseFolders []string
	replicate           bool
	l2Cache            sop.Cache
}

const (
	replicationStatusFilename = "replstat.txt"
	replicationStatusCacheKey = "Rreplstat"
	replicationStatusCacheTTLDuration = 5*time.Minute
)

var globalReplicationDetails *replicationTrackedDetails
var globalReplicationDetailsLocker sync.Mutex = sync.Mutex{}

// Instantiates a replication tracker.
func NewReplicationTracker(ctx context.Context, storesBaseFolders []string, replicate bool, l2Cache sop.Cache) (*replicationTracker, error) {
	if l2Cache == nil {
		l2Cache = redis.NewClient()
	}
	isFirstFolderActive := true
	rt := replicationTracker{
		storesBaseFolders:   storesBaseFolders,
		replicate:           replicate,
		l2Cache: l2Cache,
	}
	rt.IsFirstFolderActive = isFirstFolderActive
	if replicate {
		if err := rt.syncWithL2Cache(ctx, false); err != nil {
			log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
		}
		// Minimize reading the replication "status" if we have read it and is tracking it globally.
		if globalReplicationDetails != nil {
			globalReplicationDetailsLocker.Lock()

			rt.IsFirstFolderActive = globalReplicationDetails.IsFirstFolderActive
			rt.replicationStatus = globalReplicationDetails.replicationStatus

			globalReplicationDetailsLocker.Unlock()
		} else {
			if err := rt.readStatusFromHomeFolder(); err != nil {
				return nil, fmt.Errorf("failed reading replication status (%sº file, details: %v", replicationStatusFilename, err)
			}
			globalReplicationDetailsLocker.Lock()

			globalReplicationDetails = &replicationTrackedDetails{
				IsFirstFolderActive: isFirstFolderActive,
			}
			globalReplicationDetails.IsFirstFolderActive = rt.IsFirstFolderActive
			globalReplicationDetails.replicationStatus = rt.replicationStatus

			// Sync l2 cache.
			if err := rt.syncWithL2Cache(ctx, true); err != nil {
				log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
			}

			globalReplicationDetailsLocker.Unlock()
		}
	}
	if rt.replicationStatus.IsInDeltaSync {
		return nil, fmt.Errorf("delta sync is happening, transaction should fail")
	}
	return &rt, nil
}

// Handle replication related error is invoked from a transaction when an IO error is encountered.
// This function should handle the act of failing over to the passive destinations making them as active and the active to be passive.
func (r *replicationTracker) HandleReplicationRelatedError(ctx context.Context, ioError error, rollbackSucceeded bool) {
	if !r.replicate {
		return
	}
	rootErr := errors.Unwrap(ioError)
	err1, ok1 := rootErr.(ReplicationRelatedError)
	err2, ok2 := ioError.(ReplicationRelatedError)
	if ok2 {
		err1 = err2
	}
	if ok1 || ok2 {
		log.Error(fmt.Sprintf("a replication related error detected (rollback: %v), details: %v", rollbackSucceeded, err1.Error()))
		// Cause a failover switch to passive destinations on succeeding transactions.
		if err := r.failover(ctx); err != nil {
			log.Error(fmt.Sprintf("failover to folder %s failed, details: %v", r.getPassiveBaseFolder(), err.Error()))
		}
	}
}

func (r *replicationTracker) handleFailedToReplicate(ctx context.Context) {
	if !r.replicate || r.replicationStatus.FailedToReplicate {
		return
	}

	if err := r.syncWithL2Cache(ctx, false); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	if r.replicationStatus.FailedToReplicate {
		return
	}

	globalReplicationDetailsLocker.Lock()

	if r.replicationStatus.FailedToReplicate {
		globalReplicationDetailsLocker.Unlock()
		return
	}

	r.replicationStatus.FailedToReplicate = true
	globalReplicationDetails.replicationStatus.FailedToReplicate = true
	if err := r.writeReplicationStatus(r.formatActiveFolderEntity(replicationStatusFilename)); err != nil {
		log.Warn(fmt.Sprintf("handleFailedToReplicate writeReplicationStatus failed, details: %v", err))
	}

	// Sync l2 cache.
	if err := r.syncWithL2Cache(ctx, true); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	globalReplicationDetailsLocker.Unlock()
}

func (r *replicationTracker) failover(ctx context.Context) error {
	if globalReplicationDetails.IsFirstFolderActive == !r.IsFirstFolderActive ||
		r.replicationStatus.FailedToReplicate {
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	if err := r.syncWithL2Cache(ctx, false); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	if globalReplicationDetails.IsFirstFolderActive == !r.IsFirstFolderActive ||
		r.replicationStatus.FailedToReplicate {
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	globalReplicationDetailsLocker.Lock()
	if globalReplicationDetails.IsFirstFolderActive == !r.IsFirstFolderActive {
		globalReplicationDetailsLocker.Unlock()
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	// Set to failed to replicate because when we flip passive to active, then yes, we should not
	// replicate on the previously active drive because it failed.
	r.replicationStatus.FailedToReplicate = true

	if err := r.writeReplicationStatus(r.formatPassiveFolderEntity(replicationStatusFilename)); err != nil {
		globalReplicationDetailsLocker.Unlock()
		return err
	}

	// Switch the passive into active & vice versa.
	r.IsFirstFolderActive = !r.IsFirstFolderActive
	globalReplicationDetails.IsFirstFolderActive = r.IsFirstFolderActive
	globalReplicationDetails.replicationStatus = r.replicationStatus

	// Sync l2 cache.
	if err := r.syncWithL2Cache(ctx, true); err != nil {
		log.Warn(fmt.Sprintf("error while updating global replication status & L2 cache, details: %v", err))
	}

	globalReplicationDetailsLocker.Unlock()

	log.Info(fmt.Sprintf("failover event occurred, newly active folder is, %s", r.getActiveBaseFolder()))
	return nil
}

func (r *replicationTracker) getActiveBaseFolder() string {
	if r.IsFirstFolderActive {
		return r.storesBaseFolders[0]
	}
	return r.storesBaseFolders[1]
}
func (r *replicationTracker) getPassiveBaseFolder() string {
	if r.IsFirstFolderActive {
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

func (r *replicationTracker) readStatusFromHomeFolder() error {
	fio := NewDefaultFileIO()
	// Detect the active folder based on time stamp of the file.
	if !fio.Exists(r.formatActiveFolderEntity(replicationStatusFilename)) {
		if fio.Exists(r.formatPassiveFolderEntity(replicationStatusFilename)) {
			if err := r.readReplicationStatus(r.formatPassiveFolderEntity(replicationStatusFilename)); err == nil {
				// Switch passive to active if we are able to read the delta sync status file.
				r.IsFirstFolderActive = !r.IsFirstFolderActive
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
		r.IsFirstFolderActive = !r.IsFirstFolderActive
	} else {
		stat2, err := os.Stat(r.formatPassiveFolderEntity(replicationStatusFilename))
		if err == nil {
			if stat2.ModTime().After(stat.ModTime()) {
				r.IsFirstFolderActive = !r.IsFirstFolderActive
			}
		}
	}

	return r.readReplicationStatus(r.formatActiveFolderEntity(replicationStatusFilename))
}

func (r *replicationTracker) writeReplicationStatus(filename string) error {
	fio := NewDefaultFileIO()
	ba, _ := encoding.DefaultMarshaler.Marshal(r.replicationStatus)
	if err := fio.WriteFile(filename, ba, permission); err != nil {
		return err
	}
	return nil
}

func (r *replicationTracker) readReplicationStatus(filename string) error {
	fio := NewDefaultFileIO()
	// Read the delta sync status.
	ba, err := fio.ReadFile(filename)
	if err != nil {
		return err
	}
	if err = encoding.DefaultMarshaler.Unmarshal(ba, &r.replicationStatus); err != nil {
		return err
	}
	return nil
}

// Sync global and this replication trackers with the L2 cache record.
func (r *replicationTracker)syncWithL2Cache(ctx context.Context, pushValue bool) error {

	var rtd replicationTrackedDetails
	// Update L2 cache of new value in global status.
	if pushValue {
		// When stable, perhaps we just issue a SetStruct here to sync L2 cache.
		if found, err := r.l2Cache.GetStructEx(ctx, replicationStatusCacheKey, &rtd, replicationStatusCacheTTLDuration); err != nil {
			return err
		} else if !found {
			if err := r.l2Cache.SetStruct(ctx, replicationStatusCacheKey, *globalReplicationDetails, replicationStatusCacheTTLDuration); err != nil {
				return err
			}
			return nil
		}
		// Found in L2 cache, sync it if needed.
		if rtd.isEqual(*globalReplicationDetails) {
			log.Debug("global replication details & l2 Cache copy is found to be in sync")
			return nil
		}
		if err := r.l2Cache.SetStruct(ctx, replicationStatusCacheKey, *globalReplicationDetails, replicationStatusCacheTTLDuration); err != nil {
			return err
		}
		log.Debug(fmt.Sprintf("l2 cache had been updated with global replication details value: %v", globalReplicationDetails))
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
	globalReplicationDetails = &rtd
	// Sync this object.
	r.replicationTrackedDetails = rtd

	return nil
}
