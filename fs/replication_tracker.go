package fs

import (
	"fmt"
	"os"
	"strings"
	"sync"

	log "log/slog"

	"github.com/SharedCode/sop/encoding"
)

type replicationStatus struct {
	IsInDeltaSync     bool
	FailedToReplicate bool
}

type replicationTracker struct {
	// Array so we can use in replication across two folders, if in replication mode.
	storesBaseFolders []string
	// If true, folder as specified in storesBaseFolders[0] will be the active folder,
	// otherwise the 2nd folder, as specified in storesBaseFolders[1].
	isFirstFolderActive bool
	replicate           bool
	replicationStatus   replicationStatus
}

const (
	replicationStatusFilename = "repl_stat.txt"
)

var globalReplicationTracker *replicationTracker
var globalReplicationTrackerLocker sync.Mutex = sync.Mutex{}

// Instantiates a replication tracker.
func NewReplicationTracker(storesBaseFolders []string, replicate bool) (*replicationTracker, error) {
	isFirstFolderActive := true
	rt := replicationTracker{
		storesBaseFolders:   storesBaseFolders,
		isFirstFolderActive: isFirstFolderActive,
		replicate:           replicate,
	}
	if replicate {
		// Minimize reading the replication "status" if we have read it and is tracking it globally.
		if globalReplicationTracker != nil {
			globalReplicationTrackerLocker.Lock()

			rt.isFirstFolderActive = globalReplicationTracker.isFirstFolderActive
			rt.replicationStatus = globalReplicationTracker.replicationStatus

			globalReplicationTrackerLocker.Unlock()
		} else {
			if err := rt.readStatusFromHomeFolder(); err != nil {
				return nil, fmt.Errorf("failed reading replication status (%sº file, details: %v", replicationStatusFilename, err)
			}
			globalReplicationTrackerLocker.Lock()

			globalReplicationTracker = &replicationTracker{
				storesBaseFolders:   storesBaseFolders,
				isFirstFolderActive: isFirstFolderActive,
				replicate:           replicate,
			}
			globalReplicationTracker.isFirstFolderActive = rt.isFirstFolderActive
			globalReplicationTracker.replicationStatus = rt.replicationStatus

			globalReplicationTrackerLocker.Unlock()
		}
	}
	if rt.replicationStatus.IsInDeltaSync {
		return nil, fmt.Errorf("delta sync is happening, transaction should fail")
	}
	return &rt, nil
}

// Handle replication related error is invoked from a transaction when an IO error is encountered.
// This function should handle the act of failing over to the passive destinations making them as active and the actives to be passives.
func (r *replicationTracker) HandleReplicationRelatedError(ioError error, rollbackSucceeded bool) {
	if !r.replicate {
		return
	}
	if err, ok := ioError.(ReplicationRelatedError); ok {
		log.Error(fmt.Sprintf("a replication related error detected (rollback succeeded: %v), details: %v", rollbackSucceeded, err.Error()))
		// Cause a failover switch to passive destinations on succeeding transactions.
		if err := r.failover(); err != nil {
			log.Error(fmt.Sprintf("failover to folder %s failed, details: %v", r.getPassiveBaseFolder(), err.Error()))
		}
	}
}

func (r *replicationTracker) handleFailedToReplicate() {
	if !r.replicate || r.replicationStatus.FailedToReplicate {
		return
	}
	globalReplicationTrackerLocker.Lock()
	if r.replicationStatus.FailedToReplicate {
		globalReplicationTrackerLocker.Unlock()
		return
	}

	r.replicationStatus.FailedToReplicate = true
	globalReplicationTracker.replicationStatus.FailedToReplicate = true
	r.writeReplicationStatus(r.formatActiveFolderEntity(replicationStatusFilename))

	globalReplicationTrackerLocker.Unlock()
}

func (r *replicationTracker) failover() error {
	if globalReplicationTracker.isFirstFolderActive == !r.isFirstFolderActive ||
		r.replicationStatus.FailedToReplicate {
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	globalReplicationTrackerLocker.Lock()
	if globalReplicationTracker.isFirstFolderActive == !r.isFirstFolderActive {
		globalReplicationTrackerLocker.Unlock()
		// Do nothing if global tracker already knows that a failover already occurred.
		return nil
	}

	// Set to failed to replicate because when we flip passive to active, then yes, we should not
	// replicate on the previously active drive because it failed.
	r.replicationStatus.FailedToReplicate = true

	r.writeReplicationStatus(r.formatPassiveFolderEntity(replicationStatusFilename))

	// Switch the passive into active & vice versa.
	r.isFirstFolderActive = !r.isFirstFolderActive
	globalReplicationTracker.isFirstFolderActive = r.isFirstFolderActive
	globalReplicationTracker.replicationStatus = r.replicationStatus

	globalReplicationTrackerLocker.Unlock()

	log.Info(fmt.Sprintf("failover event occurred, newly active folder is, %s", r.getActiveBaseFolder()))
	return nil
}

func (r *replicationTracker) getActiveBaseFolder() string {
	if r.isFirstFolderActive {
		return r.storesBaseFolders[0]
	}
	return r.storesBaseFolders[1]
}
func (r *replicationTracker) getPassiveBaseFolder() string {
	if r.isFirstFolderActive {
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
				r.isFirstFolderActive = !r.isFirstFolderActive
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
		r.isFirstFolderActive = !r.isFirstFolderActive
	} else {
		stat2, err := os.Stat(r.formatPassiveFolderEntity(replicationStatusFilename))
		if err == nil {
			if stat2.ModTime().After(stat.ModTime()) {
				r.isFirstFolderActive = !r.isFirstFolderActive
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
