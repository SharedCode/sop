package fs

import (
	"fmt"
	"os"
	"strings"

	log "log/slog"
)

type replicationTracker struct {
	// Array so we can use in replication across two folders, if in replication mode.
	storesBaseFolders []string
	// If true, folder as specified in storesBaseFolders[0] will be the active folder,
	// otherwise the 2nd folder, as specified in storesBaseFolders[1].
	isFirstFolderActive bool
	replicate           bool
	isInDeltaSync       bool
}

const(
	replicationStatusFilename = "repl_status.txt"
)

var globalReplicationTracker *replicationTracker

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
			rt.isFirstFolderActive = globalReplicationTracker.isFirstFolderActive
			rt.isInDeltaSync = globalReplicationTracker.isInDeltaSync
		} else {
			rt.readStatusFromHomeFolder(storesBaseFolders)
			globalReplicationTracker = &replicationTracker{
				storesBaseFolders:   storesBaseFolders,
				isFirstFolderActive: isFirstFolderActive,
				replicate:           replicate,
			}
			globalReplicationTracker.isFirstFolderActive = rt.isFirstFolderActive
			globalReplicationTracker.isInDeltaSync = rt.isInDeltaSync
		}
	}
	if rt.isInDeltaSync {
		return nil, fmt.Errorf("delta sync is happening, transaction should fail")
	}
	return &rt, nil
}

// Handle replication related error is invoked from a transaction when an IO error is encountered.
// This function should handle the act of failing over to the passive destinations making them as active and the actives to be passives.
func (r *replicationTracker)HandleReplicationRelatedError(ioError error, rollbackSucceeded bool) bool {
	if err, ok := ioError.(ReplicationRelatedError); ok {
		log.Error(fmt.Sprintf("a replication related error detected (rollback succeeded: %v), details: %v", rollbackSucceeded, err.Error()))
		// Cause a failover switch to passive destinations on succeeding transactions.
		//r.failover()
	}
	return true
}

func (r *replicationTracker) failover() {
	if globalReplicationTracker.isFirstFolderActive == !r.isFirstFolderActive {
		// Do nothing if global tracker already knows that a failover already occurred.
		return
	}

	fio := NewDefaultFileIO(ToFilePath)
	fio.WriteFile(r.formatPassiveFolderEntity(replicationStatusFilename), fmt.Appendf(nil, "isInDeltaSync:%v", r.isInDeltaSync), permission)

	// Swtich the passive into active & vice versa.
	r.isFirstFolderActive = !r.isFirstFolderActive
	globalReplicationTracker.isFirstFolderActive = r.isFirstFolderActive
	log.Info(fmt.Sprintf("failover event occurred, newly active folder is, %s", r.getActiveBaseFolder()))
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

func (r *replicationTracker) readStatusFromHomeFolder(storesBaseFolders []string) {

	//fio := NewDefaultFileIO(ToFilePath)
	//fio.ReadFile(r.formatPassiveFolderEntity(replicationStatusFilename), fmt.Appendf(nil, "isInDeltaSync:%v", r.isInDeltaSync), permission)

}
