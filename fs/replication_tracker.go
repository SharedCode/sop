package fs

import (
	"fmt"
	"os"
	"strings"
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

// Instantiates a replication tracker.
func NewReplicationTracker(storesBaseFolders []string, replicate bool) (*replicationTracker, error) {
	isFirstFolderActive := true
	rt := replicationTracker{
		storesBaseFolders:   storesBaseFolders,
		isFirstFolderActive: isFirstFolderActive,
		replicate:           replicate,
	}
	if replicate {
		rt.readStatusFromHomeFolder(storesBaseFolders)
	}
	if rt.isInDeltaSync {
		return nil, fmt.Errorf("delta sync is happening, transaction should fail")
	}
	return &rt, nil
}

func (r *replicationTracker) failover() {
	r.isFirstFolderActive = !r.isFirstFolderActive
	// TODO: write to log file the new active folder flag.
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
	// TODO: read the replication status from log file(s) in home folder.

}
