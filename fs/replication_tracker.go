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
}

func NewReplicationTracker(storesBaseFolders []string, replicate bool) *replicationTracker {
	isFirstFolderActive := true
	if replicate {
		isFirstFolderActive = detectIfFirstIsActiveFolder(storesBaseFolders)
	}
	return &replicationTracker{
		storesBaseFolders:   storesBaseFolders,
		isFirstFolderActive: isFirstFolderActive,
		replicate:           replicate,
	}
}

func (r *replicationTracker) failover() {
	r.isFirstFolderActive = !r.isFirstFolderActive
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

func detectIfFirstIsActiveFolder(storesBaseFolders []string) bool {
	// TODO
	return true
}
