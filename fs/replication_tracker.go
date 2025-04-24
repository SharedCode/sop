package fs

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

func (r *replicationTracker) Failover() {
	r.isFirstFolderActive = !r.isFirstFolderActive
}

func detectIfFirstIsActiveFolder(storesBaseFolders []string) bool {
	// TODO
	return true
}
