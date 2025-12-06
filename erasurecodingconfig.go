package sop

// ErasureCodingConfig defines per-blob-table erasure coding settings, including
// shard counts, storage locations, and optional automatic shard repair.
type ErasureCodingConfig struct {
	// DataShardsCount is the number of data shards.
	DataShardsCount int `json:"data_shards_count"`
	// ParityShardsCount is the number of parity shards.
	ParityShardsCount int `json:"parity_shards_count"`
	// BaseFolderPathsAcrossDrives lists the drive base paths where data and parity shard files are stored.
	BaseFolderPathsAcrossDrives []string `json:"base_folder_paths_across_drives"`

	// RepairCorruptedShards indicates whether to attempt automatic repair when corrupted shards are detected.
	// Auto-repair can be expensive; applications can disable it to prioritize throughput and handle drive
	// failures via external workflows.
	RepairCorruptedShards bool `json:"repair_corrupted_shards"`
}
