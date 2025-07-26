package fs

// Erasure Coding Config
type ErasureCodingConfig struct {
	// Count of data shards.
	DataShardsCount int `json:"data_shards_count"`
	// Count of parity shards.
	ParityShardsCount int `json:"parity_shards_count"`
	// Base folder paths across different drives to store the data (data & parity shards) files.
	BaseFolderPathsAcrossDrives []string `json:"base_folder_paths_across_drives"`

	// Flag to tell SOP whether to repair detected corrupted shards or not.
	//
	// Auto-repair is kind of expensive so, your code is given chance to run
	// optimally (false) or with feature to auto-repair (true). Usually, when
	// a drive failure occurs, this will be reported by SOP library and which,
	// you can write your app to handle this or not (default can be NOT to auto-repair).
	RepairCorruptedShards bool `json:"repair_corrupted_shards"`
}
