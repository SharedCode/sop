package in_red_fs

import (
	"fmt"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/fs"
)

// Transaction Options contains the Transaction parameters.
type TransationOptions struct {
	// Base folder where the Stores (registry, blob & store repository) subdirectories & files
	// will be created in.
	StoresBaseFolder string
	// Transaction Mode can be Read-only or Read-Write.
	Mode sop.TransactionMode
	// Transaction maximum "commit" time. If commits takes longer than this then transaction will roll back.
	MaxTime time.Duration
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int
	// Cache interface, will default to Redis if not specified.
	Cache sop.Cache
}

type TransationOptionsWithReplication struct {
	// Base folder where the Stores (registry, blob & store repository) subdirectories & files
	// will be created in. This is expected to be two element array, the 2nd element specifies
	// a 2nd folder for use in SOP replication.
	StoresBaseFolders []string
	// Transaction Mode can be Read-only or Read-Write.
	Mode sop.TransactionMode
	// Transaction maximum "commit" time. If commits takes longer than this then transaction will roll back.
	MaxTime time.Duration
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int
	// Cache interface, will default to Redis if not specified.
	Cache sop.Cache
	// Erasure Config contains config data useful for Erasure Coding based file IO (& replication).
	ErasureConfig map[string]fs.ErasureCodingConfig
}

// Create a new TransactionOptions using defaults for cache related.
func NewTransactionOptions(storeFolder string, mode sop.TransactionMode, maxTime time.Duration,
	registryHashMod int) (TransationOptions, error) {

	if storeFolder == "" {
		return TransationOptions{}, fmt.Errorf("storeFolder can't be empty")
	}

	if registryHashMod < fs.MinimumModValue {
		registryHashMod = fs.MinimumModValue
	}
	if registryHashMod > fs.MaximumModValue {
		registryHashMod = fs.MaximumModValue
	}

	return TransationOptions{
		StoresBaseFolder:     storeFolder,
		Mode:                 mode,
		MaxTime:              maxTime,
		RegistryHashModValue: registryHashMod,
	}, nil
}

// Create a new TransactionOptionsWithReplication using defaults for cache related.
func NewTransactionOptionsWithReplication(storeFolders []string, mode sop.TransactionMode, maxTime time.Duration,
	registryHashMod int,
	erasureConfig map[string]fs.ErasureCodingConfig) (TransationOptionsWithReplication, error) {
	if erasureConfig == nil {
		erasureConfig = fs.GetGlobalErasureConfig()
	}
	if storeFolders == nil && len(erasureConfig) > 0 {
		storeFolders = make([]string, 0, 2)
		defaultEntry := erasureConfig[""]
		if len(defaultEntry.BaseFolderPathsAcrossDrives) >= 2 {
			storeFolders = append(storeFolders, defaultEntry.BaseFolderPathsAcrossDrives[0])
			storeFolders = append(storeFolders, defaultEntry.BaseFolderPathsAcrossDrives[1])
		} else {
			for _, v := range erasureConfig {
				if len(v.BaseFolderPathsAcrossDrives) >= 2 {
					storeFolders = append(storeFolders, v.BaseFolderPathsAcrossDrives[0])
					storeFolders = append(storeFolders, v.BaseFolderPathsAcrossDrives[1])
					break
				}
			}
		}
	}

	if len(storeFolders) == 0 {
		return TransationOptionsWithReplication{}, fmt.Errorf("storeFolders is nil & can't get extracted from erasureConfig")
	}

	if registryHashMod < fs.MinimumModValue {
		registryHashMod = fs.MinimumModValue
	}
	if registryHashMod > fs.MaximumModValue {
		registryHashMod = fs.MaximumModValue
	}

	return TransationOptionsWithReplication{
		StoresBaseFolders:    storeFolders,
		Mode:                 mode,
		MaxTime:              maxTime,
		RegistryHashModValue: registryHashMod,
		ErasureConfig:        erasureConfig,
	}, nil
}
