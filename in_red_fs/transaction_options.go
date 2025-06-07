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

// Instantiates a new TransactionOptionsWithReplication options struct populated with values from parameters
// and some fields using recommended default values or seeded with values based on the parameters received.
//
// storesFolders & erasureConfig parameters serve the same purpose as how they got used/
// values passed in in the call to NewTransactionOptionsWithReplication(..).
// storesFolders should contain the active & passive stores' base folder paths.
// erasureConfig should be nil if storesFolders is already specified.
//
// Also, if you want SOP to use the global erasure config and there is one set (in "fs" package), then these
// two can be nil. In a bit later, SOP may support caching in L2 cache the storesFolders, so,
// in that version, this function may just take it from L2 cache (Redis).
//
// If storesFolders is nil, SOP will use the 1st two drive/paths it can find from the
// erasureConfig or the global erasure config, whichever is passed in or available.
// The default erasureConfig map entry (with key "") will be tried for use and if this is not
// set or it only has one path, then the erasureConfig map will be iterated and whichever
// entry with at least two drive paths set, then will "win" as the stores base folders paths.
//
// Explicitly specifying it in storesFolders param is recommended.
func NewTransactionOptionsWithReplication(mode sop.TransactionMode, maxTime time.Duration, registryHashMod int,
	storesFolders []string, erasureConfig map[string]fs.ErasureCodingConfig) (TransationOptionsWithReplication, error) {
	if erasureConfig == nil {
		erasureConfig = fs.GetGlobalErasureConfig()
	}
	if storesFolders == nil && len(erasureConfig) > 0 {
		storesFolders = make([]string, 0, 2)
		defaultEntry := erasureConfig[""]
		if len(defaultEntry.BaseFolderPathsAcrossDrives) >= 2 {
			storesFolders = append(storesFolders, defaultEntry.BaseFolderPathsAcrossDrives[0])
			storesFolders = append(storesFolders, defaultEntry.BaseFolderPathsAcrossDrives[1])
		} else {
			for _, v := range erasureConfig {
				if len(v.BaseFolderPathsAcrossDrives) >= 2 {
					storesFolders = append(storesFolders, v.BaseFolderPathsAcrossDrives[0])
					storesFolders = append(storesFolders, v.BaseFolderPathsAcrossDrives[1])
					break
				}
			}
		}
	}

	if len(storesFolders) < 2 {
		return TransationOptionsWithReplication{}, fmt.Errorf("'storeFolders' need to be array of two strings(drive/folder paths). 'was not able to reuse anything from 'erasureConfig'")
	}

	if registryHashMod < fs.MinimumModValue {
		registryHashMod = fs.MinimumModValue
	}
	if registryHashMod > fs.MaximumModValue {
		registryHashMod = fs.MaximumModValue
	}

	return TransationOptionsWithReplication{
		StoresBaseFolders:    storesFolders,
		Mode:                 mode,
		MaxTime:              maxTime,
		RegistryHashModValue: registryHashMod,
		ErasureConfig:        erasureConfig,
	}, nil
}
