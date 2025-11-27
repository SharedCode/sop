package inredfs

import (
	"fmt"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/fs"
)

// TransationOptions contains the transaction parameters.
type TransationOptions struct {
	// Base folder where the Stores (registry, blob & store repository) subdirectories & files
	// will be created in.
	StoresBaseFolder string
	// Transaction Mode can be Read-only or Read-Write.
	Mode sop.TransactionMode
	// Transaction maximum "commit" time. Acts as the commit window cap and lock TTL.
	// Effective limit is min(ctx deadline, MaxTime). If commit exceeds this, the transaction will roll back.
	// For end-to-end bounded operations, prefer ctx.Deadline >= MaxTime plus a small slack.
	MaxTime time.Duration
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int
	// Cache interface, will default to Redis if not specified.
	Cache sop.L2Cache
}

// TransationOptionsWithReplication contains transaction parameters for replication-enabled setups.
type TransationOptionsWithReplication struct {
	// Base folder where the Stores (registry, blob & store repository) subdirectories & files
	// will be created in. This is expected to be two element array, the 2nd element specifies
	// a 2nd folder for use in SOP replication.
	StoresBaseFolders []string `json:"stores_folders"`
	// Transaction Mode can be Read-only or Read-Write.
	Mode sop.TransactionMode `json:"mode"`
	// Transaction maximum "commit" time. Acts as the commit window cap and lock TTL.
	// Effective limit is min(ctx deadline, MaxTime). If commit exceeds this, the transaction will roll back.
	// For end-to-end bounded operations, prefer ctx.Deadline >= MaxTime plus a small slack.
	MaxTime time.Duration `json:"max_time"`
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int `json:"registry_hash_mod"`
	// Cache interface, will default to Redis if not specified.
	Cache sop.L2Cache
	// Erasure Config contains config data useful for Erasure Coding based file IO (& replication).
	ErasureConfig map[string]fs.ErasureCodingConfig `json:"erasure_config"`
}

// IsEmpty returns true if this TransationOptionsWithReplication has no configured values.
func (towr *TransationOptionsWithReplication) IsEmpty() bool {
	return len(towr.StoresBaseFolders) == 0 && towr.Cache == nil && towr.ErasureConfig == nil && towr.RegistryHashModValue == 0
}

// NewTransactionOptions creates a new TransationOptions with validated defaults for cache-related settings.
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

// NewTransactionOptionsWithReplication instantiates TransationOptionsWithReplication, applying defaults and validations.
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
	if erasureConfig == nil {
		return TransationOptionsWithReplication{}, fmt.Errorf("erasureConfig can't be nil")
	}
	if len(storesFolders) != 2 {
		return TransationOptionsWithReplication{}, fmt.Errorf("'storeFolders' need to be array of two strings(drive/folder paths)")
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
