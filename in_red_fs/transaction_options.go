package in_red_fs

import (
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
	RegistryHashModValue fs.HashModValueType
	// Cache interface, will default to Redis if not specified.
	Cache sop.Cache
	// true will tell registry's hashmap to use Redis for file region locking, otherwise
	// it will use the syscall API for lock managements on file.
	UseCacheForFileRegionLocks bool
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
	RegistryHashModValue fs.HashModValueType
	// Cache interface, will default to Redis if not specified.
	Cache sop.Cache
	// true will tell registry's hashmap to use Redis for file region locking, otherwise
	// it will use the syscall API for lock managements on file.
	UseCacheForFileRegionLocks bool
	// Erasure Config contains config data useful for Erasure Coding based file IO (& replication).
	ErasureConfig map[string]fs.ErasureCodingConfig
}
