// Package inredfs provides SOP implementations that use Redis for caching
// and the local filesystem for backend data storage. It includes package-level
// initialization helpers (Initialize, IsInitialized, Shutdown), transaction
// constructors (with and without replication), B-tree convenience wrappers,
// and a streaming data store built on top of B-trees for chunked large-object
// storage.
package inredfs

import (
	"github.com/sharedcode/sop/redis"
)

// Initialize assigns configs and opens connections to subsystems used by this package (e.g., Redis).
func Initialize(redisConfig redis.Options) error {
	if _, err := redis.OpenConnection(redisConfig); err != nil {
		return err
	}
	return nil
}

// IsInitialized reports whether required components have been initialized.
func IsInitialized() bool {
	return redis.IsConnectionInstantiated()
}

// Shutdown closes all connections used by this package.
func Shutdown() {
	redis.CloseConnection()
}
