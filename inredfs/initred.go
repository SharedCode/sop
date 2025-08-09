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
