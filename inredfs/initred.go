package inredfs

import (
	"github.com/sharedcode/sop/redis"
)

// Assign the configs & open connections to different sub-systems used by this package.
// Example, connection to Redis.
func Initialize(redisConfig redis.Options) error {
	if _, err := redis.OpenConnection(redisConfig); err != nil {
		return err
	}
	return nil
}

// Returns true if components required were initialized, false otherwise.
func IsInitialized() bool {
	return redis.IsConnectionInstantiated()
}

// Shutdown or closes all connections used in this package.
func Shutdown() {
	redis.CloseConnection()
}
