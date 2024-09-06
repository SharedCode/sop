// Package contains caching layer for S3(minion) using Redis.
// Cached S3 package can be used in many ways, like directly for implementing a cached S3 I/O.
// Or it can be used part of SOP's B-Tree store, like to implement using S3 as backend blobs storage, for example.
package red_s3

import (
	"github.com/SharedCode/sop/redis"
)

// Initialize Redis connection.
func Initialize(redisConfig redis.Options) error {
	if _, err := redis.OpenConnection(redisConfig); err != nil {
		return err
	}
	return nil
}

// Returns true if Redis is initialized(connected).
func IsInitialized() bool {
	return redis.IsConnectionInstantiated()
}

// Shutdown closes the Redis connection.
func Shutdown() {
	redis.CloseConnection()
}
