// Package contains caching layer for S3(minion) using Redis.
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
