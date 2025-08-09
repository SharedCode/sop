// Package inredcfs provides SOP implementations that use Redis for caching and the filesystem
// for backend storage. It exposes package-level initialization helpers and B-Tree wrappers that
// enable filesystem replication.
package inredcfs

import (
	cas "github.com/sharedcode/sop/cassandra"
	"github.com/sharedcode/sop/inredck"
	"github.com/sharedcode/sop/redis"
)

// Assign the configs & open connections to different sub-systems used by this package.
// Example, connection to Cassandra, Redis.
func Initialize(cassandraConfig cas.Config, redisConfig redis.Options) error {
	return inredck.Initialize(cassandraConfig, redisConfig)
}

// Returns true if components required were initialized, false otherwise.
func IsInitialized() bool {
	return inredck.IsInitialized()
}

// Shutdown or closes all connections used in this package.
func Shutdown() {
	inredck.Shutdown()
}
