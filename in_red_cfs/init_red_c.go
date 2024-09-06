package in_red_cfs

import (
	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/redis"
)

// Assign the configs & open connections to different sub-systems used by this package.
// Example, connection to Cassandra, Redis.
func Initialize(cassandraConfig cas.Config, redisConfig redis.Options) error {
	return in_red_ck.Initialize(cassandraConfig, redisConfig)
}

// Returns true if components required were initialized, false otherwise.
func IsInitialized() bool {
	return in_red_ck.IsInitialized()
}

// Shutdown or closes all connections used in this package.
func Shutdown() {
	in_red_ck.Shutdown()
}
