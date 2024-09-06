package in_red_ck

import (
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/redis"
)

// Assign the configs & open connections to different sub-systems used by this package.
// Example, connection to Cassandra, Redis.
func Initialize(cassandraConfig cas.Config, redisConfig redis.Options) error {
	if _, err := cas.OpenConnection(cassandraConfig); err != nil {
		return err
	}
	if _, err := redis.OpenConnection(redisConfig); err != nil {
		return err
	}
	return nil
}

// Returns true if components required were initialized, false otherwise.
func IsInitialized() bool {
	return cas.IsConnectionInstantiated() && redis.IsConnectionInstantiated()
}

// Shutdown or closes all connections used in this package.
func Shutdown() {
	cas.CloseConnection()
	redis.CloseConnection()
}
