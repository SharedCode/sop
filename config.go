package sop

import (
	"time"
)

type DatabaseType int

const (
	// Standalone mode uses an in-memory cache for coordination (locks, etc.).
	// It is appropriate for standalone or embedded applications running in a single process.
	Standalone DatabaseType = iota
	// Clustered mode uses Redis for coordination (locks, etc.).
	// It allows hosting multiple application instances across a network, properly orchestrated by SOP.
	Clustered
)

// RedisCacheConfig holds configuration for connecting to a Redis server or cluster.
type RedisCacheConfig struct {
	// Address is the host:port of the Redis server/cluster.
	Address string `json:"address"`
	// Password is the password used to authenticate.
	Password string `json:"password"`
	// DB is the database index to select.
	DB int `json:"db"`
	// URL is the connection string (e.g. redis://user:pass@host:port/db).
	// If provided, it overrides Address, Password, and DB.
	URL string `json:"url,omitempty"`
}

// DatabaseOptions holds the configuration for the database.
type DatabaseOptions struct {
	// StoresFolders specifies the folders for replication.
	// If more than one folder, i.e. - one for Active drive/folder,
	// & another for Passive drive/folder, Registry replication is enabled.
	StoresFolders []string `json:"stores_folders,omitempty"`
	// ErasureConfig specifies the erasure coding configuration for Blob store replication.
	ErasureConfig map[string]ErasureCodingConfig `json:"erasure_config,omitempty"`
	// Keyspace to be used for the transaction (Cassandra).
	Keyspace string `json:"keyspace,omitempty"`
	// CacheType specifies the type of cache to use (e.g. InMemory, Redis).
	CacheType L2CacheType `json:"cache_type"`
	// RedisConfig specifies the Redis configuration when CacheType is Redis.
	RedisConfig *RedisCacheConfig `json:"redis_config,omitempty"`
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int `json:"registry_hash_mod,omitempty"`

	// Type specifies the database type (Standalone or Clustered).
	// This is a convenience field that sets the default CacheType.
	Type DatabaseType `json:"type"`
}

// TransactionOptions holds the configuration for transactions.
// It duplicates DatabaseOptions fields to allow flat initialization syntax.
type TransactionOptions struct {
	// StoresFolders specifies the folders for replication.
	StoresFolders []string `json:"stores_folders,omitempty"`
	// ErasureConfig specifies the erasure coding configuration for replication.
	ErasureConfig map[string]ErasureCodingConfig `json:"erasure_config,omitempty"`
	// Keyspace to be used for the transaction (Cassandra).
	Keyspace string `json:"keyspace,omitempty"`
	// CacheType specifies the type of cache to use (e.g. InMemory, Redis).
	CacheType L2CacheType `json:"cache_type"`
	// RedisConfig specifies the Redis configuration when CacheType is Redis.
	RedisConfig *RedisCacheConfig `json:"redis_config,omitempty"`
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int `json:"registry_hash_mod,omitempty"`

	// Transaction Mode can be Read-only or Read-Write.
	Mode TransactionMode `json:"mode"`
	// Transaction maximum "commit" time. Acts as the commit window cap and lock TTL.
	MaxTime time.Duration `json:"max_time"`
}

// Copy Database Options to Transaction Options.
func (do DatabaseOptions) CopyTo(transOptions *TransactionOptions) {
	transOptions.StoresFolders = do.StoresFolders
	transOptions.Keyspace = do.Keyspace
	transOptions.ErasureConfig = do.ErasureConfig
	transOptions.CacheType = do.CacheType
	transOptions.RedisConfig = do.RedisConfig
	transOptions.RegistryHashModValue = do.RegistryHashModValue
}

// IsEmpty returns true if database config is considered empty, i.e. - missing folder is primary.
// A Database should always have folder(s) where Registry data files are/will be stored.
func (do DatabaseOptions) IsEmpty() bool {
	return len(do.StoresFolders) == 0
}

// GetDatabaseOptions returns the DatabaseOptions subset from TransactionOptions.
func (to TransactionOptions) GetDatabaseOptions() DatabaseOptions {
	return DatabaseOptions{
		StoresFolders:        to.StoresFolders,
		Keyspace:             to.Keyspace,
		ErasureConfig:        to.ErasureConfig,
		CacheType:            to.CacheType,
		RedisConfig:          to.RedisConfig,
		RegistryHashModValue: to.RegistryHashModValue,
	}
}

func (to TransactionOptions) IsReplicated() bool {
	return len(to.ErasureConfig) > 0 || len(to.StoresFolders) > 1
}

func (do DatabaseOptions) IsReplicated() bool {
	return len(do.ErasureConfig) > 0 || len(do.StoresFolders) > 1
}

func (to TransactionOptions) IsCassandraHybrid() bool {
	return to.Keyspace != ""
}

func (do DatabaseOptions) IsCassandraHybrid() bool {
	return do.Keyspace != ""
}

func (do DatabaseOptions) GetDatabaseType() DatabaseType {
	switch do.CacheType {
	case Redis:
		return Clustered
	default:
		return Standalone
	}
}

func (do *DatabaseOptions) SetDatabaseType(t DatabaseType) {
	do.Type = t
	if t == Clustered {
		do.CacheType = Redis
	} else {
		do.CacheType = InMemory
	}
}
