package sop

import (
	"time"
)

type DatabaseType int

const (
	Standalone = iota
	Clustered
)

// DatabaseOptions holds the configuration for the database.
type DatabaseOptions struct {
	// StoresFolders specifies the folders for replication.
	// If provided, replication is enabled.
	StoresFolders []string `json:"stores_folders,omitempty"`
	// Keyspace to be used for the transaction (Cassandra).
	Keyspace string `json:"keyspace,omitempty"`
	// ErasureConfig specifies the erasure coding configuration for replication.
	ErasureConfig map[string]ErasureCodingConfig `json:"erasure_config,omitempty"`
	// CacheType specifies the type of cache to use (e.g. InMemory, Redis).
	CacheType CacheType `json:"cache_type"`
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int `json:"registry_hash_mod,omitempty"`
}

// TransactionOptions holds the configuration for transactions.
// It duplicates DatabaseOptions fields to allow flat initialization syntax.
type TransactionOptions struct {
	// StoresFolders specifies the folders for replication.
	StoresFolders []string `json:"stores_folders,omitempty"`
	// Keyspace to be used for the transaction (Cassandra).
	Keyspace string `json:"keyspace,omitempty"`
	// ErasureConfig specifies the erasure coding configuration for replication.
	ErasureConfig map[string]ErasureCodingConfig `json:"erasure_config,omitempty"`
	// CacheType specifies the type of cache to use (e.g. InMemory, Redis).
	CacheType CacheType `json:"cache_type"`
	// Registry hash modulo value used for hashing.
	RegistryHashModValue int `json:"registry_hash_mod,omitempty"`

	// Transaction Mode can be Read-only or Read-Write.
	Mode TransactionMode `json:"mode"`
	// Transaction maximum "commit" time. Acts as the commit window cap and lock TTL.
	MaxTime time.Duration `json:"max_time"`
	// Logging enables transaction logging.
	Logging bool `json:"logging"`
}

// Copy Database Options to Transaction Options.
func (t DatabaseOptions) CopyTo(transOptions *TransactionOptions) {
	transOptions.StoresFolders = t.StoresFolders
	transOptions.Keyspace = t.Keyspace
	transOptions.ErasureConfig = t.ErasureConfig
	transOptions.CacheType = t.CacheType
	transOptions.RegistryHashModValue = t.RegistryHashModValue
}

// GetDatabaseOptions returns the DatabaseOptions subset from TransactionOptions.
func (t TransactionOptions) GetDatabaseOptions() DatabaseOptions {
	return DatabaseOptions{
		StoresFolders:        t.StoresFolders,
		Keyspace:             t.Keyspace,
		ErasureConfig:        t.ErasureConfig,
		CacheType:            t.CacheType,
		RegistryHashModValue: t.RegistryHashModValue,
	}
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
	if t == Clustered {
		do.CacheType = Redis
	} else {
		do.CacheType = InMemory
	}
}
