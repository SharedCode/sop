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
	CacheType L2CacheType `json:"cache_type"`
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
	// Keyspace to be used for the transaction (Cassandra).
	Keyspace string `json:"keyspace,omitempty"`
	// ErasureConfig specifies the erasure coding configuration for replication.
	ErasureConfig map[string]ErasureCodingConfig `json:"erasure_config,omitempty"`
	// CacheType specifies the type of cache to use (e.g. InMemory, Redis).
	CacheType L2CacheType `json:"cache_type"`
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
	transOptions.RegistryHashModValue = do.RegistryHashModValue
}

// GetDatabaseOptions returns the DatabaseOptions subset from TransactionOptions.
func (to TransactionOptions) GetDatabaseOptions() DatabaseOptions {
	return DatabaseOptions{
		StoresFolders:        to.StoresFolders,
		Keyspace:             to.Keyspace,
		ErasureConfig:        to.ErasureConfig,
		CacheType:            to.CacheType,
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
