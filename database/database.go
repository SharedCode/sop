package database

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/incfs"
	"github.com/sharedcode/sop/infs"
)

// DatabaseOptions holds the configuration for the database.
// Deprecated: Use sop.DatabaseOptions instead.
type DatabaseOptions = sop.DatabaseOptions

// Database manages the storage and retrieval of data (B-Trees).
// It supports both Standalone (local) and Clustered (distributed) modes.
type Database struct {
	config sop.DatabaseOptions
	cache  sop.L2Cache
}

// NewDatabase creates a new database manager.
func NewDatabase(config sop.DatabaseOptions) *Database {
	var c sop.L2Cache
	if config.CacheType == sop.Redis {
		// Use the registered Redis cache factory.
		// Note: The application must import github.com/sharedcode/sop/adapters/redis
		// and initialize the connection (e.g. redis.OpenConnection) for this to work.
		c = sop.NewCacheClientByType(sop.Redis)
		if c == nil {
			// Fallback or Error?
			// Since Clustered implies distributed cache, we should probably panic or error if not available.
			// But NewDatabase signature returns *Database.
			// We'll panic for now as this is a configuration error.
			panic(fmt.Errorf("clustered mode requested but Redis adapter not registered. Please import 'github.com/sharedcode/sop/adapters/redis' in your main package"))
		}
	} else {
		// Default to InMemory if not specified or explicitly set.
		if config.CacheType == sop.NoCache {
			config.CacheType = sop.InMemory
		}
		c = cache.NewInMemoryCache()
	}

	return &Database{
		config: config,
		cache:  c,
	}
}

// NewCassandraDatabase creates a new database manager for Cassandra.
func NewCassandraDatabase(config sop.DatabaseOptions) *Database {
	if config.Keyspace == "" {
		panic("Cassandra mode requires Keyspace to be set")
	}
	c := sop.NewCacheClientByType(sop.Redis)
	if c == nil {
		panic(fmt.Errorf("Cassandra mode requires Redis adapter. Please import 'github.com/sharedcode/sop/adapters/redis'"))
	}
	config.CacheType = sop.Redis
	return &Database{
		config: config,
		cache:  c,
	}
}

// Cache returns the L2 cache used by the database.
func (db *Database) Cache() sop.L2Cache {
	return db.cache
}

// StoragePath returns the base storage path.
func (db *Database) StoragePath() string {
	if len(db.config.StoresFolders) > 0 {
		return db.config.StoresFolders[0]
	}
	return ""
}

// ErasureConfig returns the configured erasure coding configuration.
func (db *Database) ErasureConfig() map[string]sop.ErasureCodingConfig {
	return db.config.ErasureConfig
}

// CacheType returns the configured cache type.
func (db *Database) CacheType() sop.CacheType {
	return db.config.CacheType
}

// StoresFolders returns the list of storage folders (for replication).
func (db *Database) StoresFolders() []string {
	return db.config.StoresFolders
}

// BeginTransaction starts a new transaction.
func (db *Database) BeginTransaction(ctx context.Context, mode sop.TransactionMode, maxTime ...time.Duration) (sop.Transaction, error) {
	var mt time.Duration
	if len(maxTime) > 0 {
		mt = maxTime[0]
	} else {
		mt = 15 * time.Minute
	}

	var opts sop.TransactionOptions
	// Merge DatabaseOptions into TransactionOptions
	db.config.CopyTo(&opts)
	opts.Mode = mode
	opts.MaxTime = mt
	opts.Logging = true

	var t sop.Transaction
	var err error
	if db.config.Keyspace != "" {
		t, err = incfs.NewTransaction(ctx, opts)
	} else if len(opts.ErasureConfig) > 0 || len(opts.StoresFolders) > 1 {
		// Use Replication
		// Ensure BaseFolderPathsAcrossDrives is populated if missing, using StoresFolders.
		for k, v := range opts.ErasureConfig {
			if len(v.BaseFolderPathsAcrossDrives) == 0 {
				v.BaseFolderPathsAcrossDrives = opts.StoresFolders
				opts.ErasureConfig[k] = v
			}
		}
		t, err = infs.NewTransactionWithReplication(ctx, opts)
	} else {
		t, err = infs.NewTransaction(ctx, opts)
	}

	if err != nil {
		return nil, err
	}

	if err := t.Begin(ctx); err != nil {
		return nil, err
	}

	return t, nil
}

// OpenBtree opens a general purpose B-Tree store.
// This allows the Database to manage standard Key-Value stores alongside AI stores.
func (db *Database) OpenBtree(ctx context.Context, name string, t sop.Transaction) (btree.BtreeInterface[any, any], error) {
	if len(db.config.StoresFolders) > 0 {
		if err := os.MkdirAll(db.config.StoresFolders[0], 0755); err != nil {
			return nil, err
		}
	}
	if db.config.Keyspace != "" {
		return incfs.OpenBtree[any, any](ctx, name, t, nil)
	}
	if len(db.config.StoresFolders) > 1 || len(db.config.ErasureConfig) > 0 {
		return infs.OpenBtreeWithReplication[any, any](ctx, name, t, nil)
	}
	// We use string keys and any values for a generic store, but users can use specific types if they use infs.directly.
	// For the Database wrapper, we provide a sensible default or we could make this generic if Go allowed methods to have type parameters (it does).
	// However, since Database is a struct, we can't easily make this method generic for the return type without the struct being generic.
	// For now, we'll expose a string/any B-Tree.
	return infs.OpenBtree[any, any](ctx, name, t, nil)
}

// NewBtree creates a new general purpose B-Tree store.
func (db *Database) NewBtree(ctx context.Context, name string, t sop.Transaction, options ...sop.StoreOptions) (btree.BtreeInterface[any, any], error) {
	if len(db.config.StoresFolders) > 0 {
		if err := os.MkdirAll(db.config.StoresFolders[0], 0755); err != nil {
			return nil, err
		}
	}
	var opts sop.StoreOptions
	if len(options) > 0 {
		opts = options[0]
		opts.Name = name
	} else {
		opts = sop.StoreOptions{
			Name:                     name,
			SlotLength:               1000,
			IsUnique:                 true,
			IsValueDataInNodeSegment: true,
			LeafLoadBalancing:        false,
			Description:              "General purpose B-Tree created via Database",
		}
	}
	if db.config.Keyspace != "" {
		if len(db.config.StoresFolders) > 0 {
			opts.BlobStoreBaseFolderPath = db.config.StoresFolders[0]
		}
		return incfs.NewBtree[any, any](ctx, opts, t, nil)
	}
	if len(db.config.StoresFolders) > 1 || len(db.config.ErasureConfig) > 0 {
		return infs.NewBtreeWithReplication[any, any](ctx, opts, t, nil)
	}
	return infs.NewBtree[any, any](ctx, opts, t, nil)
}
