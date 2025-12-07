package database

import (
	"context"
	"fmt"
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
	// If CacheType is not set, infer it from Type.
	if config.CacheType == sop.NoCache {
		if config.Type == sop.Clustered {
			config.CacheType = sop.Redis
		} else {
			config.CacheType = sop.InMemory
		}
	}

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

	if opts.IsReplicated() {
		if opts.IsCassandraHybrid() {
			t, err = incfs.NewTransactionWithReplication(ctx, opts)
		} else {
			t, err = infs.NewTransactionWithReplication(ctx, opts)
		}
	} else {
		if db.config.IsCassandraHybrid() {
			t, err = incfs.NewTransaction(ctx, opts)
		} else {
			t, err = infs.NewTransaction(ctx, opts)
		}
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
	return OpenBtree[any, any](ctx, db, name, t, nil)
}

// OpenBtree opens a general purpose B-Tree store.
// This allows the Database to manage standard Key-Value stores alongside AI stores.
func OpenBtree[TK btree.Ordered, TV any](ctx context.Context, db *Database, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if db.config.IsReplicated() {
		if db.config.IsCassandraHybrid() {
			return incfs.OpenBtree[TK, TV](ctx, name, t, comparer)
		} else {
			return infs.OpenBtreeWithReplication[TK, TV](ctx, name, t, comparer)
		}
	} else {
		if db.config.IsCassandraHybrid() {
			return incfs.OpenBtree[TK, TV](ctx, name, t, comparer)
		} else {
			return infs.OpenBtree[TK, TV](ctx, name, t, comparer)
		}
	}
}

// NewBtree creates a new general purpose B-Tree store.
func (db *Database) NewBtree(ctx context.Context, name string, t sop.Transaction, options ...sop.StoreOptions) (btree.BtreeInterface[any, any], error) {
	return NewBtree[any, any](ctx, db, name, t, nil)
}

// NewBtree creates a new general purpose B-Tree store.
func NewBtree[TK btree.Ordered, TV any](ctx context.Context, db *Database, name string, t sop.Transaction, comparer btree.ComparerFunc[TK], options ...sop.StoreOptions) (btree.BtreeInterface[TK, TV], error) {
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

	// If BlobStoreBaseFolderPath is not set, use the first store folder from the database configuration.
	if opts.BlobStoreBaseFolderPath == "" && len(db.config.StoresFolders) > 0 {
		opts.BlobStoreBaseFolderPath = db.config.StoresFolders[0]
	}

	if db.config.IsReplicated() {
		if db.config.IsCassandraHybrid() {
			return incfs.NewBtreeWithReplication[TK, TV](ctx, opts, t, comparer)
		} else {
			return infs.NewBtreeWithReplication[TK, TV](ctx, opts, t, comparer)
		}
	} else {
		if db.config.IsCassandraHybrid() {
			return incfs.NewBtree[TK, TV](ctx, opts, t, comparer)
		} else {
			return infs.NewBtree[TK, TV](ctx, opts, t, comparer)
		}
	}
}

// RemoveBtree removes a B-Tree store from the database.
// This is a destructive operation and cannot be undone.
func (db *Database) RemoveBtree(ctx context.Context, name string) error {
	if db.config.IsCassandraHybrid() {
		return incfs.RemoveBtree(ctx, name, db.config.CacheType)
	}
	return infs.RemoveBtree(ctx, db.config, name)
}
