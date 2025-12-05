package database

import (
	"context"
	"fmt"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/infs"
)

// DatabaseType defines the deployment mode of the database.
type DatabaseType int

const (
	// Standalone mode uses in-memory caching and local file storage.
	// Suitable for single-node deployments.
	Standalone DatabaseType = iota
	// Clustered mode uses distributed caching (e.g., Redis) and shared storage.
	// Suitable for multi-node deployments.
	Clustered
)

// Database manages the storage and retrieval of data (B-Trees).
// It supports both Standalone (local) and Clustered (distributed) modes.
type Database struct {
	ctx           context.Context
	cache         sop.L2Cache
	storagePath   string
	dbType        DatabaseType
	erasureConfig map[string]fs.ErasureCodingConfig
	storesFolders []string
}

// NewDatabase creates a new database manager.
func NewDatabase(dbType DatabaseType, storagePath string) *Database {
	var c sop.L2Cache
	if dbType == Clustered {
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
		c = cache.NewInMemoryCache()
	}

	return &Database{
		ctx:         context.Background(),
		cache:       c,
		storagePath: storagePath,
		dbType:      dbType,
	}
}

// Cache returns the L2 cache used by the database.
func (db *Database) Cache() sop.L2Cache {
	return db.cache
}

// StoragePath returns the base storage path.
func (db *Database) StoragePath() string {
	return db.storagePath
}

// ErasureConfig returns the erasure coding configuration.
func (db *Database) ErasureConfig() map[string]fs.ErasureCodingConfig {
	return db.erasureConfig
}

// StoresFolders returns the list of storage folders (for replication).
func (db *Database) StoresFolders() []string {
	return db.storesFolders
}

// SetReplicationConfig configures the replication settings for the database.
func (db *Database) SetReplicationConfig(ec map[string]fs.ErasureCodingConfig, folders []string) {
	db.erasureConfig = ec
	db.storesFolders = folders
}

// BeginTransaction starts a new transaction.
func (db *Database) BeginTransaction(ctx context.Context, mode sop.TransactionMode, options ...infs.TransationOptionsWithReplication) (sop.Transaction, error) {
	var opts infs.TransationOptionsWithReplication
	if len(options) > 0 {
		opts = options[0]
	}

	// Merge defaults if fields are missing
	if len(opts.StoresBaseFolders) == 0 {
		if len(db.storesFolders) > 0 {
			opts.StoresBaseFolders = db.storesFolders
		} else if db.storagePath != "" {
			opts.StoresBaseFolders = []string{db.storagePath}
		}
	}
	if len(opts.ErasureConfig) == 0 && len(db.erasureConfig) > 0 {
		opts.ErasureConfig = db.erasureConfig
	}
	if opts.Cache == nil {
		opts.Cache = db.cache
	}
	// Always override mode with the explicit argument.
	opts.Mode = mode

	if len(opts.ErasureConfig) > 0 || len(opts.StoresBaseFolders) > 1 {
		// Use Replication
		// We need to re-validate or re-construct because NewTransactionOptionsWithReplication does validation.
		// But we already have the struct. We can just call NewTransactionWithReplication.
		// However, NewTransactionWithReplication expects valid input.
		// Let's try to use the helper to validate/fill defaults if needed?
		// NewTransactionOptionsWithReplication takes args and returns struct.
		// We already have struct.
		// We can just call NewTransactionWithReplication directly.
		t, err := infs.NewTransactionWithReplication(ctx, opts)
		if err != nil {
			return nil, err
		}
		if err := t.Begin(ctx); err != nil {
			return nil, err
		}
		return t, nil
	}

	// Standard
	// Map opts to standard options
	stdOpts := infs.TransationOptions{
		Mode:                 opts.Mode,
		MaxTime:              opts.MaxTime,
		RegistryHashModValue: opts.RegistryHashModValue,
		StoresBaseFolder:     opts.StoresBaseFolders[0],
		Cache:                opts.Cache,
	}
	t, err := infs.NewTransaction(ctx, stdOpts)
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
	if err := os.MkdirAll(db.storagePath, 0755); err != nil {
		return nil, err
	}
	// We use string keys and any values for a generic store, but users can use specific types if they use infs.directly.
	// For the Database wrapper, we provide a sensible default or we could make this generic if Go allowed methods to have type parameters (it does).
	// However, since Database is a struct, we can't easily make this method generic for the return type without the struct being generic.
	// For now, we'll expose a string/any B-Tree.
	return infs.OpenBtree[any, any](ctx, name, t, nil)
}

// NewBtree creates a new general purpose B-Tree store.
func (db *Database) NewBtree(ctx context.Context, name string, t sop.Transaction, options ...sop.StoreOptions) (btree.BtreeInterface[any, any], error) {
	if err := os.MkdirAll(db.storagePath, 0755); err != nil {
		return nil, err
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
	return infs.NewBtree[any, any](ctx, opts, t, nil)
}
