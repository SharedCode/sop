package database

import (
	"context"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/model"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/inredfs"
	"github.com/sharedcode/sop/redis"
)

// DatabaseType defines the deployment mode of the vector database.
type DatabaseType int

const (
	// Standalone mode uses in-memory caching and local file storage.
	// Suitable for single-node deployments.
	Standalone DatabaseType = iota
	// Clustered mode uses distributed caching (e.g., Redis) and shared storage.
	// Suitable for multi-node deployments.
	Clustered
)

// Database manages the storage and retrieval of data (B-Trees, Models, and Vectors).
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
		c = redis.NewClient()
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

// SetReplicationConfig configures the replication settings for the database.
func (db *Database) SetReplicationConfig(ec map[string]fs.ErasureCodingConfig, folders []string) {
	db.erasureConfig = ec
	db.storesFolders = folders
}

// BeginTransaction starts a new transaction.
func (db *Database) BeginTransaction(ctx context.Context, mode sop.TransactionMode, options ...inredfs.TransationOptionsWithReplication) (sop.Transaction, error) {
	var opts inredfs.TransationOptionsWithReplication
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
		t, err := inredfs.NewTransactionWithReplication(ctx, opts)
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
	stdOpts := inredfs.TransationOptions{
		Mode:                 opts.Mode,
		MaxTime:              opts.MaxTime,
		RegistryHashModValue: opts.RegistryHashModValue,
		StoresBaseFolder:     opts.StoresBaseFolders[0],
		Cache:                opts.Cache,
	}
	t, err := inredfs.NewTransaction(ctx, stdOpts)
	if err != nil {
		return nil, err
	}
	if err := t.Begin(ctx); err != nil {
		return nil, err
	}
	return t, nil
}

// OpenModelStore opens a ModelStore for the specified name using the provided transaction.
func (db *Database) OpenModelStore(ctx context.Context, name string, t sop.Transaction) (ai.ModelStore, error) {
	if err := os.MkdirAll(db.storagePath, 0755); err != nil {
		return nil, err
	}
	return model.New(name, t), nil
}

// OpenBtree opens a general purpose B-Tree store.
// This allows the Database to manage standard Key-Value stores alongside AI stores.
func (db *Database) OpenBtree(ctx context.Context, name string, t sop.Transaction) (btree.BtreeInterface[any, any], error) {
	if err := os.MkdirAll(db.storagePath, 0755); err != nil {
		return nil, err
	}
	// We use string keys and any values for a generic store, but users can use specific types if they use inredfs directly.
	// For the Database wrapper, we provide a sensible default or we could make this generic if Go allowed methods to have type parameters (it does).
	// However, since Database is a struct, we can't easily make this method generic for the return type without the struct being generic.
	// For now, we'll expose a string/any B-Tree.
	return inredfs.OpenBtree[any, any](ctx, name, t, nil)
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
	return inredfs.NewBtree[any, any](ctx, opts, t, nil)
}

// OpenVectorStore opens a vector store with map[string]any payload.
func (db *Database) OpenVectorStore(ctx context.Context, name string, t sop.Transaction, cfg vector.Config) (ai.VectorStore[map[string]any], error) {
	if err := os.MkdirAll(db.storagePath, 0755); err != nil {
		return nil, err
	}
	// Populate replication config if available and not set in cfg
	if len(cfg.StoresFolders) == 0 && len(db.storesFolders) > 0 {
		cfg.StoresFolders = db.storesFolders
	}
	if len(cfg.ErasureConfig) == 0 && len(db.erasureConfig) > 0 {
		cfg.ErasureConfig = db.erasureConfig
	}
	if cfg.StoragePath == "" {
		cfg.StoragePath = db.storagePath
	}
	if cfg.Cache == nil {
		cfg.Cache = db.cache
	}
	return vector.Open[map[string]any](ctx, t, name, cfg)
}
