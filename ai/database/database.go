package database

import (
	"context"
	"os"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/model"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/search"
)

// Database extends the core sop.Database with AI capabilities.
type Database struct {
	config sop.DatabaseOptions
	cache  sop.L2Cache
}

// NewDatabase creates a new AI-enabled database manager.
func NewDatabase(config sop.DatabaseOptions) *Database {
	config, _ = database.ValidateOptions(config)
	return &Database{
		config: config,
		cache:  sop.NewCacheClientByType(config.CacheType),
	}
}

// NewCassandraDatabase creates a new AI-enabled database manager backed by Cassandra.
func NewCassandraDatabase(config sop.DatabaseOptions) *Database {
	config, _ = database.ValidateCassandraOptions(config)
	return &Database{
		config: config,
		cache:  sop.NewCacheClientByType(config.CacheType),
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
	return database.BeginTransaction(ctx, db.config, mode, maxTime...)
}

// Config returns the database configuration.
func (db *Database) Config() sop.DatabaseOptions {
	return db.config
}

// OpenModelStore opens a ModelStore for the specified name using the provided transaction.
func (db *Database) OpenModelStore(ctx context.Context, name string, t sop.Transaction) (ai.ModelStore, error) {
	if db.StoragePath() != "" {
		if err := os.MkdirAll(db.StoragePath(), 0755); err != nil {
			return nil, err
		}
	}
	return model.New(name, t), nil
}

// OpenVectorStore opens a vector store with map[string]any payload.
func (db *Database) OpenVectorStore(ctx context.Context, name string, t sop.Transaction, cfg vector.Config) (ai.VectorStore[map[string]any], error) {
	if db.StoragePath() != "" {
		if err := os.MkdirAll(db.StoragePath(), 0755); err != nil {
			return nil, err
		}
	}
	// Populate replication config if available and not set in cfg
	// We access the core database's configuration via the getters we added.
	if len(cfg.TransactionOptions.StoresFolders) == 0 && len(db.StoresFolders()) > 0 {
		cfg.TransactionOptions.StoresFolders = db.StoresFolders()
	}
	if len(cfg.TransactionOptions.ErasureConfig) == 0 && len(db.ErasureConfig()) > 0 {
		cfg.TransactionOptions.ErasureConfig = db.ErasureConfig()
	}
	if cfg.TransactionOptions.CacheType == 0 {
		cfg.TransactionOptions.CacheType = db.CacheType()
	}
	if cfg.Cache == nil {
		cfg.Cache = db.Cache()
	}
	return vector.Open[map[string]any](ctx, t, name, cfg)
}

// OpenSearch opens a text search index for the specified name using the provided transaction.
func (db *Database) OpenSearch(ctx context.Context, name string, t sop.Transaction) (*search.Index, error) {
	if db.StoragePath() != "" {
		if err := os.MkdirAll(db.StoragePath(), 0755); err != nil {
			return nil, err
		}
	}
	return search.NewIndex(ctx, t, name)
}
