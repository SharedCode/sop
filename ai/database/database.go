package database

import (
	"context"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/model"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/search"
)

// Database extends the core sop.Database with AI capabilities.
type Database struct {
	*database.Database
}

// NewDatabase creates a new AI-enabled database manager.
// It wraps the core database.NewDatabase.
func NewDatabase(config sop.DatabaseOptions) *Database {
	return &Database{
		Database: database.NewDatabase(config),
	}
}

// NewCassandraDatabase creates a new AI-enabled database manager backed by Cassandra.
func NewCassandraDatabase(config sop.DatabaseOptions) *Database {
	return &Database{
		Database: database.NewCassandraDatabase(config),
	}
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
