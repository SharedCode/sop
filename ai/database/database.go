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
func NewDatabase(dbType database.DatabaseType, storagePath string) *Database {
	return &Database{
		Database: database.NewDatabase(dbType, storagePath),
	}
}

// OpenModelStore opens a ModelStore for the specified name using the provided transaction.
func (db *Database) OpenModelStore(ctx context.Context, name string, t sop.Transaction) (ai.ModelStore, error) {
	if err := os.MkdirAll(db.StoragePath(), 0755); err != nil {
		return nil, err
	}
	return model.New(name, t), nil
}

// OpenVectorStore opens a vector store with map[string]any payload.
func (db *Database) OpenVectorStore(ctx context.Context, name string, t sop.Transaction, cfg vector.Config) (ai.VectorStore[map[string]any], error) {
	if err := os.MkdirAll(db.StoragePath(), 0755); err != nil {
		return nil, err
	}
	// Populate replication config if available and not set in cfg
	// We access the core database's configuration via the getters we added.
	if len(cfg.StoresFolders) == 0 && len(db.StoresFolders()) > 0 {
		cfg.StoresFolders = db.StoresFolders()
	}
	if len(cfg.ErasureConfig) == 0 && len(db.ErasureConfig()) > 0 {
		cfg.ErasureConfig = db.ErasureConfig()
	}
	if cfg.StoragePath == "" {
		cfg.StoragePath = db.StoragePath()
	}
	if cfg.Cache == nil {
		cfg.Cache = db.Cache()
	}
	return vector.Open[map[string]any](ctx, t, name, cfg)
}

// OpenSearch opens a text search index for the specified name using the provided transaction.
func (db *Database) OpenSearch(ctx context.Context, name string, t sop.Transaction) (*search.Index, error) {
	if err := os.MkdirAll(db.StoragePath(), 0755); err != nil {
		return nil, err
	}
	return search.NewIndex(ctx, t, name)
}
