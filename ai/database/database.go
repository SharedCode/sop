package database

import (
	"context"
	"fmt"
	"os"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/model"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/btree"
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
	var to sop.TransactionOptions
	config.CopyTo(&to)
	return &Database{
		config: config,
		cache:  sop.GetL2Cache(to),
	}
}

// NewCassandraDatabase creates a new AI-enabled database manager backed by Cassandra.
func NewCassandraDatabase(config sop.DatabaseOptions) *Database {
	config, _ = database.ValidateCassandraOptions(config)
	var to sop.TransactionOptions
	config.CopyTo(&to)
	return &Database{
		config: config,
		cache:  sop.GetL2Cache(to),
	}
}

// Cache returns the L2 cache used by the database.
func (db *Database) Cache() sop.L2Cache {
	return db.cache
}

// Options returns the database options.
func (db *Database) Options() sop.DatabaseOptions {
	return db.config
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
func (db *Database) CacheType() sop.L2CacheType {
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

// OpenBtree opens a general purpose B-Tree store with string keys and any values.
func (db *Database) OpenBtree(ctx context.Context, name string, t sop.Transaction) (btree.BtreeInterface[string, any], error) {
	if db.StoragePath() != "" {
		if err := os.MkdirAll(db.StoragePath(), 0755); err != nil {
			return nil, err
		}
	}
	return database.OpenBtree[string, any](ctx, db.config, name, t, nil)
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

// RemoveModelStore removes the model store and its underlying B-Tree.
func (db *Database) RemoveModelStore(ctx context.Context, name string) error {
	return database.RemoveBtree(ctx, db.config, fmt.Sprintf("%s_models", name))
}

// RemoveVectorStore removes the vector store and its underlying B-Trees.
func (db *Database) RemoveVectorStore(ctx context.Context, name string) error {
	suffixes := []string{
		"_sys_config",
		"_tmp_vecs",
		"_data",
	}

	var lastErr error
	succeeded := false

	for _, suffix := range suffixes {
		if err := database.RemoveBtree(ctx, db.config, fmt.Sprintf("%s%s", name, suffix)); err != nil {
			lastErr = err
		}
	}

	// Also try to remove versioned tables.
	// We use an internal transaction to peek at the current version.
	trans, err := db.BeginTransaction(ctx, sop.ForReading)
	if err == nil {
		vs, err := db.OpenVectorStore(ctx, name, trans, vector.Config{})
		if err == nil {
			version, _ := vs.Version(ctx)

			f := func(versionSuffix string) int {
				i := 0
				if err := database.RemoveBtree(ctx, db.config, fmt.Sprintf("%s%s%s", name, "_lku", versionSuffix)); err == nil {
					i++
				} else {
					lastErr = err
				}
				if err := database.RemoveBtree(ctx, db.config, fmt.Sprintf("%s%s%s", name, "_centroids", versionSuffix)); err == nil {
					i++
				} else {
					lastErr = err
				}
				if err := database.RemoveBtree(ctx, db.config, fmt.Sprintf("%s%s%s", name, "_vecs", versionSuffix)); err == nil {
					i++
				} else {
					lastErr = err
				}

				log.Debug(fmt.Sprintf("version suffix %s, success count: %d", versionSuffix, i))

				return i
			}

			// Delete the unversioned current set.
			if f("") == 3 {
				succeeded = true
			}

			// Remove the current version vector components.
			versionSuffix := fmt.Sprintf("_%d", version)
			if f(versionSuffix) == 3 {
				succeeded = true
			}

			// Remove the previous version vector components, if there is.
			if version > 0 {
				versionSuffix = fmt.Sprintf("_%d", version-1)
				if f(versionSuffix) == 3 {
					succeeded = true
				}
			}

			// Remove the next version vector components, if there is.
			versionSuffix = fmt.Sprintf("_%d", version+1)
			if f(versionSuffix) == 3 {
				succeeded = true
			}
		}
		// We only read, so rollback is fine/preferred to release locks immediately.
		_ = trans.Rollback(ctx)
	}

	// We know we deleted a set of VectorStore component files, return nil to denote success.
	if succeeded {
		lastErr = nil
	}

	return lastErr
}

// RemoveSearch removes the search index and its underlying B-Trees.
func (db *Database) RemoveSearch(ctx context.Context, name string) error {
	var lastErr error
	remove := func(n string) {
		if err := database.RemoveBtree(ctx, db.config, n); err != nil {
			lastErr = err
		}
	}

	suffixes := []string{
		"_postings",
		"_term_stats",
		"_doc_stats",
		"_global",
	}

	for _, suffix := range suffixes {
		remove(fmt.Sprintf("%s%s", name, suffix))
	}
	return lastErr
}
