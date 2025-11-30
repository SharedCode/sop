package database

import (
	"context"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/vector"
	"github.com/sharedcode/sop/cache"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/redis"
)

// Database manages the storage and retrieval of AI data (Models and Vectors).
// It supports both Standalone (local) and Clustered (distributed) modes.
type Database struct {
	ctx           context.Context
	cache         sop.L2Cache
	storagePath   string
	dbType        ai.DatabaseType
	erasureConfig map[string]fs.ErasureCodingConfig
	storesFolders []string
}

// NewDatabase creates a new AI database manager.
func NewDatabase(dbType ai.DatabaseType, storagePath string) *Database {
	var c sop.L2Cache
	if dbType == ai.Clustered {
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

// SetReplicationConfig configures the replication settings for the database.
func (db *Database) SetReplicationConfig(ec map[string]fs.ErasureCodingConfig, folders []string) {
	db.erasureConfig = ec
	db.storesFolders = folders
}

// OpenModelStore returns a ModelStore for the specified name.
// It ensures the underlying storage is initialized.
func (db *Database) OpenModelStore(name string) (ai.ModelStore, error) {
	if err := os.MkdirAll(db.storagePath, 0755); err != nil {
		return nil, err
	}
	return &btreeModelStore{
		db:   db,
		name: name,
	}, nil
}

// OpenVectorStore returns a VectorStore for the specified name.
// T is the type of the payload stored with the vectors.
func OpenVectorStore[T any](ctx context.Context, db *Database, name string) ai.VectorStore[T] {
	vdb := vector.NewDatabase[T](db.dbType)
	vdb.SetStoragePath(db.storagePath)
	if len(db.erasureConfig) > 0 || len(db.storesFolders) > 0 {
		vdb.SetReplicationConfig(db.erasureConfig, db.storesFolders)
	}
	// Default usage mode is BuildOnceQueryMany, but can be changed on the returned store if needed?
	// Actually VectorDatabase has SetUsageMode.
	// If we want to expose it, we might need to add it to Database or allow configuring vdb.
	// For now, we use defaults.
	return vdb.Open(ctx, name)
}
