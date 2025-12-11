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

// ValidateOptions validates and prepares the database options.
// It infers CacheType if not set.
func ValidateOptions(config sop.DatabaseOptions) (sop.DatabaseOptions, error) {
	// If CacheType is not set, infer it from Type.
	if config.CacheType == sop.NoCache {
		if config.Type == sop.Clustered {
			config.CacheType = sop.Redis
		} else {
			config.CacheType = sop.InMemory
		}
	}

	if config.CacheType == sop.Redis {
		// Check if Redis adapter is registered.
		c := sop.GetL2Cache(sop.Redis)
		if c == nil {
			// Auto register InMemory L2 Cache.
			sop.RegisterL2CacheFactory(sop.InMemory, cache.NewL2InMemoryCache)
		}
	} else if config.CacheType == sop.InMemory {
		c := sop.GetL2Cache(sop.InMemory)
		if c == nil {
			// Auto register InMemory L2 Cache.
			sop.RegisterL2CacheFactory(sop.InMemory, cache.NewL2InMemoryCache)
		}
	}

	return config, nil
}

// ValidateCassandraOptions validates and prepares the database options for Cassandra.
func ValidateCassandraOptions(config sop.DatabaseOptions) (sop.DatabaseOptions, error) {
	if config.Keyspace == "" {
		return config, fmt.Errorf("Cassandra mode requires Keyspace to be set")
	}
	c := sop.GetL2Cache(sop.Redis)
	if c == nil {
		return config, fmt.Errorf("Cassandra mode requires Redis adapter. Please import 'github.com/sharedcode/sop/adapters/redis'")
	}
	config.CacheType = sop.Redis
	return config, nil
}

// BeginTransaction starts a new transaction.
func BeginTransaction(ctx context.Context, config sop.DatabaseOptions, mode sop.TransactionMode, maxTime ...time.Duration) (sop.Transaction, error) {
	config, _ = ValidateOptions(config)
	var mt time.Duration
	if len(maxTime) > 0 {
		mt = maxTime[0]
	} else {
		mt = 15 * time.Minute
	}

	var opts sop.TransactionOptions
	// Merge DatabaseOptions into TransactionOptions
	config.CopyTo(&opts)
	opts.Mode = mode
	opts.MaxTime = mt

	var t sop.Transaction
	var err error

	if opts.IsReplicated() {
		if opts.IsCassandraHybrid() {
			t, err = incfs.NewTransactionWithReplication(ctx, opts)
		} else {
			t, err = infs.NewTransactionWithReplication(ctx, opts)
		}
	} else {
		if config.IsCassandraHybrid() {
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
func OpenBtree[TK btree.Ordered, TV any](ctx context.Context, config sop.DatabaseOptions, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (btree.BtreeInterface[TK, TV], error) {
	if config.IsReplicated() {
		if config.IsCassandraHybrid() {
			return incfs.OpenBtree[TK, TV](ctx, name, t, comparer)
		} else {
			return infs.OpenBtreeWithReplication[TK, TV](ctx, name, t, comparer)
		}
	} else {
		if config.IsCassandraHybrid() {
			return incfs.OpenBtree[TK, TV](ctx, name, t, comparer)
		} else {
			return infs.OpenBtree[TK, TV](ctx, name, t, comparer)
		}
	}
}

// NewBtree creates a new general purpose B-Tree store.
func NewBtree[TK btree.Ordered, TV any](ctx context.Context, config sop.DatabaseOptions, name string, t sop.Transaction, comparer btree.ComparerFunc[TK], options ...sop.StoreOptions) (btree.BtreeInterface[TK, TV], error) {
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
	if opts.BlobStoreBaseFolderPath == "" && len(config.StoresFolders) > 0 {
		opts.BlobStoreBaseFolderPath = config.StoresFolders[0]
	}

	if config.IsReplicated() {
		if config.IsCassandraHybrid() {
			return incfs.NewBtreeWithReplication[TK, TV](ctx, opts, t, comparer)
		} else {
			return infs.NewBtreeWithReplication[TK, TV](ctx, opts, t, comparer)
		}
	} else {
		if config.IsCassandraHybrid() {
			return incfs.NewBtree[TK, TV](ctx, opts, t, comparer)
		} else {
			return infs.NewBtree[TK, TV](ctx, opts, t, comparer)
		}
	}
}

// RemoveBtree removes a B-Tree store from the database.
// This is a destructive operation and cannot be undone.
func RemoveBtree(ctx context.Context, config sop.DatabaseOptions, name string) error {
	if config.IsCassandraHybrid() {
		return incfs.RemoveBtree(ctx, name, config.CacheType)
	}
	return infs.RemoveBtree(ctx, name, config.StoresFolders, config.CacheType)
}

// ReinstateFailedDrives asks the replication tracker to reinstate failed passive targets.
// This API is only applicable for infs backend.
func ReinstateFailedDrives(ctx context.Context, config sop.DatabaseOptions) error {
	if config.IsCassandraHybrid() {
		return fmt.Errorf("ReinstateFailedDrives only apply for infs, a pure File System based backend")
	}
	return infs.ReinstateFailedDrives(ctx, config.StoresFolders, config.CacheType)
}
