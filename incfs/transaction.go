package incfs

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/sharedcode/sop"
	cas "github.com/sharedcode/sop/adapters/cassandra"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/internal/inredck"
)

// NewTransaction is a convenience function to create an end-user facing transaction object that wraps the two-phase commit transaction.
func NewTransaction(ctx context.Context, config sop.TransactionOptions) (sop.Transaction, error) {
	fio := fs.NewFileIO()
	toFilePath := fs.DefaultToFilePath
	if len(config.StoresFolders) > 0 {
		toFilePath = func(basePath string, id sop.UUID) string {
			return fs.DefaultToFilePath(filepath.Join(config.StoresFolders[0], basePath), id)
		}
	}
	bs := fs.NewBlobStore(toFilePath, fio)
	mbsf := fs.NewManageStoreFolder(fio)

	var conn *cas.Connection
	var err error
	var needsClose bool
	if config.IsCassandraHybrid() {
		conn, err = cas.GetConnection(config.Keyspace)
		if err != nil {
			return nil, err
		}
		needsClose = true
	}

	if config.CacheType == sop.NoCache {
		config.CacheType = sop.Redis
	}
	cache := sop.GetL2Cache(config.CacheType)
	twoPT, err := inredck.NewTwoPhaseCommitTransaction(config.Mode, config.MaxTime, bs, cas.NewStoreRepository(mbsf, conn, cache), conn)
	if err != nil {
		return nil, err
	}
	if needsClose {
		// Close the Cassandra connection.
		twoPT.OnCommit(func(context.Context) error {
			conn.Close()
			return nil
		})
	}
	return sop.NewTransaction(config.Mode, twoPT)
}

// NewTransactionWithReplication creates a transaction that supports Erasure Coding file IO.
func NewTransactionWithReplication(ctx context.Context, config sop.TransactionOptions) (sop.Transaction, error) {
	if config.ErasureConfig == nil {
		config.ErasureConfig = fs.GetGlobalErasureConfig()
		if config.ErasureConfig == nil {
			return nil, fmt.Errorf("ErasureConfig can't be nil")
		}
	}
	fio := fs.NewFileIO()
	bs, err := fs.NewBlobStoreWithEC(fs.DefaultToFilePath, fio, config.ErasureConfig)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)

	var conn *cas.Connection
	var needsClose bool
	if config.IsCassandraHybrid() {
		conn, err = cas.GetConnection(config.Keyspace)
		if err != nil {
			return nil, err
		}
		needsClose = true
	}

	if config.CacheType == sop.NoCache {
		config.CacheType = sop.Redis
	}
	cache := sop.GetL2Cache(config.CacheType)
	twoPT, err := inredck.NewTwoPhaseCommitTransaction(config.Mode, config.MaxTime, bs, cas.NewStoreRepository(mbsf, conn, cache), conn)
	if err != nil {
		return nil, err
	}
	// Close the Cassandra connection.
	if needsClose {
		twoPT.OnCommit(func(context.Context) error {
			conn.Close()
			return nil
		})
	}
	return sop.NewTransaction(config.Mode, twoPT)
}
