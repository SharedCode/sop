package incfs

import (
	"fmt"
	"time"

	"github.com/sharedcode/sop"
	cas "github.com/sharedcode/sop/adapters/cassandra"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/internal/inredck"
)

// NewTransaction is a convenience function to create an end-user facing transaction object that wraps the two-phase commit transaction.
func NewTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	return NewTransactionExt(fs.DefaultToFilePath, mode, maxTime, logging)
}

// NewTransactionExt is synonymous to NewTransaction but allows you to specify a custom 'ToFilePath' functionality
// that can allow you to implement your logic to partition the blob files into different storage drives for example, of your liking.
// Perhaps based on the first hex letter of the GUID and/or in combination of the blob store's base folder path.
//
// See SOP FileSystem(sop/fs) package's DefaultToFilePath function for an example how to implement one.
func NewTransactionExt(toFilePath fs.ToFilePathFunc, mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	fio := fs.NewFileIO()
	bs := fs.NewBlobStore(fs.DefaultToFilePath, fio)
	mbsf := fs.NewManageStoreFolder(fio)
	twoPT, err := inredck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepository(mbsf))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, logging)
}

// NewTransactionWithReplication creates a transaction that supports Erasure Coding file IO.
func NewTransactionWithReplication(mode sop.TransactionMode, maxTime time.Duration, logging bool, erasureConfig map[string]fs.ErasureCodingConfig) (sop.Transaction, error) {
	if erasureConfig == nil {
		erasureConfig = fs.GetGlobalErasureConfig()
		if erasureConfig == nil {
			return nil, fmt.Errorf("erasureConfig can't be nil")
		}
	}
	fio := fs.NewFileIO()
	bs, err := fs.NewBlobStoreWithEC(fs.DefaultToFilePath, fio, erasureConfig)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageStoreFolder(fio)
	twoPT, err := inredck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepository(mbsf))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, logging)
}
