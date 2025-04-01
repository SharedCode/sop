package in_red_cfs

import (
	"fmt"
	"time"

	log "log/slog"

	"github.com/SharedCode/sop"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cfs/fs"
	"github.com/SharedCode/sop/in_red_ck"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	return NewTransactionExt(fs.DefaultToFilePath, mode, maxTime, logging)
}

// NewTransactionExt is synonymous to NewTransasction but allows you to specify a custom 'ToFilePath' functionality
// that can allow you to implement your logic to partition the blob files into different storage drives for example, of your liking.
// Perhaps based on the first hex letter of the GUID and/or in combination of the blob store's base folder path.
//
// See SOP FileSystem(sop/in_red-cfs/fs) package's DefaultToFilePath function for an example how to implement one.
func NewTransactionExt(toFilePath fs.ToFilePathFunc, mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	fio := fs.NewDefaultFileIO(toFilePath)
	bs := fs.NewBlobStore(fio)
	mbsf := fs.NewManageBlobStoreFolder(fio)
	twoPT, err := in_red_ck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepositoryExt(mbsf))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}

// Create a transaction that supports Erasure Coding file IO.
func NewTransactionWithEC(mode sop.TransactionMode, maxTime time.Duration, logging bool, erasureConfig map[string]fs.ErasureCodingConfig) (sop.Transaction, error) {
	if erasureConfig == nil {
		erasureConfig = fs.GetGlobalErasureConfig()
		if erasureConfig == nil {
			return nil, fmt.Errorf("erasureConfig can't be nil")
		}
	}
	fio := fs.NewDefaultFileIO(fs.DefaultToFilePath)
	for _,ecc := range erasureConfig {
		if mode != sop.ForWriting && ecc.RepairCorruptedShards {
			log.Warn("erasureConfig.RepairCorruptedShards can only be true if transaction is in ForWriting mode, setting it to false")
			ecc.RepairCorruptedShards = false
		}
	}
	bs, err := fs.NewBlobStoreExt(fio, erasureConfig)
	if err != nil {
		return nil, err
	}
	mbsf := fs.NewManageBlobStoreFolder(fio)
	twoPT, err := in_red_ck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepositoryExt(mbsf))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}
