package in_red_cfs

import (
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_cfs/fs"
	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
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
	fio := fs.DefaultFileIO{}
	bs := fs.NewBlobStoreExt(toFilePath, fio, sop.NewMarshaler())
	mbsf := fs.NewManageBlobStoreFolder(fio)
	twoPT, err := in_red_ck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepositoryExt(mbsf))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}
