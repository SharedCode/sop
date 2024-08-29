package in_red_cfs

import (
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_ck"
	"github.com/SharedCode/sop/in_red_cfs/fs"
	cas "github.com/SharedCode/sop/in_red_ck/cassandra"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	fio := fs.DefaultFileIO{}
	bs := fs.NewBlobStore(fs.DefaultToFilePath, fio, sop.NewMarshaler())
	mbsf := fs.NewManageBlobStoreFolder(fio)
	twoPT, err :=  in_red_ck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepository(mbsf))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}
