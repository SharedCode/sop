package in_red_cfs

import (
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_ck"
	"github.com/SharedCode/sop/in_red_cfs/fs"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(mode sop.TransactionMode, maxTime time.Duration, logging bool) (sop.Transaction, error) {
	twoPT, err :=  in_red_ck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, fs.NewBlobStoreUsingDefaults())
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}
