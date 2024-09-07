package in_red_cs3

import (
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/aws_s3"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_ck"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(s3Client *s3.Client, mode sop.TransactionMode, maxTime time.Duration, logging bool, region string) (sop.Transaction, error) {
	bs, err := NewBlobStore(s3Client, sop.NewMarshaler())
	if err != nil {
		return nil, err
	}
	mbs, err := aws_s3.NewManageBucket(s3Client, region)
	if err != nil {
		return nil, err
	}
	twoPT, err := in_red_ck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepositoryExt(mbs))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}
