package in_red_cs3

import (
	"context"
	"time"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/in_red_ck"
	cas "github.com/SharedCode/sop/cassandra"
	"github.com/SharedCode/sop/in_red_cs3/s3"
	red_s3 "github.com/SharedCode/sop/red_s3/s3"
)

// NewTransaction is a convenience function to create an enduser facing transaction object that wraps the two phase commit transaction.
func NewTransaction(ctx context.Context, mode sop.TransactionMode, maxTime time.Duration, logging bool, region string) (sop.Transaction, error) {
	bs, err := s3.NewBlobStore(ctx, sop.NewMarshaler())
	if err != nil {
		return nil, err
	}
	mbs := red_s3.NewManageBucket(bs.BucketAsStore.(*red_s3.S3Bucket).S3Client, region)
	twoPT, err := in_red_ck.NewTwoPhaseCommitTransaction(mode, maxTime, logging, bs, cas.NewStoreRepositoryExt(mbs))
	if err != nil {
		return nil, err
	}
	return sop.NewTransaction(mode, twoPT, maxTime, logging)
}
