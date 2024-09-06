package aws_s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/SharedCode/sop"
)

type manageBucket struct {
	S3Client *s3.Client
	region   string
}

func NewManageBucket(s3Client *s3.Client, region string) (sop.ManageBlobStore, error) {
	if s3Client == nil {
		return nil, fmt.Errorf("s3Client parameter can't be nil")
	}
	return &manageBucket{
		S3Client: s3Client,
		region:   region,
	}, nil
}

func (mb *manageBucket) CreateBlobStore(ctx context.Context, bucketName string) error {
	_, err := mb.S3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(mb.region),
		},
	})
	if err != nil {
		return fmt.Errorf("couldn't create bucket %s in Region %s, details: %v", bucketName, mb.region, err)
	}
	return nil
}

func (mb *manageBucket) RemoveBlobStore(ctx context.Context, bucketName string) error {
	_, err := mb.S3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return fmt.Errorf("couldn't remove bucket %s, details: %v", bucketName, err)
	}
	return nil
}
