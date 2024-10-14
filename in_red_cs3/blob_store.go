package in_red_cs3

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/aws_s3"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// BlobStore has no caching built in because blobs are huge, caller code can apply caching on top of it.
type blobStore struct {
	BucketAsStore  sop.KeyValueStore[string, *aws_s3.S3Object]
	isLargeObjects bool
}

// NewBlobStore instantiates a new blobstore for AWS S3 bucket storage.
func NewBlobStore(s3Client *s3.Client) (*blobStore, error) {
	cb, err := aws_s3.NewBucketAsStore(s3Client)
	if err != nil {
		return nil, err
	}
	return &blobStore{
		BucketAsStore: cb,
	}, err
}

func (b *blobStore) GetOne(ctx context.Context, blobFilePath string, blobID sop.UUID) ([]byte, error) {
	if b.isLargeObjects {
		s3o, err := b.BucketAsStore.FetchLargeObject(ctx, blobFilePath, blobID.String())
		if err != nil {
			return nil, err
		}
		return s3o.Data, nil
	}
	s3o := b.BucketAsStore.Fetch(ctx, blobFilePath, blobID.String())
	if s3o.Error != nil {
		return nil, s3o.Error
	}
	return s3o.Details[0].Payload.Value.Data, nil
}

func (b *blobStore) Add(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := sop.BlobMarshaler.Marshal(blob.Value)
			if err != nil {
				return err
			}
			res := b.BucketAsStore.Add(ctx, storeBlobs.BlobTable, sop.KeyValuePair[string, *aws_s3.S3Object]{
				Key:   blob.Key.String(),
				Value: &aws_s3.S3Object{Data: ba},
			})
			if res.Error != nil {
				return res.Error
			}
		}
	}
	return nil
}

func (b *blobStore) Update(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, []byte]]) error {
	return b.Add(ctx, storesblobs...)
}

func (b *blobStore) Remove(ctx context.Context, storesBlobsIDs ...sop.BlobsPayload[sop.UUID]) error {
	for _, storeBlobIDs := range storesBlobsIDs {
		s3okeys := make([]string, len(storeBlobIDs.Blobs))
		for i, blobID := range storeBlobIDs.Blobs {
			s3okeys[i] = blobID.String()
		}
		res := b.BucketAsStore.Remove(ctx, storeBlobIDs.BlobTable, s3okeys...)
		if res.Error != nil {
			return res.Error
		}
	}
	return nil
}
