// Package contains general store implementations for AWS S3 bueckt I/O.
package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/SharedCode/sop"
)

const largeObjectMinSize = 10 * 1024 * 1024

// S3 bucket wrapper, see methods below. S3Bucket implements sop.KeyValueStore interface.
type S3Bucket struct {
	S3Client *s3.Client
}

// S3 object contains the data & its ETag as generated from S3.
type S3Object struct {
	Data []byte
	ETag string
}

// NewBucketAsStore returns the S3 bucket (wrapper) instance.
func NewBucketAsStore(ctx context.Context) (*S3Bucket, error) {
	// AWS S3 SDK should be installed, configured in the host machine this code will be ran.
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't load default configuration, details: %v", err)
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	return &S3Bucket{
		S3Client: s3Client,
	}, nil
}

// Fetch bucket entry with a given name.
func (b *S3Bucket) FetchLargeObject(ctx context.Context, bucketName string, name string) (*S3Object, error) {
	downloader := manager.NewDownloader(b.S3Client, func(d *manager.Downloader) {
		d.PartSize = largeObjectMinSize
	})
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(ctx, buffer, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(name),
	})
	if err != nil {
		return nil, fmt.Errorf("can't fetch large object from bucket %s, item name %s, details: %v", bucketName, name, err.Error())
	}
	return &S3Object{
		Data: buffer.Bytes(),
	}, nil
}

// Fetch bucket entry with a given name.
func (b *S3Bucket) Fetch(ctx context.Context, bucketName string, names ...string) sop.KeyValueStoreResponse[sop.KeyValuePair[string, *S3Object]] {
	r := make([]sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]], len(names))
	var lastError error
	for i, name := range names {
		result, err := b.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(name),
		})
		if err != nil {
			r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]{
				Payload: sop.KeyValuePair[string, *S3Object]{
					Key: name,
				},
				Error: err,
			}
			lastError = err
			continue
		}
		body, err := io.ReadAll(result.Body)
		if err != nil {
			r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]{
				Payload: sop.KeyValuePair[string, *S3Object]{
					Key: name,
				},
				Error: err,
			}
			lastError = err
			continue
		}
		// Package the returned object's data and attribute(s).
		r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]{
			Payload: sop.KeyValuePair[string, *S3Object]{
				Key: name,
				Value: &S3Object{
					Data: body,
					ETag: *result.ETag,
				},
			},
		}
		result.Body.Close()
	}
	if lastError != nil {
		return sop.KeyValueStoreResponse[sop.KeyValuePair[string, *S3Object]]{
			Details: r,
			Error:   fmt.Errorf("failed to completely fetch from bucket %s, last error: %v", bucketName, lastError),
		}
	}
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, *S3Object]]{
		Details: r,
	}
}

func (b *S3Bucket) Add(ctx context.Context, bucketName string, entries ...sop.KeyValuePair[string, *S3Object]) sop.KeyValueStoreResponse[sop.KeyValuePair[string, *S3Object]] {
	r := make([]sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]], len(entries))
	var lastError error
	for i, entry := range entries {
		// Upload the large object.
		if isLargeObject(entry.Value.Data) {
			largeBuffer := bytes.NewReader(entry.Value.Data)
			uploader := manager.NewUploader(b.S3Client, func(u *manager.Uploader) {
				u.PartSize = largeObjectMinSize
			})
			_, err := uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucketName),
				Key:    aws.String(entry.Key),
				Body:   largeBuffer,
			})
			if err != nil {
				r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]{
					Payload: sop.KeyValuePair[string, *S3Object]{Key: entry.Key},
					Error:   err,
				}
				lastError = err
			}
			continue
		}
		// Upload the (not large) object.
		res, err := b.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(entry.Key),
			Body:   bytes.NewReader(entry.Value.Data),
		})
		if err != nil {
			r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]{
				Payload: sop.KeyValuePair[string, *S3Object]{
					Key: entry.Key,
				},
				Error: err,
			}
			lastError = err
			continue
		}
		// Include the ETag on the item "PutObject" result (for return).
		entry.Value.ETag = *res.ETag
		r[i] = sop.KeyValueStoreItemActionResponse[sop.KeyValuePair[string, *S3Object]]{
			Payload: sop.KeyValuePair[string, *S3Object]{
				Key:   entry.Key,
				Value: entry.Value,
			},
		}
	}
	if lastError != nil {
		return sop.KeyValueStoreResponse[sop.KeyValuePair[string, *S3Object]]{
			Details: r,
			Error:   fmt.Errorf("failed to completely add items to bucket %s, last error: %v", bucketName, lastError),
		}
	}
	return sop.KeyValueStoreResponse[sop.KeyValuePair[string, *S3Object]]{
		Details: r,
	}
}

func (b *S3Bucket) Update(ctx context.Context, bucketName string, entries ...sop.KeyValuePair[string, *S3Object]) sop.KeyValueStoreResponse[sop.KeyValuePair[string, *S3Object]] {
	return b.Add(ctx, bucketName, entries...)
}

func (b *S3Bucket) Remove(ctx context.Context, bucketName string, names ...string) sop.KeyValueStoreResponse[string] {
	var objectIds []types.ObjectIdentifier
	for _, key := range names {
		objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
	}
	output, _ := b.S3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(bucketName),
		Delete: &types.Delete{Objects: objectIds},
	})

	// Package the errors that were encountered if there is.
	if len(output.Errors) > 0 {
		r := make([]sop.KeyValueStoreItemActionResponse[string], len(names))
		lookup := make(map[string]int)
		var lastError error
		for i, name := range names {
			lookup[name] = i
		}
		for i := range output.Errors {
			name := *output.Errors[i].Key
			index := lookup[name]
			r[index] = sop.KeyValueStoreItemActionResponse[string]{
				Payload: name,
				Error:   fmt.Errorf(*output.Errors[i].Message),
			}
			lastError = r[index].Error
		}
		return sop.KeyValueStoreResponse[string]{
			Error:   fmt.Errorf("failed to completely remove items from bucket %s, last error: %v", bucketName, lastError),
			Details: r,
		}
	}
	// Delete all was successful.
	return sop.KeyValueStoreResponse[string]{}
}

func isLargeObject(data []byte) bool {
	return len(data) > largeObjectMinSize
}
