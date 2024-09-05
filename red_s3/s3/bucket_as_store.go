package s3

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/SharedCode/sop"
)

const largeObjectMinSize = 10 * 1024 * 1024

type s3Bucket struct {
	bucketName string
	S3Client *s3.Client
}

type s3Object struct {
	Data []byte
	ETag string
}

func NewBucket(ctx context.Context, bucketName string) (sop.KeyValueStore[string, *s3Object], error) {
	b, err := newBucket(ctx, bucketName)
	return b, err
}

func newBucket(ctx context.Context, bucketName string) (*s3Bucket, error) {
	// AWS S3 SDK should be installed, configured in the host machine this code will be ran.
	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Println("Couldn't load default configuration. Have you set up your AWS account?")
		fmt.Println(err)
		return nil, nil
	}
	s3Client := s3.NewFromConfig(sdkConfig)
	return &s3Bucket{
		bucketName: bucketName,
		S3Client: s3Client,
	}, nil
}

// Fetch bucket entry with a given name.
func (b *s3Bucket)FetchLargeObject(ctx context.Context, name string) (*s3Object, error) {
	downloader := manager.NewDownloader(b.S3Client, func(d *manager.Downloader) {
		d.PartSize = largeObjectMinSize
	})
	buffer := manager.NewWriteAtBuffer([]byte{})
	_, err := downloader.Download(ctx, buffer, &s3.GetObjectInput{
		Bucket: aws.String(b.bucketName),
		Key:    aws.String(name),
	})
	if err != nil {
		return nil, err
	}
	return &s3Object{
		Data: buffer.Bytes(),
	}, nil
}

// Fetch bucket entry with a given name.
func (b *s3Bucket)Fetch(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
	r := make([]sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]], len(names))
	for i, name := range names {
		result, err := b.S3Client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(b.bucketName),
			Key:    aws.String(name),
		})
		if err != nil {
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
				Payload: sop.KeyValuePair[string, *s3Object]{
					Key: name,
				},
				Error: err,
			}
			continue
		}
		body, err := io.ReadAll(result.Body)
		if err != nil {
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
				Payload: sop.KeyValuePair[string, *s3Object]{
					Key: name,
				},
				Error: err,
			}
			continue
		}
		// Package the returned object's data and attribute(s).
		r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
			Payload: sop.KeyValuePair[string, *s3Object]{
				Key: name,
				Value: &s3Object{
					Data: body,
					ETag: *result.ETag,
				},
			},
		}
		result.Body.Close()	
	}

	return r
}

func (b *s3Bucket)Add(ctx context.Context, entries ...sop.KeyValuePair[string, *s3Object]) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
	r := make([]sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]], len(entries))
	for i, entry := range entries {
		// Upload the large object.
		if isLargeObject(entry.Value.Data) {
			largeBuffer := bytes.NewReader(entry.Value.Data)
			uploader := manager.NewUploader(b.S3Client, func(u *manager.Uploader) {
				u.PartSize = largeObjectMinSize
			})
			_, err := uploader.Upload(ctx, &s3.PutObjectInput{
				Bucket: aws.String(b.bucketName),
				Key:    aws.String(entry.Key),
				Body:   largeBuffer,
			})
			if err != nil {
				r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
					Payload: sop.KeyValuePair[string,*s3Object]{Key: entry.Key},
					Error: err,
				}
			}
			continue
		}
		// Upload the (not large) object.
		res, err := b.S3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(b.bucketName),
			Key:    aws.String(entry.Key),
			Body:   bytes.NewReader(entry.Value.Data),
		})
		if err != nil {
			r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
				Payload: sop.KeyValuePair[string, *s3Object]{
					Key: entry.Key,
				},
				Error: err,
			}
			continue
		}
		// Include the ETag on the item "PutObject" result (for return).
		entry.Value.ETag = *res.ETag
		r[i] = sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
			Payload: sop.KeyValuePair[string, *s3Object]{
				Key: entry.Key,
				Value: entry.Value,
			},
		}
	}
	return r
}

func (b *s3Bucket)Update(ctx context.Context, entries ...sop.KeyValuePair[string, *s3Object]) []sop.KeyValueStoreResponse[sop.KeyValuePair[string, *s3Object]] {
	return b.Add(ctx, entries...)
}

func (b *s3Bucket)Remove(ctx context.Context, names ...string) []sop.KeyValueStoreResponse[string] {
	var objectIds []types.ObjectIdentifier
	for _, key := range names {
		objectIds = append(objectIds, types.ObjectIdentifier{Key: aws.String(key)})
	}
	output, _ := b.S3Client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: aws.String(b.bucketName),
		Delete: &types.Delete{Objects: objectIds},
	})

	// Package the errors that were encountered if there is.
	if len(output.Errors) > 0 {
		r := make([]sop.KeyValueStoreResponse[string], len(names))
		lookup := make(map[string]int)
		for i, name := range names {
			lookup[name] = i
		}
		for i := range output.Errors {
			name := *output.Errors[i].Key
			index := lookup[name]
			r[index] = sop.KeyValueStoreResponse[string] {
				Payload: name,
				Error: fmt.Errorf(*output.Errors[i].Message),
			}
		}
		return r
	}
	// Delete all was successful.
	return nil
}

func isLargeObject(data []byte) bool {
	return len(data) > largeObjectMinSize
}
