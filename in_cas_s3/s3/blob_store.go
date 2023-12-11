package s3

import (
	"context"

	"github.com/SharedCode/sop/btree"
)

// BlobStore specifies the backend blob store interface used for storing & managing data blobs.
// Blobs are data that can vary in size and is big enough that they can't be stored in database
// as it will impose performance penalties. This kind of data are typically stored in blob stores
// like AWS S3, or file system, etc...
type BlobStore interface {
	Get(ctx context.Context, blobId btree.UUID) (interface{}, error)
	Add(ctx context.Context, blobId btree.UUID, blob interface{}) error
	Update(ctx context.Context, blobId btree.UUID, blob interface{}) error
	Remove(ctx context.Context, blobId btree.UUID) error
}

// NewBlobStore instantiates a new blobstore.
func NewBlobStore() BlobStore {
	return newBlobStore()
}
