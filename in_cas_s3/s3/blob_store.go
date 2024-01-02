package s3

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
)

// BlobStore specifies the backend blob store interface used for storing & managing data blobs.
// Blobs are data that can vary in size and is big enough that they can't be stored in database
// as it will impose performance penalties. This kind of data are typically stored in blob stores
// like AWS S3, or file system, etc...
type BlobStore interface {
	Get(ctx context.Context, blobId btree.UUID, target *btree.Node[interface{}, interface{}]) error
	Add(ctx context.Context, blobs ...sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]) error
	Update(ctx context.Context, blobs ...sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]) error
	Remove(ctx context.Context, blobsIds ...btree.UUID) error
}

// NewBlobStore instantiates a new (mocked) blobstore.
// TODO: implement a real blob store that either talks to S3 or to a file system to store/manage blobs.
func NewBlobStore() BlobStore {
	return newMockBlobStore()
}
