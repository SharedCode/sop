package cassandra

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

// Manage or fetch node blobs request/response payload.
type BlobsPayload[T btree.UUID | sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]] struct {
	// Blob store table name.
	BlobTable string
	// Blobs contains the blobs Ids and blobs data for upsert to the store or the blobs Ids to be removed.
	Blobs []T
}

// BlobStore specifies the backend blob store interface used for storing & managing data blobs.
// Blobs are data that can vary in size and is big enough that they can't be stored in database
// as it will impose performance penalties. This kind of data are typically stored in blob stores
// like AWS S3, or file system, etc...
type BlobStore interface {
	// Get or fetch a blob given an Id.
	GetOne(ctx context.Context, blobStoreName string, blobId btree.UUID, target *btree.Node[interface{}, interface{}]) error
	// Add blobs to store.
	Add(ctx context.Context, blobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error
	// Update blobs in store.
	Update(ctx context.Context, blobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error
	// Remove blobs in store with given Ids.
	Remove(ctx context.Context, blobsIds ...BlobsPayload[btree.UUID]) error
}

