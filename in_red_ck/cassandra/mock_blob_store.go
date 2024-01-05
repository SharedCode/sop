package cassandra

import (
	"context"
	"encoding/json"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
)

type mockBlobStore struct {
	lookup map[btree.UUID][]byte
}

// NewBlobStore instantiates a new (mocked) blobstore.
func NewMockBlobStore() BlobStore {
	return &mockBlobStore{
		lookup: make(map[btree.UUID][]byte),
	}
}

func (b *mockBlobStore) GetOne(ctx context.Context, blobName string, blobId btree.UUID, target *btree.Node[interface{}, interface{}]) error {
	if ba, ok := b.lookup[blobId]; ok {
		return json.Unmarshal(ba, target)
	}
	return nil
}

func (b *mockBlobStore) Add(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := json.Marshal(blob.Value)
			if err != nil {
				return err
			}
			b.lookup[blob.Key] = ba
		}
	}
	return nil
}

func (b *mockBlobStore) Update(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := json.Marshal(blob.Value)
			if err != nil {
				return err
			}
			b.lookup[blob.Key] = ba
		}
	}
	return nil
}

func (b *mockBlobStore) Remove(ctx context.Context, storesBlobsIds ...BlobsPayload[btree.UUID]) error {
	for _, storeBlobIds := range storesBlobsIds {
		for _, blobId := range storeBlobIds.Blobs {
			delete(b.lookup, blobId)
		}
	}
	return nil
}
