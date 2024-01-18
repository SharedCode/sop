package cassandra

import (
	"context"
	"encoding/json"

	"github.com/SharedCode/sop"
)

type mockBlobStore struct {
	lookup map[sop.UUID][]byte
}

// NewBlobStore instantiates a new (mocked) blobstore.
func NewMockBlobStore() BlobStore {
	return &mockBlobStore{
		lookup: make(map[sop.UUID][]byte),
	}
}

func (b *mockBlobStore) GetOne(ctx context.Context, blobName string, blobId sop.UUID, target interface{}) error {
	if ba, ok := b.lookup[blobId]; ok {
		return json.Unmarshal(ba, target)
	}
	return nil
}

func (b *mockBlobStore) Add(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]) error {
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

func (b *mockBlobStore) Update(ctx context.Context, storesblobs ...BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]) error {
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

func (b *mockBlobStore) Remove(ctx context.Context, storesBlobsIds ...BlobsPayload[sop.UUID]) error {
	for _, storeBlobIds := range storesBlobsIds {
		for _, blobId := range storeBlobIds.Blobs {
			delete(b.lookup, blobId)
		}
	}
	return nil
}
