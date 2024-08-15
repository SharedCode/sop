package cassandra

import (
	"context"

	"github.com/SharedCode/sop"
)

type mockBlobStore struct {
	lookup map[sop.UUID][]byte
}

// NewBlobStore instantiates a new (mocked) blobstore.
func NewMockBlobStore() sop.BlobStore {
	return &mockBlobStore{
		lookup: make(map[sop.UUID][]byte),
	}
}

func (b *mockBlobStore) GetOne(ctx context.Context, blobName string, blobID sop.UUID, target interface{}) error {
	if ba, ok := b.lookup[blobID]; ok {
		return Marshaler.Unmarshal(ba, target)
	}
	return nil
}

func (b *mockBlobStore) Add(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := Marshaler.Marshal(blob.Value)
			if err != nil {
				return err
			}
			b.lookup[blob.Key] = ba
		}
	}
	return nil
}

func (b *mockBlobStore) Update(ctx context.Context, storesblobs ...sop.BlobsPayload[sop.KeyValuePair[sop.UUID, interface{}]]) error {
	for _, storeBlobs := range storesblobs {
		for _, blob := range storeBlobs.Blobs {
			ba, err := Marshaler.Marshal(blob.Value)
			if err != nil {
				return err
			}
			b.lookup[blob.Key] = ba
		}
	}
	return nil
}

func (b *mockBlobStore) Remove(ctx context.Context, storesBlobsIDs ...sop.BlobsPayload[sop.UUID]) error {
	for _, storeBlobIDs := range storesBlobsIDs {
		for _, blobID := range storeBlobIDs.Blobs {
			delete(b.lookup, blobID)
		}
	}
	return nil
}
