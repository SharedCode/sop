package s3

import (
	"context"
	"encoding/json"

	"github.com/SharedCode/sop/btree"
)

type mockBlobStore struct {
	lookup map[btree.UUID][]byte
}

func newMockBlobStore() BlobStore {
	return &mockBlobStore{
		lookup: make(map[btree.UUID][]byte),
	}
}

func (b *mockBlobStore) Get(ctx context.Context, blobId btree.UUID, target interface{}) error {
	if ba, ok := b.lookup[blobId]; ok {
		return json.Unmarshal(ba, target)
	}
	return nil
}

func (b *mockBlobStore) Add(ctx context.Context, blobId btree.UUID, blob interface{}) error {
	ba, err := json.Marshal(blob)
	if err != nil {
		return err
	}
	b.lookup[blobId] = ba
	return nil
}

func (b *mockBlobStore) Update(ctx context.Context, blobId btree.UUID, blob interface{}) error {
	ba, err := json.Marshal(blob)
	if err != nil {
		return err
	}
	b.lookup[blobId] = ba
	return nil
}

func (b *mockBlobStore) Remove(ctx context.Context, blobId btree.UUID) error {
	delete(b.lookup, blobId)
	return nil
}
