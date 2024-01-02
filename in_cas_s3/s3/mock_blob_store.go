package s3

import (
	"context"
	"encoding/json"

	"github.com/SharedCode/sop"
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

func (b *mockBlobStore) GetOne(ctx context.Context, blobId btree.UUID, target *btree.Node[interface{}, interface{}]) error {
	if ba, ok := b.lookup[blobId]; ok {
		return json.Unmarshal(ba, target)
	}
	return nil
}

func (b *mockBlobStore) Add(ctx context.Context, blobs ...sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]) error {
	for _, blob := range blobs {
		ba, err := json.Marshal(blob.Value)
		if err != nil {
			return err
		}
		b.lookup[blob.Key] = ba
	}
	return nil
}

func (b *mockBlobStore) Update(ctx context.Context, blobs ...sop.KeyValuePair[btree.UUID, *btree.Node[interface{}, interface{}]]) error {
	for _, blob := range blobs {
		ba, err := json.Marshal(blob.Value)
		if err != nil {
			return err
		}
		b.lookup[blob.Key] = ba
	}
	return nil
}

func (b *mockBlobStore) Remove(ctx context.Context, blobsIds ...btree.UUID) error {
	for _, blobId := range blobsIds {
		delete(b.lookup, blobId)
	}
	return nil
}
