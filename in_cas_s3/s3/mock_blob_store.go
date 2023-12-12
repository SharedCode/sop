package s3

import (
	"context"
	"encoding/json"

	"github.com/SharedCode/sop/btree"
)

type blobStore struct {
	lookup map[btree.UUID][]byte
}

func newBlobStore() BlobStore {
	return &blobStore{
		lookup: make(map[btree.UUID][]byte),
	}
}

func (b *blobStore)Get(ctx context.Context, blobId btree.UUID, target interface{}) error {
	ba,_ := b.lookup[blobId]
	return json.Unmarshal(ba, target)
}

func (b *blobStore)Add(ctx context.Context, blobId btree.UUID, blob interface{}) error {
	ba, err := json.Marshal(blob)
	if err != nil {
		return err
	}
	b.lookup[blobId] = ba
	return nil
}

func (b *blobStore)Update(ctx context.Context, blobId btree.UUID, blob interface{}) error {
	ba, err := json.Marshal(blob)
	if err != nil {
		return err
	}
	b.lookup[blobId] = ba
	return nil
}

func (b *blobStore)Remove(ctx context.Context, blobId btree.UUID) error {
	delete(b.lookup, blobId)
	return nil
}
