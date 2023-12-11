package s3

import (
	"context"
	"github.com/SharedCode/sop/btree"
)

type blobStore struct {
	lookup map[btree.UUID]interface{}
}

func newBlobStore() BlobStore {
	return &blobStore{
		lookup: make(map[btree.UUID]interface{}),
	}
}

func (b *blobStore)Get(ctx context.Context, blobId btree.UUID) (interface{}, error) {
	v,_ := b.lookup[blobId]
	return v, nil
}

func (b *blobStore)Add(ctx context.Context, blobId btree.UUID, blob interface{}) error {
	b.lookup[blobId] = blob
	return nil
}

func (b *blobStore)Update(ctx context.Context, blobId btree.UUID, blob interface{}) error {
	b.lookup[blobId] = blob
	return nil
}

func (b *blobStore)Remove(ctx context.Context, blobId btree.UUID) error {
	delete(b.lookup, blobId)
	return nil
}
