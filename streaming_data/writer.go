package streaming_data

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
)

type writer[TK btree.Comparable] struct {
	btree      btree.BtreeInterface[streamingDataKey[TK], any]
	ctx        context.Context
	key        TK
	chunkIndex int
}

func newWriter[TK btree.Comparable](ctx context.Context, btree btree.BtreeInterface[streamingDataKey[TK], any]) *writer[TK] {
	return &writer[TK]{
		btree: btree,
		ctx:   ctx,
	}
}

func (e *writer[TK]) Write(p []byte) (n int, err error) {
	if ok, err := e.btree.Add(e.ctx, streamingDataKey[TK]{key: e.key, chunkIndex: e.chunkIndex}, p); err != nil || !ok {
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("Write failed, chunk did not insert to the backend Btree")
	}
	// Increment the chunk index in prep for next (chunk) Write call.
	e.chunkIndex++
	return len(p), nil
}
