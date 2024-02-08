package streaming_data

import (
	"context"
	"fmt"

	"github.com/SharedCode/sop/btree"
)

type writer[TK btree.Comparable] struct {
	btree      btree.BtreeInterface[streamingDataKey[TK], []byte]
	ctx        context.Context
	key        TK
	chunkIndex int
	addOrUpdate bool
}

func newWriter[TK btree.Comparable](ctx context.Context, addOrUpdate bool, key TK, btree btree.BtreeInterface[streamingDataKey[TK], []byte]) *writer[TK] {
	return &writer[TK]{
		btree: btree,
		ctx:   ctx,
		addOrUpdate: addOrUpdate,
		key: key,
	}
}

func (w *writer[TK]) Write(p []byte) (n int, err error) {
	// Add.
	if w.addOrUpdate {
		if ok, err := w.btree.Add(w.ctx, streamingDataKey[TK]{key: w.key, chunkIndex: w.chunkIndex}, p); err != nil || !ok {
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("Write failed, key: %v, chunk: #%d did not insert to the backend Btree", w.key, w.chunkIndex)
		}
		// Increment the chunk index in prep for next (chunk) Write call.
		w.chunkIndex++
		return len(p), nil
	}
	// Update.
	ok, err := w.btree.FindOne(w.ctx, streamingDataKey[TK]{key: w.key, chunkIndex: w.chunkIndex}, false)
	if err != nil {
		return 0, err
	}
	if ok {
		if ok, err := w.btree.UpdateCurrentItem(w.ctx, p); err != nil || !ok {
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("Write failed, key: %v, chunk: #%d did not update to the backend Btree", w.key, w.chunkIndex)
		}
		// Increment the chunk index in prep for next (chunk) Write call.
		w.chunkIndex++
		return len(p), nil
	}
	// Current chunk with index not found, new chunk should be added.
	if ok, err := w.btree.Add(w.ctx, streamingDataKey[TK]{key: w.key, chunkIndex: w.chunkIndex}, p); err != nil || !ok {
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("Write failed, key: %v, chunk: #%d did not insert to the backend Btree", w.key, w.chunkIndex)
	}
	// Increment the chunk index in prep for next (chunk) Write call.
	w.chunkIndex++
	return len(p), nil
}
