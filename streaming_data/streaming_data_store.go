package streaming_data

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_ck"
)

const (
	MinimumStreamingStoreSlotLength = 50
)

// StreamingDataStore contains methods useful for storage & management of entries that allow
// encoding and decoding to/from data streams.
type StreamingDataStore[TK btree.Ordered] struct {
	// Inherit or reuse an Object implementing BtreeInterface. Golang's inheritance is actually better,
	// it alows to inherit or reuse any object implementing a given interface. "loosely" & nicely done.
	btree.BtreeInterface[StreamingDataKey[TK], []byte]
}

// StreamingDataKey is the Key struct for our Streaming Data Store. Take note, it has to be "public"(starts with capital letter)
// and member fields "public" as well so it can get persisted by JSON encoder/decoder properly.
type StreamingDataKey[TK btree.Ordered] struct {
	Key        TK
	ChunkIndex int
}

// Compare is our Streaming Data Store comparer of keys.
func (x StreamingDataKey[TK]) Compare(other interface{}) int {
	y := other.(StreamingDataKey[TK])

	// Sorted by user define key and followed by the Chunk Index, so we can navigate/iterate it in the chunk's submitted natural order.
	i := btree.Compare(x.Key, y.Key)
	if i != 0 {
		return i
	}
	return cmp.Compare(x.ChunkIndex, y.ChunkIndex)
}

// Synonymous to NewStreamingDataStore but expects StoreOptions parameter.
func NewStreamingDataStore[TK btree.Ordered](ctx context.Context, so sop.StoreOptions, trans sop.Transaction, comparer btree.ComparerFunc[StreamingDataKey[TK]]) (*StreamingDataStore[TK], error) {
	if so.SlotLength < MinimumStreamingStoreSlotLength {
		return nil, fmt.Errorf("streaming data store requires minimum of %d SlotLength", MinimumStreamingStoreSlotLength)
	}
	if so.IsValueDataInNodeSegment {
		return nil, fmt.Errorf("streaming data store requires value data to be set for save in separate segment(IsValueDataInNodeSegment = false)")
	}
	if !so.IsUnique {
		return nil, fmt.Errorf("streaming data store requires unique key (IsUnique = true) to be set to true")
	}
	btree, err := in_red_ck.NewBtree[StreamingDataKey[TK], []byte](ctx, so, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &StreamingDataStore[TK]{
		BtreeInterface: btree,
	}, nil
}

// OpenStreamingDataStore opens an existing data store for use in "streaming data".
func OpenStreamingDataStore[TK btree.Ordered](ctx context.Context, name string, trans sop.Transaction, comparer btree.ComparerFunc[StreamingDataKey[TK]]) (*StreamingDataStore[TK], error) {
	btree, err := in_red_ck.OpenBtree[StreamingDataKey[TK], []byte](ctx, name, trans, comparer)
	if err != nil {
		return nil, err
	}
	return &StreamingDataStore[TK]{
		BtreeInterface: btree,
	}, nil
}

// Add insert an item to the b-tree and returns an encoder you can use to write the streaming data on.
func (s *StreamingDataStore[TK]) Add(ctx context.Context, key TK) (*Encoder[TK], error) {
	w := newWriter(ctx, true, key, s.BtreeInterface)
	return newEncoder(w), nil
}

// Add insert an item to the b-tree and returns an encoder you can use to write the streaming data on.
func (s *StreamingDataStore[TK]) AddIfNotExist(ctx context.Context, key TK) (*Encoder[TK], error) {
	// Return nil if key already found in B-tree.
	if found, err := s.FindOne(ctx, key); err != nil || found {
		return nil, err
	}
	return s.Add(ctx, key)
}

func (s *StreamingDataStore[TK]) Upsert(ctx context.Context, key TK) (*Encoder[TK], error) {
	if found, err := s.FindOne(ctx, key); err != nil {
		return nil, err
	} else if found {
		return s.Update(ctx, key)
	}
	// Add if not exist.
	return s.Add(ctx, key)
}

// Remove will delete the item's data chunks given its key.
func (s *StreamingDataStore[TK]) Remove(ctx context.Context, key TK) (bool, error) {
	if found, err := s.FindOne(ctx, key); err != nil || !found {
		return false, err
	}
	return s.RemoveCurrentItem(ctx)
}

// RemoveCurrentItem will delete the current item's data chunks.
func (s *StreamingDataStore[TK]) RemoveCurrentItem(ctx context.Context) (bool, error) {
	if s.BtreeInterface.Count() == 0 {
		return false, fmt.Errorf("failed to remove current item, store is empty")
	}

	key := s.BtreeInterface.GetCurrentKey().Key.Key
	keys := make([]StreamingDataKey[TK], 0, 5)
	for {
		keys = append(keys, StreamingDataKey[TK]{Key: key, ChunkIndex: s.BtreeInterface.GetCurrentKey().Key.ChunkIndex})
		if ok, err := s.BtreeInterface.Next(ctx); err != nil {
			return false, err
		} else if !ok ||
			s.BtreeInterface.GetCurrentKey().Key.Compare(StreamingDataKey[TK]{Key: key, ChunkIndex: s.BtreeInterface.GetCurrentKey().Key.ChunkIndex}) != 0 {
			break
		}
	}

	var lastErr error
	succeeded := true
	for _, k := range keys {
		if ok, err := s.BtreeInterface.Remove(ctx, k); err != nil {
			lastErr = err
		} else if !ok {
			// Only return success if all "chunks" are deleted.
			succeeded = false
		}
	}
	return succeeded, lastErr
}

// Update finds the item with key and returns an encoder you can use to upload and update the item's data chunks.
func (s *StreamingDataStore[TK]) Update(ctx context.Context, key TK) (*Encoder[TK], error) {
	if found, err := s.FindOne(ctx, key); err != nil || !found {
		return nil, err
	}
	return s.UpdateCurrentItem(ctx)
}

// UpdateCurrentItem will return an encoder that will allow you to update the current item's data chunks.
func (s *StreamingDataStore[TK]) UpdateCurrentItem(ctx context.Context) (*Encoder[TK], error) {
	if s.BtreeInterface.Count() == 0 {
		return nil, fmt.Errorf("failed to update current item, store is empty")
	}
	w := newWriter(ctx, false, s.BtreeInterface.GetCurrentKey().Key.Key, s.BtreeInterface)
	return newEncoder(w), nil
}

// Add a chunk to a given entry with specified key & chunk index. Key & chunk index should reference a new chunk record.
// The function call will fail if there is already a given chunk w/ such key & chunk index in the database (B-tree).
func (s *StreamingDataStore[TK]) AddChunk(ctx context.Context, key TK, chunkIndex int, chunkValue []byte) (bool, error) {
	return s.BtreeInterface.AddIfNotExist(ctx, StreamingDataKey[TK]{
		Key:        key,
		ChunkIndex: chunkIndex,
	}, chunkValue)
}

// Update an existing chunk (byte array) of a given entry with specified key & chunk index.
func (s *StreamingDataStore[TK]) UpdateChunk(ctx context.Context, key TK, chunkIndex int, newChunkValue []byte) (bool, error) {
	return s.BtreeInterface.Update(ctx, StreamingDataKey[TK]{
		Key:        key,
		ChunkIndex: chunkIndex,
	}, newChunkValue)
}

// Remove an existing chunk record of a given entry with specified key & chunk index.
func (s *StreamingDataStore[TK]) RemoveChunk(ctx context.Context, key TK, chunkIndex int) (bool, error) {
	return s.BtreeInterface.Remove(ctx, StreamingDataKey[TK]{
		Key:        key,
		ChunkIndex: chunkIndex,
	})
}

// GetCurrentKey returns the current item's key.
func (s *StreamingDataStore[TK]) GetCurrentKey(ctx context.Context) TK {
	if s.BtreeInterface.Count() == 0 {
		var d TK
		return d
	}
	k := s.BtreeInterface.GetCurrentKey().Key
	return k.Key
}

// GetCurrentItem returns the current item key & decoder you can use to download the data chunks (or stream it down).
func (s *StreamingDataStore[TK]) GetCurrentItem(ctx context.Context) (btree.Item[TK, json.Decoder], error) {
	if s.BtreeInterface.Count() == 0 {
		return btree.Item[TK, json.Decoder]{}, fmt.Errorf("failed to get current item, store is empty")
	}
	ck := s.BtreeInterface.GetCurrentKey().Key
	r := newReader(ctx, ck.Key, ck.ChunkIndex, s.BtreeInterface)
	return btree.Item[TK, json.Decoder]{
		Key:   ck.Key,
		Value: json.NewDecoder(r),
	}, nil
}

// GetCurrentValue returns the current item's decoder you can use to download the data chunks (or stream it down).
func (s *StreamingDataStore[TK]) GetCurrentValue(ctx context.Context) (*json.Decoder, error) {
	if s.BtreeInterface.Count() == 0 {
		return nil, fmt.Errorf("failed to get current value, store is empty")
	}
	ck := s.BtreeInterface.GetCurrentKey().Key
	r := newReader(ctx, ck.Key, ck.ChunkIndex, s.BtreeInterface)
	return json.NewDecoder(r), nil
}

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or decoder).
func (s *StreamingDataStore[TK]) FindOne(ctx context.Context, key TK) (bool, error) {
	k := StreamingDataKey[TK]{Key: key}
	return s.BtreeInterface.Find(ctx, k, false)
}

// Synonymous to FindChunk.
func (s *StreamingDataStore[TK]) FindOneWithID(ctx context.Context, key TK, chunkIndex int) (bool, error) {
	return s.FindChunk(ctx, key, chunkIndex)
}

// FindChunk will search Btree for an item with a given key and chunkIndex.
// If you passed in a chunkIndex that is beyond the number of chunks of the item then it will return false.
//
// You can use FindChunk or FindOne & Next to navigate to the fragment or chunk # you are targeting to download.
func (s *StreamingDataStore[TK]) FindChunk(ctx context.Context, key TK, chunkIndex int) (bool, error) {
	k := StreamingDataKey[TK]{Key: key, ChunkIndex: chunkIndex}
	return s.BtreeInterface.Find(ctx, k, false)
}
