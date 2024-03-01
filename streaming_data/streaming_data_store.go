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

// StreamingDataStore contains methods useful for storage & management of entries that allow
// encoding and decoding to/from data streams.
type StreamingDataStore[TK btree.Comparable] struct {
	btree btree.BtreeInterface[StreamingDataKey[TK], []byte]
}

// StreamingDataKey is the Key struct for our Streaming Data Store. Take note, it has to be "public"(starts with capital letter)
// and member fields "public" as well so it can get persisted by JSON encoder/decoder properly.
type StreamingDataKey[TK btree.Comparable] struct {
	Key        TK
	ChunkIndex int
}

// Compare is our Streaming Data Store comparer of keys.
func (x StreamingDataKey[TK]) Compare(other interface{}) int {
	y := other.(StreamingDataKey[TK])

	// Sorted by user define key and followed by the Chunk Index, so we can navigate/iterate it in the chunk's submitted natural order.
	i := btree.Compare[TK](x.Key, y.Key)
	if i != 0 {
		return i
	}
	return cmp.Compare[int](x.ChunkIndex, y.ChunkIndex)
}

// NewStreamingDataStore instantiates a new Data Store for use in "streaming data".
// That is, the "value" is saved in separate segment(partition in Cassandra) &
// actively persisted to the backend, e.g. - call to Add method will save right away
// to the separate segment and on commit, it will be a quick action as data is already saved to the data segments.
//
// This behaviour makes this store ideal for data management of huge blobs, like movies or huge data graphs.
func NewStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans in_red_ck.Transaction) *StreamingDataStore[TK] {
	btree, _ := in_red_ck.NewBtree[StreamingDataKey[TK], []byte](ctx, sop.StoreOptions{
		Name:                         name,
		SlotLength:                   500,
		IsUnique:                     true,
		IsValueDataInNodeSegment:     false,
		IsValueDataActivelyPersisted: true,
		IsValueDataGloballyCached:    false,
		LeafLoadBalancing:            false,
		Description:                  "Streaming data",
	}, trans)
	return &StreamingDataStore[TK]{
		btree: btree,
	}
}

// Add insert an item to the b-tree and returns an encoder you can use to write the streaming data on.
func (s *StreamingDataStore[TK]) Add(ctx context.Context, key TK) (*Encoder[TK], error) {
	w := newWriter(ctx, true, key, s.btree)
	return newEncoder(w), nil
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
	if s.btree.Count() == 0 {
		return false, fmt.Errorf("failed to remove current item, store is empty")
	}

	key := s.btree.GetCurrentKey().Key
	keys := make([]StreamingDataKey[TK], 0, 5)
	for {
		keys = append(keys, StreamingDataKey[TK]{Key: key, ChunkIndex: s.btree.GetCurrentKey().ChunkIndex})
		if ok, err := s.btree.Next(ctx); err != nil {
			return false, err
		} else if !ok ||
			s.btree.GetCurrentKey().Compare(StreamingDataKey[TK]{Key: key, ChunkIndex: s.btree.GetCurrentKey().ChunkIndex}) != 0 {
			break
		}
	}

	var lastErr error
	succeeded := true
	for _, k := range keys {
		if ok, err := s.btree.Remove(ctx, k); err != nil {
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
	if s.btree.Count() == 0 {
		return nil, fmt.Errorf("failed to update current item, store is empty")
	}
	w := newWriter(ctx, false, s.btree.GetCurrentKey().Key, s.btree)
	return newEncoder(w), nil
}

// GetCurrentValue returns the current item's decoder you can use to download the data chunks (or stream it down).
func (s *StreamingDataStore[TK]) GetCurrentValue(ctx context.Context) (*json.Decoder, error) {
	if s.btree.Count() == 0 {
		return nil, fmt.Errorf("failed to get current value, store is empty")
	}
	ck := s.btree.GetCurrentKey()
	r := newReader(ctx, ck.Key, ck.ChunkIndex, s.btree)
	return json.NewDecoder(r), nil
}

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or decoder).
func (s *StreamingDataStore[TK]) FindOne(ctx context.Context, key TK) (bool, error) {
	k := StreamingDataKey[TK]{Key: key}
	return s.btree.FindOne(ctx, k, false)
}

// FindChunk will search Btree for an item with a given key and chunkIndex.
// If you passed in a chunkIndex that is beyond the number of chunks of the item then it will return false.
//
// You can use FindChunk or FindOne & Next to navigate to the fragment or chunk # you are targeting to download.
func (s *StreamingDataStore[TK]) FindChunk(ctx context.Context, key TK, chunkIndex int) (bool, error) {
	k := StreamingDataKey[TK]{Key: key, ChunkIndex: chunkIndex}
	return s.btree.FindOne(ctx, k, false)
}

// GetCurrentKey returns the current item's key.
func (s *StreamingDataStore[TK]) GetCurrentKey() TK {
	return s.btree.GetCurrentKey().Key
}

// First positions the "cursor" to the first item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *StreamingDataStore[TK]) First(ctx context.Context) (bool, error) {
	return s.btree.First(ctx)
}

// Last positionts the "cursor" to the last item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *StreamingDataStore[TK]) Last(ctx context.Context) (bool, error) {
	return s.btree.Last(ctx)
}

// Next positions the "cursor" to the next item chunk as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
//
// Ensure you are not navigating passed the target chunk via calling GetCurrentKey and checking that
// it is still the Key of the item you are interested about.
func (s *StreamingDataStore[TK]) Next(ctx context.Context) (bool, error) {
	return s.btree.Next(ctx)
}

// Previous positions the "cursor" to the previous item chunk as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *StreamingDataStore[TK]) Previous(ctx context.Context) (bool, error) {
	return s.btree.Previous(ctx)
}

// IsUnique always returns true for Streaming Data Store.
func (s *StreamingDataStore[TK]) IsUnique() bool {
	return s.btree.IsUnique()
}

// Returns the total number of data chunks in this store.
func (s *StreamingDataStore[TK]) Count() int64 {
	return s.btree.Count()
}
