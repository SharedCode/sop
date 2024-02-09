package streaming_data

import (
	"cmp"
	"context"
	"encoding/json"

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
	btree, _ := in_red_ck.NewBtreeExt[StreamingDataKey[TK], []byte](ctx, name, 500, true, false, true, false, false, "Streaming data", trans)
	return &StreamingDataStore[TK]{
		btree: btree,
	}
}

// Add insert an item to the b-tree and returns an encoder you can use to write the streaming data on.
func (s *StreamingDataStore[TK]) Add(ctx context.Context, key TK) (*Encoder[TK], error) {
	w := newWriter[TK](ctx, true, key, s.btree)
	return newEncoder(ctx, w), nil
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (s *StreamingDataStore[TK]) AddIfNotExist(ctx context.Context, key TK) (*Encoder[TK], error) {
	if found, err := s.FindOne(ctx, key, false); err != nil || found {
		return nil, err
	}
	return s.Add(ctx, key)
}

// Remove will delete the item's data chunks given its key.
func (s *StreamingDataStore[TK]) Remove(ctx context.Context, key TK) (bool, error) {
	if found, err := s.FindOne(ctx, key, false); err != nil || !found {
		return false, err
	}
	return s.RemoveCurrentItem(ctx)
}

// RemoveCurrentItem will remove the current key/value pair from the store.
func (s *StreamingDataStore[TK]) RemoveCurrentItem(ctx context.Context) (bool, error) {
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

// Update finds the item with key and update its value to the value argument.
func (s *StreamingDataStore[TK]) Update(ctx context.Context, key TK) (*Encoder[TK], error) {
	if found, err := s.FindOne(ctx, key, false); err != nil || !found {
		return nil, err
	}
	return s.UpdateCurrentItem(ctx)
}

// UpdateCurrentItem will update the Value of the current item.
// Key is read-only, thus, no argument for the key.
func (s *StreamingDataStore[TK]) UpdateCurrentItem(ctx context.Context) (*Encoder[TK], error) {
	w := newWriter[TK](ctx, false, s.btree.GetCurrentKey().Key, s.btree)
	return newEncoder(ctx, w), nil
}

// GetCurrentValue returns the current item's value.
func (s *StreamingDataStore[TK]) GetCurrentValue(ctx context.Context) (*json.Decoder, error) {
	r := newReader[TK](ctx, s.btree.GetCurrentKey().Key, s.btree)
	return json.NewDecoder(r), nil
}

// FindOne will search Btree for an item with a given key. Return true if found,
// otherwise false. firstItemWithKey is useful when there are items with same key.
// true will position pointer to the first item with the given key,
// according to key ordering sequence.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *StreamingDataStore[TK]) FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error) {
	k := StreamingDataKey[TK]{Key: key}
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

// Next positions the "cursor" to the next item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *StreamingDataStore[TK]) Next(ctx context.Context) (bool, error) {
	return s.btree.Next(ctx)
}

// Previous positions the "cursor" to the previous item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *StreamingDataStore[TK]) Previous(ctx context.Context) (bool, error) {
	return s.btree.Previous(ctx)
}

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// unique check during Add of an item, then you can use AddIfNotExist method for that.
func (s *StreamingDataStore[TK]) IsUnique() bool {
	return s.btree.IsUnique()
}

// Returns the number of items in this B-Tree.
func (s *StreamingDataStore[TK]) Count() int64 {
	return s.btree.Count()
}
