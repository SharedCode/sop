package streaming_data

import (
	"cmp"
	"context"
	"encoding/json"

	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/in_red_ck"
)

// StreamingDataStore interface contains methods useful for managing entries that allow encoding or decoding
// of data streams.
type StreamingDataStore[TK btree.Comparable] interface {
	// Add insert an item to the b-tree and returns an encoder you can use to write the streaming data on.
	Add(ctx context.Context, key TK) (*json.Encoder, error)
}

type streamingDataStore[TK btree.Comparable] struct {
	btree btree.BtreeInterface[streamingDataKey[TK], any]
}
type streamingDataKey[TK btree.Comparable] struct {
	key        TK
	chunkIndex int
}
func (x streamingDataKey[TK]) Compare(other interface{}) int {
	y := other.(streamingDataKey[TK])
	i := btree.Compare[TK](x.key, y.key)
	if i != 0 {
		return i
	}
	return cmp.Compare[int](x.chunkIndex, y.chunkIndex)
}

// NewStreamingDataStore instantiates a new Data Store for use in "streaming data".
// That is, the "value" is saved in separate segment(partition in Cassandra) &
// actively persisted to the backend, e.g. - call to Add method will save right away
// to the separate segment and on commit, it will be a quick action as data is already saved to the data segments.
//
// This behaviour makes this store ideal for data management of huge blobs, like movies or huge data graphs.
func NewStreamingDataStore[TK btree.Comparable](ctx context.Context, name string, trans in_red_ck.Transaction) StreamingDataStore[TK] {
	btree, _ := in_red_ck.NewBtreeExt[streamingDataKey[TK], any](ctx, name, 500, true, false, true, false, false, "Streaming data", trans)
	return &streamingDataStore[TK]{
		btree: btree,
	}
}

// Add insert an item to the b-tree and returns an encoder you can use to write the streaming data on.
func (s *streamingDataStore[TK]) Add(ctx context.Context, key TK) (*json.Encoder, error) {
	e := newWriter[TK](ctx, s.btree)
	e.key = key
	return json.NewEncoder(e), nil
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (s *streamingDataStore[TK]) AddIfNotExist(ctx context.Context, key TK) (*json.Encoder, error) {
	if found, err := s.btree.FindOne(ctx, streamingDataKey[TK]{key: key, chunkIndex: 0}, false); err != nil || found {
		return nil, err
	}
	return s.Add(ctx, key)
}

// Remove will delete the item's data chunks given its key.
func (s *streamingDataStore[TK]) Remove(ctx context.Context, key TK) (bool, error) {
	if found, err := s.btree.FindOne(ctx, streamingDataKey[TK]{key: key, chunkIndex: 0}, false); err != nil || !found {
		return false, err
	}
	return s.RemoveCurrentItem(ctx)
}
// RemoveCurrentItem will remove the current key/value pair from the store.
func (s *streamingDataStore[TK]) RemoveCurrentItem(ctx context.Context) (bool, error) {
	key := s.btree.GetCurrentKey().key
	keys := make([]streamingDataKey[TK], 0, 5)
	for {
		keys = append(keys, streamingDataKey[TK]{key: key, chunkIndex: s.btree.GetCurrentKey().chunkIndex})
		if ok, err := s.btree.Next(ctx); err != nil{
			return false, err
		} else if !ok ||
			s.btree.GetCurrentKey().Compare(streamingDataKey[TK]{key: key, chunkIndex: s.btree.GetCurrentKey().chunkIndex}) != 0 {
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

// // Update finds the item with key and update its value to the value argument.
// Update(ctx context.Context, key TK) (json.Encoder, error)
// // UpdateCurrentItem will update the Value of the current item.
// // Key is read-only, thus, no argument for the key.
// UpdateCurrentItem(ctx context.Context) (json.Encoder, error)

// // FindOne will search Btree for an item with a given key. Return true if found,
// // otherwise false. firstItemWithKey is useful when there are items with same key.
// // true will position pointer to the first item with the given key,
// // according to key ordering sequence.
// // Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error)
// // FindOneWithID is synonymous to FindOne but allows code to supply the Item's ID to identify it.
// // This is useful for B-Tree that allows duplicate keys(IsUnique = false) as it provides a way to
// // differentiate duplicated keys via the unique ID(sop.UUID).
// FindOneWithID(ctx context.Context, key TK, id sop.UUID) (bool, error)
// // GetCurrentValue returns the current item's value.
// GetCurrentValue(ctx context.Context) (json.Decoder, error)
// // GetCurrentItemID returns the current item's ID. ID can be used in FindOneWithID method.
// GetCurrentItemID(ctx context.Context) (sop.UUID, error)

// GetCurrentKey returns the current item's key.
func (s *streamingDataStore[TK]) GetCurrentKey() TK {
	return s.GetCurrentKey()
}

// First positions the "cursor" to the first item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *streamingDataStore[TK]) First(ctx context.Context) (bool, error) {
	return s.First(ctx)
}
// Last positionts the "cursor" to the last item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *streamingDataStore[TK]) Last(ctx context.Context) (bool, error) {
	return s.Last(ctx)
}
// Next positions the "cursor" to the next item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *streamingDataStore[TK]) Next(ctx context.Context) (bool, error) {
	return s.Next(ctx)
}
// Previous positions the "cursor" to the previous item as per key ordering.
// Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
func (s *streamingDataStore[TK]) Previous(ctx context.Context) (bool, error) {
	return s.Previous(ctx)
}

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// unique check during Add of an item, then you can use AddIfNotExist method for that.
func (s *streamingDataStore[TK]) IsUnique() bool {
	return s.IsUnique()
}

// Returns the number of items in this B-Tree.
func (s *streamingDataStore[TK]) Count() int64 {
	return s.Count()
}
