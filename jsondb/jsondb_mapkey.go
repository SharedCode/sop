package jsondb

import (
	"context"
	"github.com/google/uuid"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

// Item contains Key & Value pair.
type ItemMapKey struct {
	Key   map[string]any `json:"key"`
	Value any            `json:"value"`
	ID    uuid.UUID      `json:"id"`
}

// B-tree that can operate on JSON String "wrapper". Has no logic except to take in and return
// JSON string payload.
type JsonMapKey struct {
	jsonDB *JsonDB
}

// Instantiates and creates a new B-tree that supports JSON string payloads.
func NewJsonMapKeyBtree(ctx context.Context, so sop.StoreOptions, t sop.Transaction, comparerCELexpression string) (*JsonMapKey, error) {
	j, err := NewBtree(ctx, so, t, comparerCELexpression)
	if err != nil {
		return nil, err
	}
	return &JsonMapKey{
		jsonDB: j,
	}, nil
}

// Instantiates and opens a B-tree that supports JSON string payloads.
func OpenJsonMapKeyBtree(ctx context.Context, name string, t sop.Transaction) (*JsonMapKey, error) {
	j, err := OpenBtree(ctx, name, t)
	if err != nil {
		return nil, err
	}
	return &JsonMapKey{
		jsonDB: j,
	}, nil
}

// Add adds an array of item to the b-tree and does not check for duplicates.
func (j *JsonMapKey) Add(ctx context.Context, items []ItemMapKey) (bool, error) {
	j.jsonDB.compareError = nil
	for i := range items {
		if ok, err := j.jsonDB.Add(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
		if j.jsonDB.compareError != nil {
			return false, j.jsonDB.compareError
		}
	}
	return true, nil
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (j *JsonMapKey) AddIfNotExist(ctx context.Context, items []ItemMapKey) (bool, error) {
	j.jsonDB.compareError = nil
	for i := range items {
		if ok, err := j.jsonDB.AddIfNotExist(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
		if j.jsonDB.compareError != nil {
			return false, j.jsonDB.compareError
		}
	}
	return true, nil
}

// Update finds the item with key and update its value to the incoming value argument.
func (j *JsonMapKey) Update(ctx context.Context, items []ItemMapKey) (bool, error) {
	j.jsonDB.compareError = nil
	for i := range items {
		if ok, err := j.jsonDB.Update(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
		if j.jsonDB.compareError != nil {
			return false, j.jsonDB.compareError
		}
	}
	return true, nil
}

// Add if not exist or update item if it exists.
func (j *JsonMapKey) Upsert(ctx context.Context, items []ItemMapKey) (bool, error) {
	j.jsonDB.compareError = nil
	for i := range items {
		if ok, err := j.jsonDB.Upsert(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
		if j.jsonDB.compareError != nil {
			return false, j.jsonDB.compareError
		}
	}
	return true, nil
}

// Remove will find the item with a given key then remove that item.
func (j *JsonMapKey) Remove(ctx context.Context, keys []map[string]any) (bool, error) {
	j.jsonDB.compareError = nil
	for i := range keys {
		if ok, err := j.jsonDB.Remove(ctx, keys[i]); !ok || err != nil {
			return false, err
		}
		if j.jsonDB.compareError != nil {
			return false, j.jsonDB.compareError
		}
	}
	return true, nil
}

func (j *JsonMapKey) GetItems(ctx context.Context, pagingInfo PagingInfo) (string, error) {
	return "", nil
}

func (j *JsonMapKey) GetKeys(ctx context.Context, pagingInfo PagingInfo) (string, error) {
	// keys := make([]any, size)
	// var err error
	// j.jsonDB.compareError = nil
	// for i := range keys {
	// 	if ok, err := j.jsonDB.FindOne(ctx, keys[i], true); !ok || err != nil {
	// 		return "", err
	// 	}
	// 	if j.jsonDB.compareError != nil {
	// 		return "", j.jsonDB.compareError
	// 	}
	// 	keys[i], err = j.jsonDB.GetCurrentValue(ctx)
	// 	if err != nil {
	// 		return "", err
	// 	}
	// }
	// ba, err := encoding.DefaultMarshaler.Marshal(keys)
	// if err != nil {
	// 	return "", err
	// }
	// return string(ba), nil
	return "", nil
}

// GetCurrentValue returns the current item's value.
func (j *JsonMapKey) GetValues(ctx context.Context, keys []ItemMapKey) (string, error) {
	values := make([]any, len(keys))
	var err error
	j.jsonDB.compareError = nil
	for i := range keys {
		if ok, err := j.jsonDB.FindWithID(ctx, keys[i].Key, sop.UUID(keys[i].ID)); !ok || err != nil {
			return "", err
		}
		if j.jsonDB.compareError != nil {
			return "", j.jsonDB.compareError
		}
		values[i], err = j.jsonDB.GetCurrentValue(ctx)
		if err != nil {
			return "", err
		}
	}
	ba, err := encoding.DefaultMarshaler.Marshal(values)
	if err != nil {
		return "", err
	}
	return string(ba), nil
}

// FindOne(ctx context.Context, key TK, firstItemWithKey bool) (bool, error)
// // FindOneWithID is synonymous to FindOne but allows code to supply the Item's ID to identify it.
// // This is useful for B-Tree that allows duplicate keys(IsUnique = false) as it provides a way to
// // differentiate duplicated keys via the unique ID(sop.UUID).
// FindOneWithID(ctx context.Context, key TK, id sop.UUID) (bool, error)

// // First positions the "cursor" to the first item as per key ordering.
// // Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// First(ctx context.Context) (bool, error)
// // Last positionts the "cursor" to the last item as per key ordering.
// // Use the CurrentKey/CurrentValue to retrieve the "current item" details(key &/or value).
// Last(ctx context.Context) (bool, error)

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// unique check during Add of an item, then you can use AddIfNotExist method for that.
func (j *JsonMapKey) IsUnique() bool {
	return j.jsonDB.IsUnique()
}

// Returns the number of items in this B-Tree.
func (j *JsonMapKey) Count() int64 {
	return j.jsonDB.Count()
}

// Returns StoreInfo which contains the details about this B-Tree.
func (j *JsonMapKey) GetStoreInfo() sop.StoreInfo {
	return j.jsonDB.GetStoreInfo()
}
