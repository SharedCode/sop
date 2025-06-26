package jsondb

import (
	"context"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/encoding"
)

// Item contains Key & Value pair.
type Item struct {
	ID    sop.UUID       `json:"id"`
	Key   map[string]any `json:"key"`
	Value map[string]any `json:"value"`
}

// B-tree that can operate on JSON String "wrapper". Has no logic except to take in and return
// JSON string payload.
type JsonStringWrapper struct {
	jsonDB *JsonDB
}

// Instantiates a new B-tree that supports JSON string payloads.
func NewJsonBtree(ctx context.Context, so sop.StoreOptions, t sop.Transaction, comparerCELexpression string) (*JsonStringWrapper, error) {
	j, err := NewBtree(ctx, so, t, comparerCELexpression)
	if err != nil {
		return nil, err
	}
	return &JsonStringWrapper{
		jsonDB: j,
	}, nil
}

// Add adds an array of item to the b-tree and does not check for duplicates.
func (j *JsonStringWrapper) Add(ctx context.Context, jsonData string) (bool, error) {
	var items []Item
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(jsonData), &items); err != nil {
		return false, err
	}
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
func (j *JsonStringWrapper) AddIfNotExist(ctx context.Context, jsonData string) (bool, error) {
	var items []Item
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(jsonData), &items); err != nil {
		return false, err
	}
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
func (j *JsonStringWrapper) Update(ctx context.Context, jsonData string) (bool, error) {
	var items []Item
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(jsonData), &items); err != nil {
		return false, err
	}
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
func (j *JsonStringWrapper) Upsert(ctx context.Context, jsonData string) (bool, error) {
	var items []Item
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(jsonData), &items); err != nil {
		return false, err
	}
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
func (j *JsonStringWrapper) Remove(ctx context.Context, jsonDataKeys string) (bool, error) {
	var keys []map[string]any
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(jsonDataKeys), &keys); err != nil {
		return false, err
	}
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

// TODO: add support for navigation methods: First, Last, Find() bool, GetItems(<page #>, <page size>, forward | backward direction)

// GetCurrentValue returns the current item's value.
func (j *JsonStringWrapper) GetValues(ctx context.Context, jsonDataKeys string) (string, error) {
	var keys []map[string]any
	if err := encoding.DefaultMarshaler.Unmarshal([]byte(jsonDataKeys), &keys); err != nil {
		return "", err
	}
	values := make([]map[string]any, len(keys))
	var err error
	j.jsonDB.compareError = nil
	for i := range keys {
		if ok, err := j.jsonDB.FindOne(ctx, keys[i], true); !ok || err != nil {
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

// IsUnique returns true if B-Tree is specified to store items with Unique keys, otherwise false.
// Specifying uniqueness base on key makes the B-Tree permanently set. If you want just a temporary
// unique check during Add of an item, then you can use AddIfNotExist method for that.
func (j *JsonStringWrapper) IsUnique() bool {
	return j.jsonDB.IsUnique()
}

// Returns the number of items in this B-Tree.
func (j *JsonStringWrapper) Count() int64 {
	return j.jsonDB.Count()
}

// Returns StoreInfo which contains the details about this B-Tree.
func (j *JsonStringWrapper) GetStoreInfo() sop.StoreInfo {
	return j.jsonDB.GetStoreInfo()
}
