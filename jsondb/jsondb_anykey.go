package jsondb

import (
	"context"

	"github.com/google/uuid"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/in_red_fs"
)

// Item contains Key & Value pair.
type Item struct {
	Key   any       `json:"key"`
	Value any       `json:"value"`
	ID    uuid.UUID `json:"id"`
}

// B-tree that can operate on JSON String "wrapper". Has no logic except to take in and return
// JSON string payload.
type JsonAnyKey struct {
	btree.BtreeInterface[any, any]
}

// Instantiates and creates a new B-tree that supports JSON string payloads.
func NewJsonBtree(ctx context.Context, so sop.StoreOptions, t sop.Transaction) (*JsonAnyKey, error) {
	b3, err := in_red_fs.NewBtreeWithReplication[any, any](ctx, so, t, nil)
	if err != nil {
		return nil, err
	}
	return &JsonAnyKey{
		BtreeInterface: b3,
	}, nil
}

// Instantiates and opens a B-tree that supports JSON string payloads.
func OpenJsonBtree(ctx context.Context, name string, t sop.Transaction) (*JsonAnyKey, error) {
	b3, err := in_red_fs.OpenBtreeWithReplication[any, any](ctx, name, t, nil)
	if err != nil {
		return nil, err
	}
	return &JsonAnyKey{
		BtreeInterface: b3,
	}, nil
}

// Add adds an array of item to the b-tree and does not check for duplicates.
func (j *JsonAnyKey) Add(ctx context.Context, items []Item) (bool, error) {
	for i := range items {
		if ok, err := j.BtreeInterface.Add(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (j *JsonAnyKey) AddIfNotExist(ctx context.Context, items []Item) (bool, error) {
	for i := range items {
		if ok, err := j.BtreeInterface.AddIfNotExist(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// Update finds the item with key and update its value to the incoming value argument.
func (j *JsonAnyKey) Update(ctx context.Context, items []Item) (bool, error) {
	for i := range items {
		if ok, err := j.BtreeInterface.Update(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// Add if not exist or update item if it exists.
func (j *JsonAnyKey) Upsert(ctx context.Context, items []Item) (bool, error) {
	for i := range items {
		if ok, err := j.BtreeInterface.Upsert(ctx, items[i].Key, items[i].Value); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// Remove will find the item with a given key then remove that item.
func (j *JsonAnyKey) Remove(ctx context.Context, keys []any) (bool, error) {
	for i := range keys {
		if ok, err := j.BtreeInterface.Remove(ctx, keys[i]); !ok || err != nil {
			return false, err
		}
	}
	return true, nil
}

// TODO: add support for navigation methods: First, Last, Find() bool, GetItems(<page #>, <page size>, forward | backward direction)

// GetCurrentValue returns the current item's value.
func (j *JsonAnyKey) GetValues(ctx context.Context, keys []any) (string, error) {
	values := make([]any, len(keys))
	var err error
	for i := range keys {
		if ok, err := j.BtreeInterface.FindOne(ctx, keys[i], true); !ok || err != nil {
			return "", err
		}
		values[i], err = j.BtreeInterface.GetCurrentValue(ctx)
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
