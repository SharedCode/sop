package jsondb

import (
	"context"
	"fmt"

	"github.com/google/uuid"

	"github.com/SharedCode/sop"
	"github.com/SharedCode/sop/btree"
	"github.com/SharedCode/sop/encoding"
	"github.com/SharedCode/sop/in_red_fs"
)

// Item contains Key & Value pair.
type Item struct {
	Key   any       `json:"key"`
	Value *any      `json:"value"`
	ID    uuid.UUID `json:"id"`
}

func (itm *Item) extract(source *btree.Item[any, any]) {
	itm.Key = source.Key
	itm.Value = source.Value
	itm.ID = uuid.UUID(source.ID)
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

func (j *JsonAnyKey) GetKeys(ctx context.Context, pagingInfo PagingInfo) (string, error) {
	if j.BtreeInterface.GetCurrentKey().Key == nil {
		if pagingInfo.PageOffset != 0 {
			return "", fmt.Errorf("can't fetch keys, try calling First, Last or Find/FindWithID prior to GetItems")
		}
		// Auto navigate to first item of the B-tree if page offset == 0.
		if ok, err := j.BtreeInterface.First(ctx); err != nil {
			return "", err
		} else if !ok {
			return "", fmt.Errorf("can't fetch from an empty btree")
		}
	}

	if pagingInfo.PageOffset > 0 {
		for range pagingInfo.PageOffset {
			for range pagingInfo.PageSize {
				if pagingInfo.Direction == Forward {
					if ok, err := j.BtreeInterface.Next(ctx); err != nil {
						return "", err
					} else if !ok {
						return "", fmt.Errorf("reached the end of B-tree, no items fetched")
					}
					continue
				}
				// Walk in backwards direction.
				if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
					return "", err
				} else if !ok {
					return "", fmt.Errorf("reached the top of B-tree, no items fetched")
				}
			}
		}
	}

	keys := make([]Item, 0, pagingInfo.PageSize)
	for range pagingInfo.PageSize {
		key := j.BtreeInterface.GetCurrentKey()
		itm := Item{
			Key: key.Key,
			ID:  uuid.UUID(key.ID),
		}
		keys = append(keys, itm)
		if pagingInfo.Direction == Forward {
			if ok, err := j.BtreeInterface.Next(ctx); err != nil {
				p, _ := toJsonString(keys)
				return p, err
			} else if !ok {
				return toJsonString(keys)
			}
			continue
		}
		if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
			p, _ := toJsonString(keys)
			return p, err
		} else if !ok {
			return toJsonString(keys)
		}
	}

	// Package as JSON string the result.
	return toJsonString(keys)
}

func (j *JsonAnyKey) GetItems(ctx context.Context, pagingInfo PagingInfo) (string, error) {
	if j.BtreeInterface.GetCurrentKey().Key == nil {
		if pagingInfo.PageOffset != 0 {
			return "", fmt.Errorf("can't fetch items, try calling First, Last or Find/FindWithID prior to GetItems")
		}
		// Auto navigate to first item of the B-tree if page offset == 0.
		if ok, err := j.BtreeInterface.First(ctx); err != nil {
			return "", err
		} else if !ok {
			return "", fmt.Errorf("can't fetch from an empty btree")
		}
	}

	if pagingInfo.PageOffset > 0 {
		for range pagingInfo.PageOffset {
			for range pagingInfo.PageSize {
				if pagingInfo.Direction == Forward {
					if ok, err := j.BtreeInterface.Next(ctx); err != nil {
						return "", err
					} else if !ok {
						return "", fmt.Errorf("reached the end of B-tree, no items fetched")
					}
					continue
				}
				// Walk in backwards direction.
				if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
					return "", err
				} else if !ok {
					return "", fmt.Errorf("reached the top of B-tree, no items fetched")
				}
			}
		}
	}

	items := make([]Item, 0, pagingInfo.PageSize)
	for range pagingInfo.PageSize {
		item, err := j.BtreeInterface.GetCurrentItem(ctx)
		if err != nil {
			p, _ := toJsonString(items)
			return p, err
		}
		var itm Item
		itm.extract(&item)
		items = append(items, itm)
		if pagingInfo.Direction == Forward {
			if ok, err := j.BtreeInterface.Next(ctx); err != nil {
				p, _ := toJsonString(items)
				return p, err
			} else if !ok {
				return toJsonString(items)
			}
			continue
		}
		if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
			p, _ := toJsonString(items)
			return p, err
		} else if !ok {
			return toJsonString(items)
		}
	}

	// Package as JSON string the result.
	return toJsonString(items)
}

// GetCurrentValue returns the current item's value.
func (j *JsonAnyKey) GetValues(ctx context.Context, keys []Item) (string, error) {
	values := make([]Item, len(keys))
	for i := range keys {
		if ok, err := j.BtreeInterface.FindWithID(ctx, keys[i].Key, sop.UUID(keys[i].ID)); err != nil {
			p, _ := toJsonString(values)
			return p, err
		} else if !ok {
			// Assign the source key to allow caller to deduce that item was not found, Value field is empty.
			values[i] = keys[i]
			continue
		}
		item, err := j.BtreeInterface.GetCurrentItem(ctx)
		if err != nil {
			p, _ := toJsonString(values)
			return p, err
		}
		values[i] = Item{}
		values[i].extract(&item)
	}
	return toJsonString(values)
}

// Encode to JSON string the items.
func toJsonString[T any](objects []T) (string, error) {
	if len(objects) == 0 {
		return "", nil
	}
	// Package as JSON string the result.
	ba, err := encoding.DefaultMarshaler.Marshal(objects)
	if err != nil {
		return "", err
	}
	return string(ba), nil
}
