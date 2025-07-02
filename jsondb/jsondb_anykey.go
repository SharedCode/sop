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
type Item[TK btree.Ordered, TV any] struct {
	Key   TK        `json:"key"`
	Value *TV       `json:"value"`
	ID    uuid.UUID `json:"id"`
}

func (itm *Item[TK, TV]) extract(source *btree.Item[TK, TV]) {
	itm.Key = source.Key
	itm.Value = source.Value
	itm.ID = uuid.UUID(source.ID)
}

// B-tree that can operate on JSON String "wrapper". Has no logic except to take in and return
// JSON string payload.
type JsonDBAnyKey[TK btree.Ordered, TV any] struct {
	btree.BtreeInterface[TK, TV]
	compareError error
}

type PagingDirection int

const (
	Forward = iota
	Backward
)

// Paging Info specifies fetching details.
type PagingInfo struct {
	// -1 or 0 means to fetch data starting from the current "cursor" location.
	// > 0 means to traverse to that page offset and fetch data from that "cursor" location.
	PageOffset int `json:"page_offset"`
	// Number of data elements(Keys or Items) per page.
	PageSize int `json:"page_size"`
	// Count of elements to fetch starting with the page offset. If left 0, 'will fetch PageSize number of elements
	// after traversing the B-tree, bringing the cursor to the requested page offset.
	// Otherwise, will fetch FetchCount number of data elements starting from the page offset.
	FetchCount int `json:"fetch_count"`
	// Direction of fetch is either forward(0) or backwards(1).
	Direction PagingDirection `json:"direction"`
}

// Instantiates and creates a new B-tree that supports JSON string payloads.
func NewJsonBtree[TK btree.Ordered, TV any](ctx context.Context, so sop.StoreOptions, t sop.Transaction, comparer btree.ComparerFunc[TK]) (*JsonDBAnyKey[TK, TV], error) {
	b3, err := in_red_fs.NewBtreeWithReplication[TK, TV](ctx, so, t, comparer)
	if err != nil {
		return nil, err
	}
	return &JsonDBAnyKey[TK, TV]{
		BtreeInterface: b3,
	}, nil
}

// Instantiates and opens a B-tree that supports JSON string payloads.
func OpenJsonBtree[TK btree.Ordered, TV any](ctx context.Context, name string, t sop.Transaction, comparer btree.ComparerFunc[TK]) (*JsonDBAnyKey[TK, TV], error) {
	b3, err := in_red_fs.OpenBtreeWithReplication[TK, TV](ctx, name, t, comparer)
	if err != nil {
		return nil, err
	}
	return &JsonDBAnyKey[TK, TV]{
		BtreeInterface: b3,
	}, nil
}

// Add adds an array of item to the b-tree and does not check for duplicates.
func (j *JsonDBAnyKey[TK, TV]) Add(ctx context.Context, items []Item[TK, TV]) (bool, error) {
	j.compareError = nil
	allSucceeded := true
	for i := range items {
		if ok, err := j.BtreeInterface.Add(ctx, items[i].Key, *items[i].Value); err != nil {
			return false, err
		} else if !ok {
			allSucceeded = false
		}
		if j.compareError != nil {
			return false, j.compareError
		}
	}
	return allSucceeded, nil
}

// AddIfNotExist adds an item if there is no item matching the key yet.
// Otherwise, it will do nothing and return false, for not adding the item.
// This is useful for cases one wants to add an item without creating a duplicate entry.
func (j *JsonDBAnyKey[TK, TV]) AddIfNotExist(ctx context.Context, items []Item[TK, TV]) (bool, error) {
	j.compareError = nil
	allSucceeded := true
	for i := range items {
		if ok, err := j.BtreeInterface.AddIfNotExist(ctx, items[i].Key, *items[i].Value); err != nil {
			return false, err
		} else if !ok {
			allSucceeded = false
		}
		if j.compareError != nil {
			return false, j.compareError
		}
	}
	return allSucceeded, nil
}

// Update finds the item with key and update its value to the incoming value argument.
func (j *JsonDBAnyKey[TK, TV]) Update(ctx context.Context, items []Item[TK, TV]) (bool, error) {
	j.compareError = nil
	allSucceeded := true
	for i := range items {
		if ok, err := j.BtreeInterface.Update(ctx, items[i].Key, *items[i].Value); err != nil {
			return false, err
		} else if !ok {
			allSucceeded = false
		}
		if j.compareError != nil {
			return false, j.compareError
		}
	}
	return allSucceeded, nil
}

// Add if not exist or update item if it exists.
func (j *JsonDBAnyKey[TK, TV]) Upsert(ctx context.Context, items []Item[TK, TV]) (bool, error) {
	j.compareError = nil
	allSucceeded := true
	for i := range items {
		if ok, err := j.BtreeInterface.Upsert(ctx, items[i].Key, *items[i].Value); err != nil {
			return false, err
		} else if !ok {
			allSucceeded = false
		}
		if j.compareError != nil {
			return false, j.compareError
		}
	}
	return allSucceeded, nil
}

// Remove will find the item with a given key then remove that item.
func (j *JsonDBAnyKey[TK, TV]) Remove(ctx context.Context, keys []TK) (bool, error) {
	j.compareError = nil
	allSucceeded := true
	for i := range keys {
		if ok, err := j.BtreeInterface.Remove(ctx, keys[i]); err != nil {
			return false, err
		} else if !ok {
			allSucceeded = false
		}
		if j.compareError != nil {
			return false, j.compareError
		}
	}
	return allSucceeded, nil
}

func (j *JsonDBAnyKey[TK, TV]) GetKeys(ctx context.Context, pagingInfo PagingInfo) (string, error) {
	j.compareError = nil
	if j.BtreeInterface.GetCurrentKey().ID == sop.NilUUID {
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
				j.compareError = nil
				if pagingInfo.Direction == Forward {
					if ok, err := j.BtreeInterface.Next(ctx); err != nil {
						return "", err
					} else if !ok {
						return "", fmt.Errorf("reached the end of B-tree, no items fetched")
					}
					if j.compareError != nil {
						return "", j.compareError
					}
					continue
				}
				// Walk in backwards direction.
				if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
					return "", err
				} else if !ok {
					return "", fmt.Errorf("reached the top of B-tree, no items fetched")
				}
				if j.compareError != nil {
					return "", j.compareError
				}
			}
		}
	}

	// Fetch the data elements starting with current cursor location.
	fetchCount := pagingInfo.FetchCount
	if fetchCount <= 0 {
		fetchCount = pagingInfo.PageSize
	}
	keys := make([]Item[TK, TV], 0, fetchCount)
	for range fetchCount {
		j.compareError = nil

		key := j.BtreeInterface.GetCurrentKey()
		itm := Item[TK, TV]{
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
			if j.compareError != nil {
				return "", j.compareError
			}
			continue
		}
		if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
			p, _ := toJsonString(keys)
			return p, err
		} else if !ok {
			return toJsonString(keys)
		}
		if j.compareError != nil {
			return "", j.compareError
		}
	}

	// Package as JSON string the result.
	return toJsonString(keys)
}

func (j *JsonDBAnyKey[TK, TV]) GetItems(ctx context.Context, pagingInfo PagingInfo) (string, error) {
	if j.BtreeInterface.GetCurrentKey().ID == sop.NilUUID {
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
				j.compareError = nil
				if pagingInfo.Direction == Forward {
					if ok, err := j.BtreeInterface.Next(ctx); err != nil {
						return "", err
					} else if !ok {
						return "", fmt.Errorf("reached the end of B-tree, no items fetched")
					}
					if j.compareError != nil {
						return "", j.compareError
					}
					continue
				}
				// Walk in backwards direction.
				if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
					return "", err
				} else if !ok {
					return "", fmt.Errorf("reached the top of B-tree, no items fetched")
				}
				if j.compareError != nil {
					return "", j.compareError
				}
			}
		}
	}

	// Fetch the data elements starting with current cursor location.
	fetchCount := pagingInfo.FetchCount
	if fetchCount <= 0 {
		fetchCount = pagingInfo.PageSize
	}
	items := make([]Item[TK, TV], 0, fetchCount)
	for range fetchCount {
		item, err := j.BtreeInterface.GetCurrentItem(ctx)
		if err != nil {
			p, _ := toJsonString(items)
			return p, err
		}
		var itm Item[TK, TV]
		itm.extract(&item)
		items = append(items, itm)
		j.compareError = nil
		if pagingInfo.Direction == Forward {
			if ok, err := j.BtreeInterface.Next(ctx); err != nil {
				p, _ := toJsonString(items)
				return p, err
			} else if !ok {
				return toJsonString(items)
			}
			if j.compareError != nil {
				return "", j.compareError
			}
			continue
		}
		if ok, err := j.BtreeInterface.Previous(ctx); err != nil {
			p, _ := toJsonString(items)
			return p, err
		} else if !ok {
			return toJsonString(items)
		}
		if j.compareError != nil {
			return "", j.compareError
		}
	}

	// Package as JSON string the result.
	return toJsonString(items)
}

// GetCurrentValue returns the current item's value.
func (j *JsonDBAnyKey[TK, TV]) GetValues(ctx context.Context, keys []Item[TK, TV]) (string, error) {
	values := make([]Item[TK, TV], len(keys))
	for i := range keys {
		j.compareError = nil
		var err error
		var ok bool
		id := sop.UUID(keys[i].ID)
		if id.IsNil() {
			ok, err = j.BtreeInterface.Find(ctx, keys[i].Key, true)
		} else {
			ok, err = j.BtreeInterface.FindWithID(ctx, keys[i].Key, sop.UUID(keys[i].ID))
		}
		if err != nil {
			p, _ := toJsonString(values)
			return p, err
		} else if !ok {
			// Assign the source key to allow caller to deduce that item was not found, Value field is empty.
			values[i] = keys[i]
			if j.compareError != nil {
				return "", j.compareError
			}
			continue
		}
		if j.compareError != nil {
			return "", j.compareError
		}
		item, err := j.BtreeInterface.GetCurrentItem(ctx)
		if err != nil {
			p, _ := toJsonString(values)
			return p, err
		}
		values[i] = Item[TK, TV]{}
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
