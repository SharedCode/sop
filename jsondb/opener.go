package jsondb

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/database"
)

// StoreAccessor provides a unified interface for accessing both primitive and JSON B-Trees.
type StoreAccessor interface {
	First(ctx context.Context) (bool, error)
	Last(ctx context.Context) (bool, error)
	Next(ctx context.Context) (bool, error)
	Previous(ctx context.Context) (bool, error)
	FindOne(ctx context.Context, key any, first bool) (bool, error)
	FindInDescendingOrder(ctx context.Context, key any) (bool, error)
	GetCurrentKey() (any, error)
	GetCurrentValue(ctx context.Context) (any, error)
	Add(ctx context.Context, key any, value any) (bool, error)
	Update(ctx context.Context, key any, value any) (bool, error)
	Remove(ctx context.Context, key any) (bool, error)
	GetStoreInfo() sop.StoreInfo
}

// OpenStore opens a B-Tree store, automatically detecting if it's a primitive or JSON store.
func OpenStore(ctx context.Context, dbOpts sop.DatabaseOptions, storeName string, tx sop.Transaction) (StoreAccessor, error) {
	var isPrimitiveKey bool

	if t2, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, storeName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
		}
	}

	if isPrimitiveKey {
		store, err := database.OpenBtreeCursor[string, any](ctx, dbOpts, storeName, tx, nil)
		if err != nil {
			return nil, err
		}
		return &primitiveStore{btree: store}, nil
	}

	store, err := OpenJsonBtreeMapKeyCursor(ctx, dbOpts, storeName, tx)
	if err != nil {
		return nil, err
	}
	return &jsonStore{btree: store}, nil
}

type primitiveStore struct {
	btree btree.BtreeInterface[string, any]
}

func (s *primitiveStore) First(ctx context.Context) (bool, error) { return s.btree.First(ctx) }
func (s *primitiveStore) Last(ctx context.Context) (bool, error)  { return s.btree.Last(ctx) }
func (s *primitiveStore) Next(ctx context.Context) (bool, error)  { return s.btree.Next(ctx) }
func (s *primitiveStore) Previous(ctx context.Context) (bool, error) {
	return s.btree.Previous(ctx)
}
func (s *primitiveStore) FindOne(ctx context.Context, key any, first bool) (bool, error) {
	k, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("key must be a string for primitive store")
	}
	return s.btree.Find(ctx, k, first)
}
func (s *primitiveStore) FindInDescendingOrder(ctx context.Context, key any) (bool, error) {
	k, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("key must be a string for primitive store")
	}
	return s.btree.FindInDescendingOrder(ctx, k)
}
func (s *primitiveStore) GetCurrentKey() (any, error) {
	k := s.btree.GetCurrentKey()
	return k.Key, nil
}
func (s *primitiveStore) GetCurrentValue(ctx context.Context) (any, error) {
	return s.btree.GetCurrentValue(ctx)
}
func (s *primitiveStore) Add(ctx context.Context, key any, value any) (bool, error) {
	k, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("key must be a string for primitive store")
	}
	return s.btree.Add(ctx, k, value)
}
func (s *primitiveStore) Update(ctx context.Context, key any, value any) (bool, error) {
	k, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("key must be a string for primitive store")
	}
	return s.btree.Update(ctx, k, value)
}
func (s *primitiveStore) Remove(ctx context.Context, key any) (bool, error) {
	k, ok := key.(string)
	if !ok {
		return false, fmt.Errorf("key must be a string for primitive store")
	}
	return s.btree.Remove(ctx, k)
}
func (s *primitiveStore) GetStoreInfo() sop.StoreInfo {
	return s.btree.GetStoreInfo()
}

type jsonStore struct {
	btree *JsonDBMapKey
}

func (s *jsonStore) First(ctx context.Context) (bool, error) { return s.btree.First(ctx) }
func (s *jsonStore) Last(ctx context.Context) (bool, error)  { return s.btree.Last(ctx) }
func (s *jsonStore) Next(ctx context.Context) (bool, error)  { return s.btree.Next(ctx) }
func (s *jsonStore) Previous(ctx context.Context) (bool, error) {
	return s.btree.Previous(ctx)
}
func (s *jsonStore) FindOne(ctx context.Context, key any, first bool) (bool, error) {
	k, ok := key.(map[string]any)
	if !ok {
		return false, fmt.Errorf("key must be a map[string]any for json store")
	}
	return s.btree.Find(ctx, k, first)
}
func (s *jsonStore) FindInDescendingOrder(ctx context.Context, key any) (bool, error) {
	k, ok := key.(map[string]any)
	if !ok {
		return false, fmt.Errorf("key must be a map[string]any for json store")
	}
	return s.btree.FindInDescendingOrder(ctx, k)
}
func (s *jsonStore) GetCurrentKey() (any, error) {
	return s.btree.BtreeInterface.GetCurrentKey().Key, nil
}
func (s *jsonStore) GetCurrentValue(ctx context.Context) (any, error) {
	return s.btree.BtreeInterface.GetCurrentValue(ctx)
}
func (s *jsonStore) Add(ctx context.Context, key any, value any) (bool, error) {
	k, ok := key.(map[string]any)
	if !ok {
		return false, fmt.Errorf("key must be a map[string]any for json store")
	}
	return s.btree.BtreeInterface.Add(ctx, k, value)
}
func (s *jsonStore) Update(ctx context.Context, key any, value any) (bool, error) {
	k, ok := key.(map[string]any)
	if !ok {
		return false, fmt.Errorf("key must be a map[string]any for json store")
	}
	return s.btree.BtreeInterface.Update(ctx, k, value)
}
func (s *jsonStore) Remove(ctx context.Context, key any) (bool, error) {
	k, ok := key.(map[string]any)
	if !ok {
		return false, fmt.Errorf("key must be a map[string]any for json store")
	}
	return s.btree.Remove(ctx, []map[string]any{k})
}
func (s *jsonStore) GetStoreInfo() sop.StoreInfo {
	return s.btree.GetStoreInfo()
}
