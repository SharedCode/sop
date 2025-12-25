package jsondb

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/database"
)

// StoreAccessor provides a unified interface for accessing both primitive and JSON B-Trees.
type StoreAccessor interface {
	First(ctx context.Context) (bool, error)
	Next(ctx context.Context) (bool, error)
	GetCurrentKey() (any, error)
	GetCurrentValue(ctx context.Context) (any, error)
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
		store, err := database.OpenBtree[string, any](ctx, dbOpts, storeName, tx, nil)
		if err != nil {
			return nil, err
		}
		return &primitiveStore{btree: store}, nil
	}

	store, err := OpenJsonBtreeMapKey(ctx, dbOpts, storeName, tx)
	if err != nil {
		return nil, err
	}
	return &jsonStore{btree: store}, nil
}

type primitiveStore struct {
	btree btree.BtreeInterface[string, any]
}

func (s *primitiveStore) First(ctx context.Context) (bool, error) { return s.btree.First(ctx) }
func (s *primitiveStore) Next(ctx context.Context) (bool, error)  { return s.btree.Next(ctx) }
func (s *primitiveStore) GetCurrentKey() (any, error) {
	k := s.btree.GetCurrentKey()
	return k.Key, nil
}
func (s *primitiveStore) GetCurrentValue(ctx context.Context) (any, error) {
	return s.btree.GetCurrentValue(ctx)
}

type jsonStore struct {
	btree *JsonDBMapKey
}

func (s *jsonStore) First(ctx context.Context) (bool, error) { return s.btree.First(ctx) }
func (s *jsonStore) Next(ctx context.Context) (bool, error)  { return s.btree.Next(ctx) }
func (s *jsonStore) GetCurrentKey() (any, error) {
	return s.btree.BtreeInterface.GetCurrentKey().Key, nil
}
func (s *jsonStore) GetCurrentValue(ctx context.Context) (any, error) {
	return s.btree.BtreeInterface.GetCurrentValue(ctx)
}
