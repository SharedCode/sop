package model

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/infs"
)

// btreeModelStore implements ModelStore using SOP B-Trees.
// It manages its own transactions for operations if one is not provided.
type btreeModelStore struct {
	name          string
	externalTrans sop.Transaction
	openedStore   btree.BtreeInterface[ModelKey, string]
}

// New creates a new B-Tree based model store bound to a transaction.
func New(name string, trans sop.Transaction) ai.ModelStore {
	return &btreeModelStore{
		name:          name,
		externalTrans: trans,
	}
}

// WithTransaction returns a new instance of the store bound to the provided transaction.
func (s *btreeModelStore) WithTransaction(trans sop.Transaction) ai.ModelStore {
	return &btreeModelStore{
		name:          s.name,
		externalTrans: trans,
	}
}

// ModelKey is the composite key for the Model Store.
type ModelKey struct {
	Category string
	Name     string
}

func (s *btreeModelStore) getTransaction(ctx context.Context, mode sop.TransactionMode) (sop.Transaction, bool, error) {
	if s.externalTrans != nil {
		return s.externalTrans, false, nil
	}
	return nil, false, fmt.Errorf("no transaction provided")
}

func (s *btreeModelStore) openStore(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[ModelKey, string], error) {
	if s.externalTrans != nil && s.openedStore != nil {
		return s.openedStore, nil
	}
	// Prefix the store name with the domain name to allow multiple stores in the same folder.
	storeName := fmt.Sprintf("%s_models", s.name)
	so := sop.ConfigureStore(storeName, true, 500, "AI Models Registry", sop.MediumData, "")
	comparer := func(a, b ModelKey) int {
		if a.Category < b.Category {
			return -1
		}
		if a.Category > b.Category {
			return 1
		}
		if a.Name < b.Name {
			return -1
		}
		if a.Name > b.Name {
			return 1
		}
		return 0
	}

	store, err := infs.NewBtree[ModelKey, string](ctx, so, trans, comparer)
	if err != nil {
		if err.Error() == "failed in NewBtree as transaction has replication enabled, use NewBtreeWithReplication instead" {
			store, err = infs.NewBtreeWithReplication[ModelKey, string](ctx, so, trans, comparer)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	if s.externalTrans != nil {
		s.openedStore = store
	}
	return store, nil
}

// Save persists a model with the given name and category.
func (s *btreeModelStore) Save(ctx context.Context, category string, name string, model any) error {
	trans, isOwn, err := s.getTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	store, err := s.openStore(ctx, trans)
	if err != nil {
		return err
	}

	data, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %w", err)
	}

	key := ModelKey{Category: category, Name: name}
	if _, err := store.Upsert(ctx, key, string(data)); err != nil {
		return err
	}

	if isOwn {
		return trans.Commit(ctx)
	}
	return nil
}

// Load retrieves a model by name and category.
func (s *btreeModelStore) Load(ctx context.Context, category string, name string, target any) error {
	trans, isOwn, err := s.getTransaction(ctx, sop.ForReading)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	store, err := s.openStore(ctx, trans)
	if err != nil {
		return err
	}

	key := ModelKey{Category: category, Name: name}
	found, err := store.Find(ctx, key, false)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("model not found: %s/%s", category, name)
	}

	data, err := store.GetCurrentValue(ctx)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(data), target)
}

// List returns the names of all stored models in a given category.
func (s *btreeModelStore) List(ctx context.Context, category string) ([]string, error) {
	trans, isOwn, err := s.getTransaction(ctx, sop.ForReading)
	if err != nil {
		return nil, err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	store, err := s.openStore(ctx, trans)
	if err != nil {
		return nil, err
	}

	var names []string
	// Start search at the beginning of the category
	startKey := ModelKey{Category: category, Name: ""}
	found, err := store.Find(ctx, startKey, true) // FindOne with prefix-like behavior? No, FindOne finds exact or next.
	if err != nil {
		return nil, err
	}
	// If not found, it might have positioned us at the next item.
	// We need to check if the current item matches the category.

	// If the store is empty or we are past the end, found might be false but we need to check current item?
	// SOP Find(..., true) positions the cursor.

	if !found {
		// If not found, check if we are at a valid position
		if store.GetCurrentKey().Key.Category != category {
			// Maybe the category doesn't exist at all, or we are past it.
			return nil, nil
		}
	}

	// Iterate while category matches
	for {
		key := store.GetCurrentKey().Key
		if key.Category != category {
			break
		}
		names = append(names, key.Name)

		if ok, err := store.Next(ctx); err != nil {
			return nil, err
		} else if !ok {
			break
		}
	}

	return names, nil
}

// Delete removes a model from the store.
func (s *btreeModelStore) Delete(ctx context.Context, category string, name string) error {
	trans, isOwn, err := s.getTransaction(ctx, sop.ForWriting)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(ctx)
	}

	store, err := s.openStore(ctx, trans)
	if err != nil {
		return err
	}

	key := ModelKey{Category: category, Name: name}
	found, err := store.Find(ctx, key, false)
	if err != nil {
		return err
	}
	if found {
		if _, err := store.RemoveCurrentItem(ctx); err != nil {
			return err
		}
	}

	if isOwn {
		return trans.Commit(ctx)
	}
	return nil
}
