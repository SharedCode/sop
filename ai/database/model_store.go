package database

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/inredfs"
)

// btreeModelStore implements ModelStore using SOP B-Trees.
// It manages its own transactions for operations if one is not provided.
type btreeModelStore struct {
	db            *Database
	name          string
	externalTrans sop.Transaction
}

// NewBTreeModelStore creates a new B-Tree based model store bound to a transaction.
// It uses a default name "global" for the store.
func NewBTreeModelStore(ctx context.Context, trans sop.Transaction) (ai.ModelStore, error) {
	return &btreeModelStore{
		name:          "global",
		externalTrans: trans,
	}, nil
}

// WithTransaction returns a new instance of the store bound to the provided transaction.
func (s *btreeModelStore) WithTransaction(trans sop.Transaction) ai.ModelStore {
	return &btreeModelStore{
		db:            s.db,
		name:          s.name,
		externalTrans: trans,
	}
}

// ModelKey is the composite key for the Model Store.
type ModelKey struct {
	Category string
	Name     string
}

func (s *btreeModelStore) getTransaction(mode sop.TransactionMode) (sop.Transaction, bool, error) {
	if s.externalTrans != nil {
		return s.externalTrans, false, nil
	}

	// Use the database root path for the transaction to allow global access (e.g. sys_config)
	// and domain-specific sub-stores.
	storeFolder := s.db.storagePath
	if err := os.MkdirAll(storeFolder, 0755); err != nil {
		return nil, false, fmt.Errorf("failed to create data folder %s: %w", storeFolder, err)
	}

	if len(s.db.storesFolders) > 0 {
		to, err := inredfs.NewTransactionOptionsWithReplication(mode, -1, -1, s.db.storesFolders, s.db.erasureConfig)
		if err != nil {
			return nil, false, err
		}
		to.Cache = s.db.cache
		trans, err := inredfs.NewTransactionWithReplication(s.db.ctx, to)
		if err != nil {
			return nil, false, err
		}
		if err := trans.Begin(s.db.ctx); err != nil {
			return nil, false, fmt.Errorf("transaction begin failed: %w", err)
		}
		return trans, true, nil
	}

	to, err := inredfs.NewTransactionOptions(storeFolder, mode, -1, -1)
	if err != nil {
		return nil, false, err
	}
	to.Cache = s.db.cache

	trans, err := inredfs.NewTransaction(s.db.ctx, to)
	if err != nil {
		return nil, false, err
	}

	if err := trans.Begin(s.db.ctx); err != nil {
		return nil, false, fmt.Errorf("transaction begin failed: %w", err)
	}

	return trans, true, nil
}

func (s *btreeModelStore) openStore(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[ModelKey, string], error) {
	// Prefix the store name with the domain name to allow multiple stores in the same folder.
	storeName := fmt.Sprintf("%s_models", s.name)
	so := sop.ConfigureStore(storeName, true, 100, "AI Models Registry", sop.MediumData, "")
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

	store, err := inredfs.NewBtree[ModelKey, string](ctx, so, trans, comparer)
	if err != nil {
		store, err = inredfs.NewBtreeWithReplication[ModelKey, string](ctx, so, trans, comparer)
		if err != nil {
			return nil, err
		}
	}
	return store, nil
}

// Save persists a model with the given name and category.
func (s *btreeModelStore) Save(ctx context.Context, category string, name string, model any) error {
	trans, isOwn, err := s.getTransaction(sop.ForWriting)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(s.db.ctx)
	}

	store, err := s.openStore(s.db.ctx, trans)
	if err != nil {
		return err
	}

	data, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %w", err)
	}

	key := ModelKey{Category: category, Name: name}
	if _, err := store.Upsert(s.db.ctx, key, string(data)); err != nil {
		return err
	}

	if isOwn {
		return trans.Commit(s.db.ctx)
	}
	return nil
}

// Load retrieves a model by name and category.
func (s *btreeModelStore) Load(ctx context.Context, category string, name string, target any) error {
	trans, isOwn, err := s.getTransaction(sop.ForReading)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(s.db.ctx)
	}

	store, err := s.openStore(s.db.ctx, trans)
	if err != nil {
		return err
	}

	key := ModelKey{Category: category, Name: name}
	found, err := store.Find(s.db.ctx, key, false)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("model not found: %s/%s", category, name)
	}

	data, err := store.GetCurrentValue(s.db.ctx)
	if err != nil {
		return err
	}

	return json.Unmarshal([]byte(data), target)
}

// List returns the names of all stored models in a given category.
func (s *btreeModelStore) List(ctx context.Context, category string) ([]string, error) {
	trans, isOwn, err := s.getTransaction(sop.ForReading)
	if err != nil {
		return nil, err
	}
	if isOwn {
		defer trans.Rollback(s.db.ctx)
	}

	store, err := s.openStore(s.db.ctx, trans)
	if err != nil {
		return nil, err
	}

	var names []string
	// Start search at the beginning of the category
	startKey := ModelKey{Category: category, Name: ""}
	found, err := store.Find(s.db.ctx, startKey, true) // FindOne with prefix-like behavior? No, FindOne finds exact or next.
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

		if ok, err := store.Next(s.db.ctx); err != nil {
			return nil, err
		} else if !ok {
			break
		}
	}

	return names, nil
}

// Delete removes a model from the store.
func (s *btreeModelStore) Delete(ctx context.Context, category string, name string) error {
	trans, isOwn, err := s.getTransaction(sop.ForWriting)
	if err != nil {
		return err
	}
	if isOwn {
		defer trans.Rollback(s.db.ctx)
	}

	store, err := s.openStore(s.db.ctx, trans)
	if err != nil {
		return err
	}

	key := ModelKey{Category: category, Name: name}
	found, err := store.Find(s.db.ctx, key, false)
	if err != nil {
		return err
	}
	if found {
		if _, err := store.RemoveCurrentItem(s.db.ctx); err != nil {
			return err
		}
	}

	if isOwn {
		return trans.Commit(s.db.ctx)
	}
	return nil
}
