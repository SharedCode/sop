package ai

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/inredfs"
)

// BTreeModelStore implements ModelStore using SOP B-Trees for transactional persistence.
type BTreeModelStore struct {
	trans sop.Transaction
	store btree.BtreeInterface[string, string]
}

// NewBTreeModelStore creates a new B-Tree based model store.
// It requires an active transaction.
func NewBTreeModelStore(ctx context.Context, trans sop.Transaction) (*BTreeModelStore, error) {
	// We use a dedicated B-Tree named "models"
	store, err := inredfs.NewBtree[string, string](ctx, sop.ConfigureStore("models", true, 100, "AI Models Registry", sop.MediumData, ""), trans, func(a, b string) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if err != nil {
		return nil, err
	}
	return &BTreeModelStore{
		trans: trans,
		store: store,
	}, nil
}

// Save persists a model with the given name.
func (s *BTreeModelStore) Save(ctx context.Context, name string, model any) error {
	data, err := json.Marshal(model)
	if err != nil {
		return fmt.Errorf("failed to marshal model: %w", err)
	}
	// Upsert: Add or Update
	_, err = s.store.Upsert(ctx, name, string(data))
	return err
}

// Load retrieves a model by name and populates the provided object.
func (s *BTreeModelStore) Load(ctx context.Context, name string, target any) error {
	found, err := s.store.Find(ctx, name, false)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("model not found: %s", name)
	}
	data, err := s.store.GetCurrentValue(ctx)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), target)
}

// List returns the names of all stored models.
func (s *BTreeModelStore) List(ctx context.Context) ([]string, error) {
	var names []string
	if ok, err := s.store.First(ctx); err != nil {
		return nil, err
	} else if ok {
		for {
			item := s.store.GetCurrentKey()
			names = append(names, item.Key)
			if ok, err := s.store.Next(ctx); err != nil {
				return nil, err
			} else if !ok {
				break
			}
		}
	}
	return names, nil
}

// Delete removes a model from the store.
func (s *BTreeModelStore) Delete(ctx context.Context, name string) error {
	found, err := s.store.Find(ctx, name, false)
	if err != nil {
		return err
	}
	if found {
		_, err := s.store.RemoveCurrentItem(ctx)
		return err
	}
	return nil
}
