package index

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/cache"

	"github.com/sharedcode/sop/ai/internal/port"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/inredfs"
)

// SOPIndex implements port.VectorIndex using the SOP B-Tree.
// It persists vectors and metadata to disk/Redis via SOP.
type SOPIndex struct {
	name  string
	ctx   context.Context
	cache sop.Cache
}

// storedItem is the schema for the value stored in the B-Tree.
type storedItem struct {
	Vector []float32      `json:"v"`
	Meta   map[string]any `json:"m"`
}

func NewSOP(name string) *SOPIndex {
	// Ensure we use InMemoryCache globally for the doctor app
	sop.SetCacheFactory(sop.InMemory)
	return &SOPIndex{
		name:  name,
		ctx:   context.Background(),
		cache: cache.NewInMemoryCache(),
	}
}

func (s *SOPIndex) Upsert(id string, vec []float32, meta map[string]any) error {
	// 1. Start Transaction
	trans, err := s.beginTransaction(false)
	if err != nil {
		return err
	}
	defer trans.Rollback(s.ctx)

	// 2. Open B-Tree
	b3, err := s.openBtree(trans, s.name)
	if err != nil {
		return err
	}

	// 3. Serialize Data
	item := storedItem{Vector: vec, Meta: meta}
	val, err := json.Marshal(item)
	if err != nil {
		return fmt.Errorf("marshal failed: %w", err)
	}

	// 4. Store in B-Tree
	// Key: id (e.g., "doc1")
	// Value: JSON blob
	if ok, err := b3.Add(s.ctx, id, val); err != nil {
		return err
	} else if !ok {
		// Item might exist, try Update
		if _, err := b3.Update(s.ctx, id, val); err != nil {
			return err
		}
	}

	// 5. Commit
	if err := trans.Commit(s.ctx); err != nil {
		return err
	}
	return nil
}

func (s *SOPIndex) Query(vec []float32, k int, filters map[string]any) ([]port.Hit, error) {
	// 1. Start Read-Only Transaction
	trans, err := s.beginTransaction(true)
	if err != nil {
		return nil, err
	}
	defer trans.Rollback(s.ctx)

	// 2. Open B-Tree
	b3, err := s.openBtree(trans, s.name)
	if err != nil {
		return nil, err
	}

	// 3. Scan and Calculate Cosine Similarity
	// NOTE: This is a naive O(N) scan. For production, we would use an HNSW index or similar,
	// or store the vectors in a way that allows pruning.
	// Since SOP is a B-Tree, we iterate all items.
	var hits []port.Hit

	// Iterate all items
	if ok, err := b3.First(s.ctx); ok && err == nil {
		for {
			itemKey := b3.GetCurrentKey()
			key := itemKey.Key
			vBytes, err := b3.GetCurrentValue(s.ctx)
			if err != nil {
				break
			}

			var item storedItem
			if err := json.Unmarshal(vBytes, &item); err != nil {
				continue
			}

			// Apply Filters
			if !matchFilters(item.Meta, filters) {
				if ok, _ := b3.Next(s.ctx); !ok {
					break
				}
				continue
			}

			// Calculate Score
			score := cosine(vec, item.Vector)
			hits = append(hits, port.Hit{ID: key, Score: score, Meta: item.Meta})

			if ok, _ := b3.Next(s.ctx); !ok {
				break
			}
		}
	}

	// 4. Sort and Top-K
	sort.Slice(hits, func(i, j int) bool { return hits[i].Score > hits[j].Score })
	if k > len(hits) {
		k = len(hits)
	}
	return hits[:k], nil
}

func (s *SOPIndex) Delete(id string) error {
	trans, err := s.beginTransaction(false)
	if err != nil {
		return err
	}
	defer trans.Rollback(s.ctx)

	b3, err := s.openBtree(trans, s.name)
	if err != nil {
		return err
	}

	if _, err := b3.Remove(s.ctx, id); err != nil {
		return err
	}

	return trans.Commit(s.ctx)
}

// Helper functions for SOP boilerplate
func (s *SOPIndex) beginTransaction(readOnly bool) (sop.Transaction, error) {
	// TODO: Make this configurable
	// We use ai/data so it's inside the project structure we see
	storeFolder := "./ai/data/sop"

	if err := os.MkdirAll(storeFolder, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data folder %s: %w", storeFolder, err)
	}

	mode := sop.ForWriting
	if readOnly {
		mode = sop.ForReading
	}

	to, err := inredfs.NewTransactionOptions(storeFolder, mode, -1, -1)
	if err != nil {
		return nil, err
	}

	to.Cache = s.cache

	trans, err := inredfs.NewTransaction(s.ctx, to)
	if err != nil {
		return nil, err
	}

	if err := trans.Begin(s.ctx); err != nil {
		return nil, fmt.Errorf("transaction begin failed: %w", err)
	}

	return trans, nil
}

func (s *SOPIndex) openBtree(trans sop.Transaction, name string) (btree.BtreeInterface[string, []byte], error) {
	return inredfs.NewBtree[string, []byte](s.ctx, sop.StoreOptions{
		Name:       name,
		SlotLength: 100,
	}, trans, nil)
}

func matchFilters(meta, filters map[string]any) bool {
	for k, v := range filters {
		if mv, ok := meta[k]; !ok || mv != v {
			return false
		}
	}
	return true
}
