package dynamic

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
)

// store implements DynamicVectorStore.
type store[T any] struct {
	registry   btree.BtreeInterface[sop.UUID, Handle]
	categories btree.BtreeInterface[sop.UUID, *Category]
	vectors    btree.BtreeInterface[VectorKey, Vector]
	items      btree.BtreeInterface[sop.UUID, Item[T]]
	textIndex  ai.TextIndex
	dedup      bool
}

// NewStore creates a new instance of DynamicVectorStore.
func NewStore[T any](
	categories btree.BtreeInterface[sop.UUID, *Category],
	vectors btree.BtreeInterface[VectorKey, Vector],
	items btree.BtreeInterface[sop.UUID, Item[T]],
) DynamicVectorStore[T] {
	return &store[T]{
		categories: categories,
		vectors:    vectors,
		items:      items,
		dedup:      true,
	}
}

func (s *store[T]) SetTextIndex(idx ai.TextIndex) {
	s.textIndex = idx
}

func (s *store[T]) Upsert(ctx context.Context, item ai.Item[T]) error {
	id, err := sop.ParseUUID(item.ID)
	if err != nil {
		id = sop.NewUUID()
	}

	// 1. Find nearest category
	var bestCategory sop.UUID
	var bestDist float32 = -1
	ok, err := s.categories.First(ctx)
	if err != nil {
		return err
	}
	if !ok {
		// Create a root category if none exists
		c := &Category{
			ID:           sop.NewUUID(),
			CenterVector: item.Vector, // Initial category math uses the first vector
			Name:         "Default Root",
		}
		_, err = s.AddCategory(ctx, c)
		if err != nil {
			return err
		}
		bestCategory = c.ID
		bestDist = 0
	} else {
		for {
			c, err := s.categories.GetCurrentValue(ctx)
			if err == nil && c != nil {
				dist := EuclideanDistance(item.Vector, c.CenterVector)
				if bestDist == -1 || dist < bestDist {
					bestDist = dist
					bestCategory = c.ID
				}
			}
			nextOk, nextErr := s.categories.Next(ctx)
			if nextErr != nil || !nextOk {
				break
			}
		}
	}

	// 2. Insert into vectors tree
	vID := sop.NewUUID()
	v := Vector{
		ID:         vID,
		Data:       item.Vector,
		ItemID:     id,
		CategoryID: bestCategory,
	}
	vk := VectorKey{
		CategoryID:         bestCategory,
		DistanceToCategory: bestDist,
		VectorID:           vID,
	}
	_, err = s.vectors.Add(ctx, vk, v)
	if err != nil {
		return err
	}

	// 3. Insert into items tree
	foundItem, err := s.items.Find(ctx, id, false)
	if err != nil {
		return err
	}

	if foundItem {
		existingItem, err := s.items.GetCurrentValue(ctx)
		if err != nil {
			return err
		}
		existingItem.Positions = append(existingItem.Positions, vk)
		_, err = s.items.UpdateCurrentItem(ctx, id, existingItem)
		if err != nil {
			return err
		}
	} else {
		itemObj := Item[T]{
			ID:        id,
			Data:      item.Payload,
			Positions: []VectorKey{vk},
		}
		_, err = s.items.Add(ctx, id, itemObj)
		if err != nil {
			return err
		}
	}

	// 4. Update TextIndex if present
	if s.textIndex != nil {
		strData := fmt.Sprintf("%v", item.Payload)
		err = s.textIndex.Add(ctx, id.String(), strData)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *store[T]) DeleteItem(ctx context.Context, itemID sop.UUID) error {
	found, err := s.items.Find(ctx, itemID, false)
	if err != nil {
		return err
	}
	if !found {
		return nil // already deleted
	}

	item, err := s.items.GetCurrentValue(ctx)
	if err != nil {
		return err
	}

	// Clean up all associated vectors in O(1) time
	for _, pos := range item.Positions {
		_, err := s.vectors.Remove(ctx, pos)
		if err != nil {
			return err
		}
	}

	// Finally, remove the item itself
	_, err = s.items.Remove(ctx, itemID)
	return err
}

func (s *store[T]) UpsertBatch(ctx context.Context, items []ai.Item[T]) error {
	for _, item := range items {
		if err := s.Upsert(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func (s *store[T]) Get(ctx context.Context, id sop.UUID) (*Item[T], error) {
	found, err := s.items.Find(ctx, id, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("item not found")
	}

	item, err := s.items.GetCurrentValue(ctx)
	if err != nil {
		return nil, err
	}

	return &item, nil
}

func (s *store[T]) Delete(ctx context.Context, id sop.UUID) error {
	return s.DeleteItem(ctx, id)
}

func (s *store[T]) Query(ctx context.Context, vec []float32, k int, filter func(T) bool) ([]ai.Hit[T], error) {
	var categories []*Category
	ok, err := s.categories.First(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // No data
	}

	// Gather all top level categories. Wait, for now we just get all root categories.
	for {
		c, err := s.categories.GetCurrentValue(ctx)
		if err == nil && c != nil {
			categories = append(categories, c)
		}
		nextOk, nextErr := s.categories.Next(ctx)
		if nextErr != nil || !nextOk {
			break
		}
	}

	// Find best category
	bestCategory, _ := FindClosestCategory(vec, categories)
	if bestCategory == nil {
		return nil, nil
	}

	var hits []ai.Hit[T]

	// Instead of seeking, let's just collect ALL items that exist with this CategoryID.
	// Since we mock and could have cursor issues, let's gather keys.
	ok, err = s.vectors.First(ctx)
	if err != nil {
		return nil, err
	}

	var matchingVectors []Vector
	if ok {
		for {
			vk := s.vectors.GetCurrentKey()
			// Need to verify CategoryID equality properly
			if vk.Key.CategoryID.Compare(bestCategory.ID) == 0 {
				v, err := s.vectors.GetCurrentValue(ctx)
				if err == nil {
					matchingVectors = append(matchingVectors, v)
				}
			}

			nextOk, nextErr := s.vectors.Next(ctx)
			if nextErr != nil || !nextOk {
				break
			}
		}
	}

	// Fetch items after we're done iterating vectors to avoid ANY cursor conflict
	for _, v := range matchingVectors {
		foundItem, err := s.items.Find(ctx, v.ItemID, false)
		if foundItem && err == nil {
			item, err := s.items.GetCurrentValue(ctx)
			if err == nil {
				// Let's print out what we found
				// fmt.Printf("Found item: %v\n", item.Data)
				if filter == nil || filter(item.Data) {
					hits = append(hits, ai.Hit[T]{
						ID:      item.ID.String(),
						Score:   EuclideanDistance(vec, v.Data),
						Payload: item.Data,
					})
				}
			}
		}
	}

	// Sort hits by score ascending (Euclidean distance: lower is better)
	for i := 0; i < len(hits); i++ {
		for j := i + 1; j < len(hits); j++ {
			if hits[i].Score > hits[j].Score {
				hits[i], hits[j] = hits[j], hits[i]
			}
		}
	}

	if len(hits) > k {
		hits = hits[:k]
	}
	return hits, nil
}

func (s *store[T]) Count(ctx context.Context) (int64, error) {
	return s.items.Count(), nil
}

func (s *store[T]) Categories(ctx context.Context) (btree.BtreeInterface[sop.UUID, *Category], error) {
	return s.categories, nil
}

func (s *store[T]) Consolidate(ctx context.Context) error {
	return nil
}

func (s *store[T]) UpdateEmbedderInfo(ctx context.Context, provider string, model string, dimensions int) error {
	return nil
}

func (s *store[T]) SetDeduplication(enabled bool) {
	s.dedup = enabled
}

func (s *store[T]) Vectors(ctx context.Context) (btree.BtreeInterface[VectorKey, Vector], error) {
	return s.vectors, nil
}

func (s *store[T]) Items(ctx context.Context) (btree.BtreeInterface[sop.UUID, Item[T]], error) {
	return s.items, nil
}

func (s *store[T]) Version(ctx context.Context) (int64, error) {
	return 0, nil
}

// QueryText performs a BM25 or keyword text search on the stored text representation of the thoughts.
func (s *store[T]) QueryText(ctx context.Context, text string, k int, filter func(T) bool) ([]ai.Hit[T], error) {
	if s.textIndex == nil {
		return nil, fmt.Errorf("text search is not enabled on this store")
	}

	searchResults, err := s.textIndex.Search(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("text search failed: %w", err)
	}

	var results []ai.Hit[T]
	for _, res := range searchResults {
		if len(results) >= k {
			break
		}

		// Fetch the payload directly from items tree since we have an ID
		id, err := sop.ParseUUID(res.DocID)
		if err != nil {
			continue
		}
		foundItem, err := s.items.Find(ctx, id, false)
		var payload T
		if foundItem && err == nil {
			item, err := s.items.GetCurrentValue(ctx)
			if err == nil {
				payload = item.Data
				if filter != nil && !filter(payload) {
					continue
				}
			}
		}

		results = append(results, ai.Hit[T]{
			ID:      res.DocID,
			Score:   float32(res.Score),
			Payload: payload,
		})
	}
	return results, nil
}
