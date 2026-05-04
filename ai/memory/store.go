package memory

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
)

// store implements MemoryStore.
type store[T any] struct {
	categories btree.BtreeInterface[sop.UUID, *Category]
	vectors    btree.BtreeInterface[VectorKey, Vector]
	items      btree.BtreeInterface[sop.UUID, Item[T]]
	textIndex  ai.TextIndex
	llm        LLM[T]
	dedup      bool
}

// NewStore creates a new instance of MemoryStore.
func NewStore[T any](
	categories btree.BtreeInterface[sop.UUID, *Category],
	vectors btree.BtreeInterface[VectorKey, Vector],
	items btree.BtreeInterface[sop.UUID, Item[T]],
) MemoryStore[T] {
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

func (s *store[T]) SetLLM(l LLM[T]) {
	s.llm = l
}

func (s *store[T]) Upsert(ctx context.Context, item Item[T], vec []float32) error {
	id := item.ID
	if id == sop.NilUUID {
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
		var c *Category
		if s.llm != nil {
			if genCat, gErr := s.llm.GenerateCategory(ctx, item.Data); gErr == nil && genCat != nil {
				c = genCat
				if c.ID == sop.NilUUID {
					c.ID = sop.NewUUID()
				}
				if len(c.CenterVector) == 0 {
					c.CenterVector = vec
				}
			}
		}
		if c == nil {
			c = &Category{
				ID:           sop.NewUUID(),
				CenterVector: vec, // Initial category math uses the first vector
				Name:         "Default Root",
			}
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
				dist := EuclideanDistance(vec, c.CenterVector)
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

		// Active Memory Clustering: Re-evaluate category boundary via LLM if item's distance is too far from existing centers.
		if bestDist > 0.60 && s.llm != nil {
			if genCat, gErr := s.llm.GenerateCategory(ctx, item.Data); gErr == nil && genCat != nil {
				if genCat.ID == sop.NilUUID {
					genCat.ID = sop.NewUUID()
				}
				if len(genCat.CenterVector) == 0 {
					genCat.CenterVector = vec
				}
				if _, addErr := s.AddCategory(ctx, genCat); addErr == nil {
					bestCategory = genCat.ID
					// bestDist resets to perfect fit for the newly generated category
					bestDist = 0
				}
			}
		}
	}

	// 2. Insert into vectors tree
	vID := sop.NewUUID()
	v := Vector{
		ID:         vID,
		Data:       vec,
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
			Data:      item.Data,
			Positions: []VectorKey{vk},
		}
		_, err = s.items.Add(ctx, id, itemObj)
		if err != nil {
			return err
		}
	}

	// 4. Update TextIndex if present
	if s.textIndex != nil {
		
strData := ""
if m, ok := any(item.Data).(map[string]any); ok {
  strData = fmt.Sprintf("%v %v", m["text"], m["description"])
} else if st, ok := any(item.Data).(interface{ SearchText() string }); ok {
  strData = st.SearchText()
} else {
  strData = fmt.Sprintf("%v", item.Data)
}
		err = s.textIndex.Add(ctx, id.String(), strData)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpsertByCategory strictly inserts data assuming an explicit, fixed Category.
func (s *store[T]) UpsertByCategory(ctx context.Context, categoryName string, item Item[T], vecs [][]float32) error {
	id := item.ID

	// Ensure the Category exists
	var c *Category
	ok, _ := s.categories.First(ctx)
	if ok {
		for {
			cat, err := s.categories.GetCurrentValue(ctx)
			if err == nil && cat != nil && cat.Name == categoryName {
				c = cat
				break
			}
			ok, _ := s.categories.Next(ctx)
			if !ok {
				break
			}
		}
	}

	if c == nil {
		// Auto-vivify static category
		c = &Category{ID: sop.NewUUID(), Name: categoryName}
		s.categories.Add(ctx, c.ID, c)
	}

	// Insert Vector links
	var keys []VectorKey
	for _, vec := range vecs {
		vk := VectorKey{CategoryID: c.ID, VectorID: sop.NewUUID()}
		s.vectors.Add(ctx, vk, Vector{ID: vk.VectorID, ItemID: id, CategoryID: c.ID, Data: vec})
		keys = append(keys, vk)
	}

	// Insert Item
	if found, _ := s.items.Find(ctx, id, false); found {
		oldItem, _ := s.items.GetCurrentValue(ctx)
		// Clean up obsolete vectors from the B-Tree
		for _, pos := range oldItem.Positions {
			s.vectors.Remove(ctx, pos)
		}
		s.items.UpdateCurrentItem(ctx, id, Item[T]{ID: id, CategoryID: c.ID, Summaries: item.Summaries, Data: item.Data, Positions: keys})
	} else {
		s.items.Add(ctx, id, Item[T]{ID: id, CategoryID: c.ID, Summaries: item.Summaries, Data: item.Data, Positions: keys})
	}

	// Update global text index
	if s.textIndex != nil {
		
strData := ""
if m, ok := any(item.Data).(map[string]any); ok {
  strData = fmt.Sprintf("%v %v", m["text"], m["description"])
} else if st, ok := any(item.Data).(interface{ SearchText() string }); ok {
  strData = st.SearchText()
} else {
  strData = fmt.Sprintf("%v", item.Data)
}
		s.textIndex.Add(ctx, id.String(), strData) // Assumes item.ID actually contained textual representation.
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

func (s *store[T]) UpsertBatch(ctx context.Context, items []Item[T], vecs [][]float32) error {
	for i, item := range items {
		if err := s.Upsert(ctx, item, vecs[i]); err != nil {
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

func (s *store[T]) QueryBatch(ctx context.Context, vectors [][]float32, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	results := make([][]ai.Hit[T], len(vectors))
	for i, vec := range vectors {
		res, err := s.Query(ctx, vec, opts)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}
	return results, nil
}
func (s *store[T]) Query(ctx context.Context, vec []float32, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	if opts == nil {
		opts = &SearchOptions[T]{Limit: 10}
	}
	var targetCategory *Category
	var categories []*Category
	ok, err := s.categories.First(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // No data
	}

	for {
		c, err := s.categories.GetCurrentValue(ctx)
		if err == nil && c != nil {
			if opts.Category != "" && c.Name == opts.Category {
				targetCategory = c
				break
			}
			if opts.Category == "" {
				categories = append(categories, c)
			}
		}
		nextOk, nextErr := s.categories.Next(ctx)
		if nextErr != nil || !nextOk {
			break
		}
	}

	if opts.Category != "" {
		if targetCategory == nil {
			return nil, nil // Category strictly required but not found
		}
	} else {
		targetCategory, _ = FindClosestCategory(vec, categories)
	}

	if targetCategory == nil {
		return nil, nil
	}

	var hits []ai.Hit[T]
	ok, err = s.vectors.First(ctx)
	if err != nil {
		return nil, err
	}

	var matchingVectors []Vector
	if ok {
		for {
			vk := s.vectors.GetCurrentKey()
			// Need to verify CategoryID equality properly
			if vk.Key.CategoryID.Compare(targetCategory.ID) == 0 {
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
				if opts.Filter == nil || opts.Filter(item.Data) {
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

	if len(hits) > opts.Limit {
		hits = hits[:opts.Limit]
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

// QueryTextBatch performs a BM25 or keyword batch search on the stored text representation.
func (s *store[T]) QueryTextBatch(ctx context.Context, texts []string, opts *SearchOptions[T]) ([][]ai.Hit[T], error) {
	results := make([][]ai.Hit[T], len(texts))
	for i, txt := range texts {
		res, err := s.QueryText(ctx, txt, opts)
		if err != nil {
			return nil, err
		}
		results[i] = res
	}
	return results, nil
}

// QueryText performs a BM25 or keyword text search on the stored text representation of the thoughts.

func (s *store[T]) QueryText(ctx context.Context, text string, opts *SearchOptions[T]) ([]ai.Hit[T], error) {
	if opts == nil {
		opts = &SearchOptions[T]{Limit: 10}
	}
	if s.textIndex == nil {
		return nil, fmt.Errorf("text search is not enabled on this store")
	}

	var targetCategoryID sop.UUID
	if opts.Category != "" {
		ok, err := s.categories.First(ctx)
		if err == nil && ok {
			for {
				c, err := s.categories.GetCurrentValue(ctx)
				if err == nil && c != nil && c.Name == opts.Category {
					targetCategoryID = c.ID
					break
				}
				nextOk, _ := s.categories.Next(ctx)
				if !nextOk {
					break
				}
			}
		}
		if targetCategoryID == sop.NilUUID {
			return nil, nil // Category strictly requested but not found
		}
	}

	searchResults, err := s.textIndex.Search(ctx, text)
	if err != nil {
		return nil, fmt.Errorf("text search failed: %w", err)
	}

	var results []ai.Hit[T]
	for _, res := range searchResults {
		if len(results) >= opts.Limit {
			break
		}

		id, err := sop.ParseUUID(res.DocID)
		if err != nil {
			continue
		}
		foundItem, err := s.items.Find(ctx, id, false)
		var payload T
		if foundItem && err == nil {
			item, err := s.items.GetCurrentValue(ctx)
			if err == nil {
				// Category strictly requested, filtering...
				if opts.Category != "" && item.CategoryID.Compare(targetCategoryID) != 0 {
					continue
				}

				payload = item.Data
				if opts.Filter != nil && !opts.Filter(payload) {
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
