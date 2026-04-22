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
	registry  btree.BtreeInterface[sop.UUID, Handle]
	centroids btree.BtreeInterface[sop.UUID, *Centroid]
	vectors   btree.BtreeInterface[VectorKey, Vector]
	content   btree.BtreeInterface[ContentKey, Payload[T]]
	textIndex ai.TextIndex
	dedup     bool
}

// NewStore creates a new instance of DynamicVectorStore.
func NewStore[T any](
	
	centroids btree.BtreeInterface[sop.UUID, *Centroid],
	vectors btree.BtreeInterface[VectorKey, Vector],
	content btree.BtreeInterface[ContentKey, Payload[T]],
) DynamicVectorStore[T] {
	return &store[T]{
		
		centroids: centroids,
		vectors:   vectors,
		content:   content,
		dedup:     true,
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

	// 1. Find nearest centroid
	var bestCentroid sop.UUID
	var bestDist float32 = -1
	ok, err := s.centroids.First(ctx)
	if err != nil {
		return err
	}
	if !ok {
		// Create a root centroid if none exists
		c := &Centroid{
			ID:           sop.NewUUID(),
			CenterVector: item.Vector, // Initial centroid math uses the first vector
			Name:         "Default Root",
		}
		_, err = s.AddCentroid(ctx, c)
		if err != nil {
			return err
		}
		bestCentroid = c.ID
		bestDist = 0
	} else {
		for {
			c, err := s.centroids.GetCurrentValue(ctx)
			if err == nil && c != nil {
				dist := EuclideanDistance(item.Vector, c.CenterVector)
				if bestDist == -1 || dist < bestDist {
					bestDist = dist
					bestCentroid = c.ID
				}
			}
			nextOk, nextErr := s.centroids.Next(ctx)
			if nextErr != nil || !nextOk {
				break
			}
		}
	}

	// 2. Insert into vectors tree
	vID := sop.NewUUID()
	v := Vector{
		ID:        vID,
		Data:      item.Vector,
		PayloadID: id,
	}
	vk := VectorKey{
		CentroidID:         bestCentroid,
		DistanceToCentroid: bestDist,
		VectorID:           vID,
	}
	_, err = s.vectors.Add(ctx, vk, v)
	if err != nil {
		return err
	}

	// 3. Insert into content tree
	ck := ContentKey{
		VectorID:   vID,
		PayloadID:  id,
		CentroidID: bestCentroid,
		Distance:   bestDist,
	}
	payload := Payload[T]{
		ID:   id,
		Data: item.Payload,
	}
	_, err = s.content.Add(ctx, ck, payload)
	return err
}

func (s *store[T]) UpsertBatch(ctx context.Context, items []ai.Item[T]) error {
	return fmt.Errorf("not implemented")
}

func (s *store[T]) Get(ctx context.Context, id sop.UUID) (*Payload[T], error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *store[T]) Delete(ctx context.Context, id sop.UUID) error {
	return fmt.Errorf("not implemented")
}

func (s *store[T]) Query(ctx context.Context, vec []float32, k int, filter func(T) bool) ([]ai.Hit[T], error) {
	var centroids []*Centroid
	ok, err := s.centroids.First(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil // No data
	}

	// Gather all top level centroids. Wait, for now we just get all root centroids.
	for {
		c, err := s.centroids.GetCurrentValue(ctx)
		if err == nil && c != nil {
			centroids = append(centroids, c)
		}
		nextOk, nextErr := s.centroids.Next(ctx)
		if nextErr != nil || !nextOk {
			break
		}
	}

	// Find best centroid
	bestCentroid, _ := FindClosestCentroid(vec, centroids)
	if bestCentroid == nil {
		return nil, nil
	}

	var hits []ai.Hit[T]
	searchKey := VectorKey{CentroidID: bestCentroid.ID}

	ok, err = s.vectors.Find(ctx, searchKey, true)
	if err != nil {
		return nil, err
	}

	if ok {
		for {
			vk := s.vectors.GetCurrentKey()
			if vk.Key.CentroidID != bestCentroid.ID {
				break
			}
			v, err := s.vectors.GetCurrentValue(ctx)
			if err == nil {
				// Fetch payload
				ck := ContentKey{
					VectorID:   v.ID,
					PayloadID:  v.PayloadID,
					CentroidID: bestCentroid.ID,
					Distance:   vk.Key.DistanceToCentroid,
				}
				foundPayload, err := s.content.Find(ctx, ck, true)
				if foundPayload && err == nil {
					payload, err := s.content.GetCurrentValue(ctx)
					if err == nil {
						if filter == nil || filter(payload.Data) {
							hits = append(hits, ai.Hit[T]{
								ID:      payload.ID.String(),
								Score:   EuclideanDistance(vec, v.Data),
								Payload: payload.Data,
							})
						}
					}
				}
			}

			nextOk, nextErr := s.vectors.Next(ctx)
			if nextErr != nil || !nextOk {
				break
			}
		}
	}

	// Sort by score ascending (lower is better for Euclidean)
	// If sorting were cosine, it'd be reversed. Assuming Euclidean:
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
	return 0, fmt.Errorf("not implemented")
}

func (s *store[T]) Centroids(ctx context.Context) (btree.BtreeInterface[sop.UUID, *Centroid], error) {
	return s.centroids, nil
}

func (s *store[T]) Consolidate(ctx context.Context) error {
	return fmt.Errorf("not implemented")
}

func (s *store[T]) UpdateEmbedderInfo(ctx context.Context, provider string, model string, dimensions int) error {
	return fmt.Errorf("not implemented")
}

func (s *store[T]) SetDeduplication(enabled bool) {
	s.dedup = enabled
}

func (s *store[T]) Vectors(ctx context.Context) (btree.BtreeInterface[VectorKey, Vector], error) {
	return s.vectors, nil
}

func (s *store[T]) Content(ctx context.Context) (btree.BtreeInterface[ContentKey, Payload[T]], error) {
	return s.content, nil
}

func (s *store[T]) Version(ctx context.Context) (int64, error) {
	return 0, fmt.Errorf("not implemented")
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

		results = append(results, ai.Hit[T]{
			ID:    res.DocID,
			Score: float32(res.Score),
		})
	}
	return results, nil
}

func (s *store[T]) Registry(ctx context.Context) (btree.BtreeInterface[sop.UUID, Handle], error) {
	return s.registry, nil
}
