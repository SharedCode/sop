package dynamic

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
)

// AddCentroid dynamically inserts a new explicit sub-category directly into the B-Tree.
func (s *store[T]) AddCentroid(ctx context.Context, c *Centroid) (sop.UUID, error) {
	if c == nil {
		return sop.UUID{}, fmt.Errorf("centroid cannot be nil")
	}

	// Validate/Ensure ID
	if c.ID.IsNil() {
		c.ID = sop.NewUUID()
	}

	// Lock the transaction on this insert in the centroids BTree
	ok, err := s.centroids.Add(ctx, c.ID, c)
	if err != nil {
		return sop.UUID{}, fmt.Errorf("failed to insert new centroid: %w", err)
	}
	if !ok {
		return sop.UUID{}, fmt.Errorf("centroid with ID %v already exists", c.ID)
	}

	// If this has a parent, we must update the parent's ChildrenIDs list to declare this hierarchy
	if !c.ParentID.IsNil() {
		ok, err := s.centroids.Find(ctx, c.ParentID, false)
		if err != nil {
			return sop.UUID{}, fmt.Errorf("failed to lookup parent centroid: %w", err)
		}
		if ok {
			parent, err := s.centroids.GetCurrentValue(ctx)
			if err != nil {
				return sop.UUID{}, fmt.Errorf("failed to fetch parent centroid value: %w", err)
			}
			// Link the child to the parent centroid hierarchy
			parent.ChildrenIDs = append(parent.ChildrenIDs, c.ID)
			_, err = s.centroids.UpdateCurrentItem(ctx, c.ParentID, parent)
			if err != nil {
				return sop.UUID{}, fmt.Errorf("failed to link centroid to parent: %w", err)
			}
		}
	}

	return c.ID, nil
}

// SplitCentroid reorganizes a centroid that has exceeded capacity boundaries.
// It selects two dispersed seeds, bifurcates the vectors mathematically,
// injects the two new centroids, wires them as Children of the original Centroid,
// and deletes the original mathematical points.
func (s *store[T]) SplitCentroid(ctx context.Context, centroidID sop.UUID) error {
	// 1. Target and Extract the Parent Centroid
	ok, err := s.centroids.Find(ctx, centroidID, false)
	if err != nil {
		return fmt.Errorf("failed to query splitting centroid: %w", err)
	}
	if !ok {
		return fmt.Errorf("centroid %v not found", centroidID)
	}
	origCentroid, err := s.centroids.GetCurrentValue(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch splitting centroid value: %w", err)
	}

	// 2. We need the list of Vectors assigned to this exact centroid
	// (To be fully complete, this would do a Range Query on the vectors BTree from VectorKey{OrigCentroidID} mapping)
	var vecs []Vector
	searchKey := VectorKey{CentroidID: centroidID}

	// Position the cursor at the first vector for this centroid
	ok, err = s.vectors.Find(ctx, searchKey, true)
	if err != nil {
		return fmt.Errorf("failed to position cursor for vectors: %w", err)
	}

	if ok {
		for {
			vk := s.vectors.GetCurrentKey()
			if vk.Key.CentroidID != centroidID {
				break // Moved past this centroid's vectors
			}
			v, err := s.vectors.GetCurrentValue(ctx)
			if err != nil {
				return fmt.Errorf("failed to get vector value: %w", err)
			}
			vecs = append(vecs, v)

			nextOk, nextErr := s.vectors.Next(ctx)
			if nextErr != nil || !nextOk {
				break
			}
		}
	}

	// Remove collected vectors from original centroid
	for _, v := range vecs {
		vk := VectorKey{
			CentroidID:         centroidID,
			DistanceToCentroid: EuclideanDistance(v.Data, origCentroid.CenterVector),
			VectorID:           v.ID,
		}
		_, _ = s.vectors.Remove(ctx, vk)
	}

	// Implementation scaffolded for Swarm Computing expansion:
	// A vector node will coordinate via these keys to physically distribute sub-buckets.
	_ = origCentroid
	_ = vecs

	return fmt.Errorf("SplitCentroid Swarm data bifurcation not strictly implemented yet")
}
