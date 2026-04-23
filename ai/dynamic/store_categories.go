package dynamic

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
)

// AddCategory dynamically inserts a new explicit sub-category directly into the B-Tree.
func (s *store[T]) AddCategory(ctx context.Context, c *Category) (sop.UUID, error) {
	if c == nil {
		return sop.UUID{}, fmt.Errorf("category cannot be nil")
	}

	// Validate/Ensure ID
	if c.ID.IsNil() {
		c.ID = sop.NewUUID()
	}

	// Lock the transaction on this insert in the categories BTree
	ok, err := s.categories.Add(ctx, c.ID, c)
	if err != nil {
		return sop.UUID{}, fmt.Errorf("failed to insert new category: %w", err)
	}
	if !ok {
		return sop.UUID{}, fmt.Errorf("category with ID %v already exists", c.ID)
	}

	// If this has a parent, we must update the parent's ChildrenIDs list to declare this hierarchy
	if !c.ParentID.IsNil() {
		ok, err := s.categories.Find(ctx, c.ParentID, false)
		if err != nil {
			return sop.UUID{}, fmt.Errorf("failed to lookup parent category: %w", err)
		}
		if ok {
			parent, err := s.categories.GetCurrentValue(ctx)
			if err != nil {
				return sop.UUID{}, fmt.Errorf("failed to fetch parent category value: %w", err)
			}
			// Link the child to the parent category hierarchy
			parent.ChildrenIDs = append(parent.ChildrenIDs, c.ID)
			_, err = s.categories.UpdateCurrentItem(ctx, c.ParentID, parent)
			if err != nil {
				return sop.UUID{}, fmt.Errorf("failed to link category to parent: %w", err)
			}
		}
	}

	return c.ID, nil
}

// SplitCategory reorganizes a category that has exceeded capacity boundaries.
// It selects two dispersed seeds, bifurcates the vectors mathematically,
// injects the two new categories, wires them as Children of the original Category,
// and deletes the original mathematical points.
func (s *store[T]) SplitCategory(ctx context.Context, categoryID sop.UUID) error {
	// 1. Target and Extract the Parent Category
	ok, err := s.categories.Find(ctx, categoryID, false)
	if err != nil {
		return fmt.Errorf("failed to query splitting category: %w", err)
	}
	if !ok {
		return fmt.Errorf("category %v not found", categoryID)
	}
	origCategory, err := s.categories.GetCurrentValue(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch splitting category value: %w", err)
	}

	// 2. We need the list of Vectors assigned to this exact category
	// (To be fully complete, this would do a Range Query on the vectors BTree from VectorKey{OrigCategoryID} mapping)
	var vecs []Vector
	searchKey := VectorKey{CategoryID: categoryID}

	// Position the cursor at the first vector for this category
	ok, err = s.vectors.Find(ctx, searchKey, true)
	if err != nil {
		return fmt.Errorf("failed to position cursor for vectors: %w", err)
	}

	if ok {
		for {
			vk := s.vectors.GetCurrentKey()
			if vk.Key.CategoryID != categoryID {
				break // Moved past this category's vectors
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

	// Remove collected vectors from original category
	for _, v := range vecs {
		vk := VectorKey{
			CategoryID:         categoryID,
			DistanceToCategory: EuclideanDistance(v.Data, origCategory.CenterVector),
			VectorID:           v.ID,
		}
		_, _ = s.vectors.Remove(ctx, vk)
	}

	// Implementation scaffolded for Swarm Computing expansion:
	// A vector node will coordinate via these keys to physically distribute sub-buckets.
	_ = origCategory
	_ = vecs

	return fmt.Errorf("SplitCategory Swarm data bifurcation not strictly implemented yet")
}
