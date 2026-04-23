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

