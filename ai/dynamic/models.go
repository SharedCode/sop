package dynamic

import (
	"github.com/sharedcode/sop"
)

// Item represents the actual content (The "Thought" or Document).
// Singular form as requested. It is fundamentally mapped to one or more Vector embeddings.
type Item[T any] struct {
	ID         sop.UUID    `json:"id"`
	CategoryID sop.UUID    `json:"category_id"`
	Data       T           `json:"data"`      // The application data or structured thought
	Positions  []VectorKey `json:"positions"` // Direct links to its Vectors for O(1) cleanup during Category moves
}

// Vector represents the pointer/index fragment mapping the math to the Item.
type Vector struct {
	ID         sop.UUID  `json:"id"`
	Data       []float32 `json:"data"`        // Math coordinate
	ItemID     sop.UUID  `json:"item_id"`     // Points to the actual Item
	CategoryID sop.UUID  `json:"category_id"` // Redundant, but helps validation
}

// Category represents the semantic Map/Hierarchy (formerly Centroid).
// Singular form matching Item.
type Category struct {
	ID           sop.UUID   `json:"id"`
	ParentID     sop.UUID   `json:"parent_id,omitempty"`    // Points to parent Category
	CenterVector []float32  `json:"center_vector"`          // Mathematical center of this chunk/category
	ChildrenIDs  []sop.UUID `json:"children_ids,omitempty"` // IDs of Sub-Categories
	Radius       float32    `json:"radius,omitempty"`       // Size of the cluster
	ItemCount    int        `json:"item_count,omitempty"`   // Number of vectors/items in this bucket
	Name         string     `json:"name,omitempty"`         // Human-readable concept name
	Description  string     `json:"description,omitempty"`  // Broader context
}

// VectorKey is the key for the Vectors B-Tree. It dictates how vectors are sorted
// mathematically relative to their parent Category.
type VectorKey struct {
	CategoryID         sop.UUID // Points to the hierarchical Category ID
	DistanceToCategory float32
	VectorID           sop.UUID // Points to the specific Vector ID
}
