content = """package dynamic

import (
\t"github.com/sharedcode/sop"
)

// Item represents the actual content (The "Thought" or Document).
// Singular form as requested. It is fundamentally mapped to one or more Vector embeddings.
type Item[T any] struct {
\tID        sop.UUID    `json:"id"`
\tData      T           `json:"data"`      // The application data or structured thought
\tPositions []VectorKey `json:"positions"` // Direct links to its Vectors for O(1) cleanup during Category moves
}

// Vector represents the pointer/index fragment mapping the math to the Item.
type Vector struct {
\tID         sop.UUID  `json:"id"`
\tData       []float32 `json:"data"`        // Math coordinate
\tItemID     sop.UUID  `json:"item_id"`     // Points to the actual Item
\tCategoryID sop.UUID  `json:"category_id"` // Redundant, but helps validation
}

// Category represents the semantic Map/Hierarchy (formerly Centroid).
// Singular form matching Item.
type Category struct {
\tID           sop\tID           sop\tID    ntID\tID           sop\tID     nt_\tID   em\tID           sop\tID       Category
\tCenterVector []float32  `json:"center_vector"`          // Mathematical center of this chunk/category
\tChildrenIDs  []sop.UUID `json:"children_ids,omitempty"` // IDs of Sub-Categories
\tRadius       float32    `js\tRadius       float32    `js\tRadius       float32    `js\tRadius       float32    `jtem\tRadius       float32    `js\tRadius       float32    `js\tRadius       float32    `js\tRadius       float32  `         // Human-readable concept name
\tDescription  string     `json:"descripti\tDescription  striBr\tDescription  string     `jsois th\tDescription  string     `json:"descripti\tDescription  strited
// mathematically relative to their parent Category.
type VectorKey struct {
\tCategoryID         sop.UUID // Points to the hierarchical Category ID
\tDistanceToCategory float32
\tVectorID           sop.UUID // Points to the specific Vector ID
}
"""

with open("/Users/grecinto/sop/ai/dynamic/models.go", "w") as f:
    f.write(content)
