package dynamic

import (
	"github.com/sharedcode/sop"
)

// Payload represents the actual content (The "Thought" or Document).
// It has no knowledge of math or B-Trees.
type Payload[T any] struct {
	ID   sop.UUID `json:"id"`
	Data T        `json:"data"` // The application data or structured thought
}

// Vector represents the pointer/index fragment mapping the math to the Payload.
// Multiple varying Vectors can point to the same PayloadID.
type Vector struct {
	ID        sop.UUID  `json:"id"`
	Data      []float32 `json:"data"`       // Math coordinate
	PayloadID sop.UUID  `json:"payload_id"` // -> Points to the actual Thought (Payload)
}

// Centroid represents the Map/Hierarchy. It is structurally disjoint from payload pieces.
type Centroid struct {
	ID           sop.UUID   `json:"id"`
	ParentID     sop.UUID   `json:"parent_id,omitempty"`    // Points to parent Centroid
	CenterVector []float32  `json:"center_vector"`          // Mathematical center of this chunk/category
	ChildrenIDs  []sop.UUID `json:"children_ids,omitempty"` // IDs of Sub-Centroids
	Radius       float32    `json:"radius,omitempty"`       // Size of the cluster
	VectorCount  int        `json:"vector_count,omitempty"` // Number of vectors in this bucket
	Name         string     `json:"name,omitempty"`         // Human-readable concept name
	Description  string     `json:"description,omitempty"`  // Broader context
}

// VectorKey is the key for the Vectors B-Tree. It dictates how vectors are sorted
// mathematically relative to their parent Centroid.
type VectorKey struct {
	CentroidID         sop.UUID // Points to the hierarchical Centroid ID
	DistanceToCentroid float32
	VectorID           sop.UUID // Points to the specific Vector ID

	// IsDeleted marks this key as a Tombstone.
	IsDeleted bool
}

// ContentKey is the key for the Content B-Tree.
type ContentKey struct {
	VectorID       sop.UUID `json:"vid"`
	PayloadID      sop.UUID `json:"id"`
	CentroidID     sop.UUID `json:"cid"`
	Distance       float32  `json:"dist"`
	Version        int64    `json:"ver"`
	Deleted        bool     `json:"del"`
	NextCentroidID sop.UUID `json:"ncid"`
	NextDistance   float32  `json:"ndist"`
	NextVersion    int64    `json:"nver"`
}
