package dynamic

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
)

// DynamicVectorStore is the m-way tree dynamic capability database interface.
type DynamicVectorStore[T any] interface {
	// Upsert adds or updates a single item in the store.
	Upsert(ctx context.Context, item ai.Item[T]) error
	// UpsertBatch adds or updates multiple items in the store efficiently.
	UpsertBatch(ctx context.Context, items []ai.Item[T]) error

	// Get retrieves a item by its logical ID.
	Get(ctx context.Context, id sop.UUID) (*Item[T], error)
	// Delete removes an item by its logical ID.
	Delete(ctx context.Context, id sop.UUID) error

	// Query searches for the nearest neighbors to the given vector coordinates.
	// filters is a function that returns true if the item should be included.
	Query(ctx context.Context, vec []float32, k int, filter func(T) bool) ([]ai.Hit[T], error)

	// QueryText performs a BM25 or keyword text search on the stored text representation of the thoughts.
	QueryText(ctx context.Context, text string, k int, filter func(T) bool) ([]ai.Hit[T], error)

	// Count returns the total number of items in the store.
	Count(ctx context.Context) (int64, error)

	// Categories returns a B-Tree interface to manually read/update hierarchical categories.
	Categories(ctx context.Context) (btree.BtreeInterface[sop.UUID, *Category], error)

	// AddCategory adds a new category to the store dynamically.
	// This allows for runtime expansion of the concept space without full rebalancing.
	AddCategory(ctx context.Context, c *Category) (sop.UUID, error)

	// Consolidate reads accumulated vectors from short-term memory (TempVectors),
	// dynamically routes them into existing Categories using AssignAndIndex logic,
	// and clears them from short-term memory.
	Consolidate(ctx context.Context) error

	// UpdateEmbedderInfo updates the configuration defining which embedder was used
	// to index the vectors, persisting it in the system configuration of the store.
	UpdateEmbedderInfo(ctx context.Context, provider string, model string, dimensions int) error

	// SetDeduplication enables or disables the internal deduplication check during Upsert.
	SetDeduplication(enabled bool)

	// Vectors returns the Vectors B-Tree for advanced manipulation (Mathematical layout).
	Vectors(ctx context.Context) (btree.BtreeInterface[VectorKey, Vector], error)

	// Content returns the Content B-Tree for advanced manipulation (The actual Item Data).
	Items(ctx context.Context) (btree.BtreeInterface[sop.UUID, Item[T]], error)

	// Version returns the Vector store's version number, which is a unix elapsed time.
	Version(ctx context.Context) (int64, error)
}
