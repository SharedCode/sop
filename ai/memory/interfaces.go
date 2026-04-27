package memory

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
)

// MemoryStore is the m-way tree dynamic capability database interface.
type MemoryStore[T any] interface {
	// Upsert adds or updates a single item in the store.
	Upsert(ctx context.Context, item Item[T], vec []float32) error
	// UpsertBatch adds or updates multiple items in the store efficiently.
	// UpsertByCategory explicitly assigns a category ignoring spatial routing.
	UpsertByCategory(ctx context.Context, categoryName string, item Item[T], vec []float32) error
	UpsertBatch(ctx context.Context, items []Item[T], vecs [][]float32) error

	// Get retrieves a item by its logical ID.
	Get(ctx context.Context, id sop.UUID) (*Item[T], error)
	// Delete removes an item by its logical ID.
	Delete(ctx context.Context, id sop.UUID) error

	// Query searches for the nearest neighbors to the given vector coordinates.
	// filters is a function that returns true if the item should be included.
	Query(ctx context.Context, vec []float32, opts *SearchOptions[T]) ([]ai.Hit[T], error)

	// QueryBatch searches for the nearest neighbors for a slice of query vectors.
	QueryBatch(ctx context.Context, vecs [][]float32, opts *SearchOptions[T]) ([][]ai.Hit[T], error)

	// QueryText performs a BM25 or keyword text search on the stored text representation of the thoughts.
	QueryText(ctx context.Context, text string, opts *SearchOptions[T]) ([]ai.Hit[T], error)

	// QueryTextBatch performs a BM25 or keyword text search for an array of queries.
	QueryTextBatch(ctx context.Context, texts []string, opts *SearchOptions[T]) ([][]ai.Hit[T], error)

	// Count returns the total number of items in the store.
	Count(ctx context.Context) (int64, error)

	// Categories returns a B-Tree interface to manually read/update hierarchical categories.
	Categories(ctx context.Context) (btree.BtreeInterface[sop.UUID, *Category], error)

	// AddCategory adds a new category to the store dynamically.
	// This allows for runtime expansion of the concept space without full rebalancing.
	AddCategory(ctx context.Context, c *Category) (sop.UUID, error)

	// AddCategoryParent connects an existing category to an additional parent, supporting
	// the polyhierarchy DAG structure. This is often leveraged during LLM Sleep Cycles.
	AddCategoryParent(ctx context.Context, categoryID sop.UUID, parent CategoryParent) error



	// Consolidate reads accumulated vectors from short-term memory (TempVectors),
	// dynamically routes them into existing Categories using AssignAndIndex logic,
	// and clears them from short-term memory.
	Consolidate(ctx context.Context) error

	// UpdateEmbedderInfo updates the configuration defining which embedder was used
	// to index the vectors, persisting it in the system configuration of the store.
	UpdateEmbedderInfo(ctx context.Context, provider string, model string, dimensions int) error

	// SetDeduplication enables or disables the internal deduplication check during Upsert.
	SetDeduplication(enabled bool)

	// SetLLM sets the LLM interface used to generate categories dynamically.
	SetLLM(llm LLM[T])

	// Vectors returns the Vectors B-Tree for advanced manipulation (Mathematical layout).
	Vectors(ctx context.Context) (btree.BtreeInterface[VectorKey, Vector], error)

	// Content returns the Content B-Tree for advanced manipulation (The actual Item Data).
	Items(ctx context.Context) (btree.BtreeInterface[sop.UUID, Item[T]], error)

	// Version returns the Vector store's version number, which is a unix elapsed time.
	Version(ctx context.Context) (int64, error)
}

// SearchOptions provides optional parameters for querying the vector store
type SearchOptions[T any] struct {
	Limit    int
	Category string
	Filter   func(T) bool
}
