package ai

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
)

// Embeddings defines the interface for generating vector embeddings from text.
type Embeddings interface {
	// Name returns the name of the embedding model.
	Name() string
	// Dim returns the dimension of the embeddings.
	Dim() int
	// EmbedTexts generates embeddings for a batch of texts.
	EmbedTexts(texts []string) ([][]float32, error)
}

// Centroid represents a cluster center and its metadata.
type Centroid struct {
	Vector      []float32
	VectorCount int
}

// VectorKey is the key for the Vectors B-Tree.
type VectorKey struct {
	CentroidID         int
	DistanceToCentroid float32
	ItemID             string
}

// VectorStore defines the interface for a vector database domain (like a table).
type VectorStore[T any] interface {
	// Upsert adds or updates a single item in the store.
	// It now accepts an Item[T] which includes the Vector, Payload, and optional CentroidID.
	Upsert(item Item[T]) error
	// UpsertBatch adds or updates multiple items in the store efficiently.
	UpsertBatch(items []Item[T]) error
	// Get retrieves an item by its ID.
	Get(id string) (*Item[T], error)
	// Delete removes an item by its ID.
	Delete(id string) error
	// Query searches for the nearest neighbors to the given vector.
	// filters is a function that returns true if the item should be included.
	Query(vec []float32, k int, filter func(T) bool) ([]Hit[T], error)
	// Count returns the total number of items in the store.
	Count() (int64, error)
	// AddCentroid adds a new centroid to the store dynamically.
	// This allows for runtime expansion of the concept space without full rebalancing.
	AddCentroid(vec []float32) (int, error)
	// SetDeduplication enables or disables the internal deduplication check during Upsert.
	// Disabling this can speed up ingestion for pristine data but may lead to ghost vectors if duplicates exist.
	SetDeduplication(enabled bool)

	// WithTransaction returns a new instance of the store bound to the provided transaction.
	// Operations on this instance will participate in the transaction but will NOT commit/rollback it.
	WithTransaction(trans sop.Transaction) VectorStore[T]

	// Centroids returns the Centroids B-Tree for advanced manipulation.
	Centroids(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[int, Centroid], error)
	// Vectors returns the Vectors B-Tree for advanced manipulation.
	Vectors(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[VectorKey, []float32], error)
	// Content returns the Content B-Tree for advanced manipulation.
	Content(ctx context.Context, trans sop.Transaction) (btree.BtreeInterface[string, string], error)
}

// DatabaseType defines the deployment mode of the vector database.
type DatabaseType int

const (
	// Standalone mode uses in-memory caching and local file storage.
	// Suitable for single-node deployments.
	Standalone DatabaseType = iota
	// Clustered mode uses distributed caching (e.g., Redis) and shared storage.
	// Suitable for multi-node deployments.
	Clustered
)

// UsageMode defines how the vector database is intended to be used.
type UsageMode int

const (
	// BuildOnceQueryMany optimizes for a single ingestion phase followed by read-only queries.
	// Temporary structures (TempVectors, Lookup) are deleted after indexing to save space.
	BuildOnceQueryMany UsageMode = iota

	// DynamicWithVectorCountTracking optimizes for continuous updates (CRUD).
	// It maintains necessary metadata (like vector counts per centroid) to support
	// future rebalancing and structural adjustments.
	DynamicWithVectorCountTracking

	// Dynamic optimizes for continuous updates (CRUD). Does not maintain metadata like vector counts.
	// Useful for scenarios where the Agent explicitly manages the Centroids & Vector assignments.
	Dynamic
)

// VectorDatabase defines the interface for a vector database manager.
// It handles configuration and the creation of domain-specific stores (VectorStore).
type VectorDatabase[T any] interface {
	// SetUsageMode configures the usage pattern (e.g., BuildOnceQueryMany, Dynamic).
	SetUsageMode(mode UsageMode)
	// SetStoragePath configures the root file system path for data persistence.
	SetStoragePath(path string)
	// SetReadMode configures the transaction mode (e.g., NoCheck for speed) for Query operations.
	// We use int here to avoid dependency on sop package, but it corresponds to sop.TransactionMode.
	SetReadMode(mode int)
	// Open returns a VectorStore for the specified domain (e.g., "doctor", "nurse").
	Open(domain string) VectorStore[T]
}

// Item represents a vector item returned to the user.
type Item[T any] struct {
	ID         string
	Vector     []float32
	Payload    T
	CentroidID int // Optional: Explicitly assign to a centroid (0 = auto)
}

// Hit represents a search result.
type Hit[T any] struct {
	ID      string
	Score   float32
	Payload T
}

// Generator defines the interface for an LLM or text generation model.
type Generator interface {
	// Name returns the name of the generator.
	Name() string
	// Generate produces text based on the provided prompt and options.
	Generate(ctx context.Context, prompt string, opts GenOptions) (GenOutput, error)
	// EstimateCost calculates the estimated cost of the generation.
	EstimateCost(inTokens, outTokens int) float64
}

// GenOptions configures the generation process.
type GenOptions struct {
	MaxTokens   int
	Temperature float32
	TopP        float32
	Stop        []string
}

// GenOutput represents the result of a generation.
type GenOutput struct {
	Text       string
	TokensUsed int
	Raw        any
}

// PolicyEngine defines the interface for evaluating content against safety policies.
type PolicyEngine interface {
	// Evaluate checks the content against the policy rules.
	Evaluate(stage string, sample ContentSample, labels []Label) (PolicyDecision, error)
}

// Classifier defines the interface for content classification models.
type Classifier interface {
	// Name returns the name of the classifier.
	Name() string
	// Classify analyzes the content and returns a list of labels.
	Classify(sample ContentSample) ([]Label, error)
}

// ContentSample represents the content being evaluated for safety.
type ContentSample struct {
	Text string
	Meta map[string]any
}

// Label represents a classification result (e.g., "profanity", "hate_speech").
type Label struct {
	Name   string
	Score  float32
	Source string
}

// PolicyDecision represents the outcome of a policy evaluation.
type PolicyDecision struct {
	Action   string   // "allow", "block", "flag"
	Reasons  []string // Why the action was taken
	PolicyID string
}

// Domain represents a vertical or specific AI application domain.
type Domain[T any] interface {
	ID() string
	Name() string
	Embedder() Embeddings
	Index() VectorStore[T]
	Policies() PolicyEngine
	Classifier() Classifier
	Prompt(kind string) (string, error)
	DataPath() string
}

// Agent defines the interface for an AI agent service.
type Agent[T any] interface {
	Search(ctx context.Context, query string, limit int) ([]Hit[T], error)
	Ask(ctx context.Context, query string) (string, error)
}

// ModelStore defines the interface for persisting and retrieving AI models.
// It allows for the management of "Skills" (small models) and "Brains" (large models).
type ModelStore interface {
	// Save persists a model with the given name and category.
	// The model can be any serializable object (e.g., Perceptron, NeuralNet).
	Save(ctx context.Context, category string, name string, model any) error

	// Load retrieves a model by name and category and populates the provided object.
	// The target parameter must be a pointer to the model struct.
	Load(ctx context.Context, category string, name string, target any) error

	// List returns the names of all stored models in a given category.
	List(ctx context.Context, category string) ([]string, error)

	// Delete removes a model from the store.
	Delete(ctx context.Context, category string, name string) error

	// WithTransaction returns a new instance of the store bound to the provided transaction.
	WithTransaction(trans sop.Transaction) ModelStore
}
