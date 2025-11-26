package ai

import "context"

// Embeddings defines the interface for generating vector embeddings from text.
type Embeddings interface {
	// Name returns the name of the embedding model.
	Name() string
	// Dim returns the dimension of the embeddings.
	Dim() int
	// EmbedTexts generates embeddings for a batch of texts.
	EmbedTexts(texts []string) ([][]float32, error)
}

// VectorIndex defines the interface for a vector database.
type VectorIndex interface {
	// Upsert adds or updates a single item in the index.
	Upsert(id string, vec []float32, meta map[string]any) error
	// UpsertBatch adds or updates multiple items in the index efficiently.
	UpsertBatch(items []Item) error
	// Get retrieves an item by its ID.
	Get(id string) (*Item, error)
	// Delete removes an item by its ID.
	Delete(id string) error
	// Query searches for the nearest neighbors to the given vector.
	Query(vec []float32, k int, filters map[string]any) ([]Hit, error)
	// Count returns the total number of items in the index.
	Count() (int64, error)
}

// Item represents a vector item returned to the user.
type Item struct {
	ID     string
	Vector []float32
	Meta   map[string]any
}

// Hit represents a search result.
type Hit struct {
	ID    string
	Score float32
	Meta  map[string]any
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
type Domain interface {
	ID() string
	Embedder() Embeddings
	Index() VectorIndex
	Policies() PolicyEngine
	Classifier() Classifier
	Prompt(kind string) (string, error)
	DataPath() string
}

// Agent defines the interface for an AI agent service.
type Agent interface {
	Search(ctx context.Context, query string, limit int) ([]Hit, error)
	Ask(ctx context.Context, query string) (string, error)
}
