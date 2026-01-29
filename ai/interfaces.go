package ai

import (
	"context"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/search"
)

// ContextKey is a type for context keys used in the AI package.
type ContextKey string

const (
	// CtxKeyProvider is the context key for overriding the AI provider.
	CtxKeyProvider ContextKey = "ai_provider"
	// CtxKeyExecutor is the context key for passing the ToolExecutor.
	CtxKeyExecutor ContextKey = "ai_executor"
	// CtxKeyDeobfuscator is the context key for passing the Deobfuscator.
	CtxKeyDeobfuscator ContextKey = "ai_deobfuscator"
	// CtxKeyHistory is the context key for passing conversation history.
	CtxKeyHistory ContextKey = "ai_history"
	// CtxKeyWriter is the context key for passing an io.Writer for streaming output.
	CtxKeyWriter ContextKey = "ai_writer"
	// CtxKeyDatabase is the context key for passing the target database for script execution.
	CtxKeyDatabase ContextKey = "ai_database"
	// CtxKeyResultStreamer is the context key for passing the ResultStreamer.
	CtxKeyResultStreamer ContextKey = "ai_result_streamer"
	// CtxKeyScriptRecorder is the context key for passing the ScriptRecorder.
	CtxKeyScriptRecorder ContextKey = "ai_script_recorder"
	// CtxKeyAutoFlush is the context key for enabling/disabling auto-flush (boolean).
	CtxKeyAutoFlush ContextKey = "ai_auto_flush"
)

// ResultStreamer defines the interface for streaming tool results.
type ResultStreamer interface {
	// BeginArray starts a JSON array output.
	BeginArray()
	// SetMetadata sets metadata for the result (e.g. headers, record counts).
	// Should be called before the first WriteItem.
	SetMetadata(meta map[string]any)
	// WriteItem writes a single item to the output (e.g. an element of an array).
	WriteItem(item any)
	// EndArray ends the JSON array output.
	EndArray()
}

// Deobfuscator defines the interface for de-obfuscating text.
type Deobfuscator interface {
	Deobfuscate(text string) string
}

// Embeddings defines the interface for generating vector embeddings from text.
type Embeddings interface {
	// Name returns the name of the embedding model.
	Name() string
	// Dim returns the dimension of the embeddings.
	Dim() int
	// EmbedTexts generates embeddings for a batch of texts.
	EmbedTexts(ctx context.Context, texts []string) ([][]float32, error)
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
	// IsDeleted marks this key as a Tombstone.
	// It ensures the Optimize process can find this entry and physically delete
	// the corresponding data from the Content store.
	IsDeleted bool
}

// ContentKey is the key for the Content B-Tree.
// It includes metadata to allow updates without modifying the Value segment (Payload).
//
// SOP's key structure handling allows apps to ride data in it that acts as persistent space
// as well without affecting the overall key/value pair behaviour and BTree ness.
// This means critical metadata (like Version, CentroidID, Deleted flag) is available
// during key traversal (e.g. Range scans) without needing to fetch the potentially large Value payload.
type ContentKey struct {
	ItemID         string  `json:"id"`
	CentroidID     int     `json:"cid"`
	Distance       float32 `json:"dist"`
	Version        int64   `json:"ver"`
	Deleted        bool    `json:"del"`
	NextCentroidID int     `json:"ncid"`
	NextDistance   float32 `json:"ndist"`
	NextVersion    int64   `json:"nver"`
}

// VectorStore defines the interface for a vector database domain (like a table).
type VectorStore[T any] interface {
	// Upsert adds or updates a single item in the store.
	// It now accepts an Item[T] which includes the Vector, Payload, and optional CentroidID.
	Upsert(ctx context.Context, item Item[T]) error
	// UpsertBatch adds or updates multiple items in the store efficiently.
	UpsertBatch(ctx context.Context, items []Item[T]) error
	// Get retrieves an item by its ID.
	Get(ctx context.Context, id string) (*Item[T], error)
	// Delete removes an item by its ID.
	Delete(ctx context.Context, id string) error
	// Query searches for the nearest neighbors to the given vector.
	// filters is a function that returns true if the item should be included.
	Query(ctx context.Context, vec []float32, k int, filter func(T) bool) ([]Hit[T], error)
	// Count returns the total number of items in the store.
	Count(ctx context.Context) (int64, error)
	// AddCentroid adds a new centroid to the store dynamically.
	// This allows for runtime expansion of the concept space without full rebalancing.
	AddCentroid(ctx context.Context, vec []float32) (int, error)

	// Optimize reorganizes the index to improve query performance.
	// It re-calculates centroids based on the full dataset and re-distributes vectors.
	// This is recommended after a large batch ingestion (BuildOnceQueryMany mode) to "Seal" the index.
	Optimize(ctx context.Context) error

	// SetDeduplication enables or disables the internal deduplication check during Upsert.
	// Disabling this can speed up ingestion for pristine data but may lead to ghost vectors if duplicates exist.
	SetDeduplication(enabled bool)

	// Centroids returns the Centroids B-Tree for advanced manipulation.
	Centroids(ctx context.Context) (btree.BtreeInterface[int, Centroid], error)
	// Vectors returns the Vectors B-Tree for advanced manipulation.
	Vectors(ctx context.Context) (btree.BtreeInterface[VectorKey, []float32], error)
	// Content returns the Content B-Tree for advanced manipulation.
	Content(ctx context.Context) (btree.BtreeInterface[ContentKey, string], error)
	// Lookup returns the Sequence Lookup B-Tree for advanced manipulation (e.g. random sampling).
	Lookup(ctx context.Context) (btree.BtreeInterface[int, string], error)

	// Version returns the Vector store's version number, which is a unix elapsed time.
	Version(ctx context.Context) (int64, error)
}

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
	Evaluate(ctx context.Context, stage string, sample ContentSample, labels []Label) (PolicyDecision, error)
}

// Classifier defines the interface for content classification models.
type Classifier interface {
	// Name returns the name of the classifier.
	Name() string
	// Classify analyzes the content and returns a list of labels.
	Classify(ctx context.Context, sample ContentSample) ([]Label, error)
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
	// Index returns the vector index used for retrieval.
	// It requires a transaction to be passed in.
	Index(ctx context.Context, tx sop.Transaction) (VectorStore[T], error)
	// TextIndex returns the text search index used for retrieval.
	TextIndex(ctx context.Context, tx sop.Transaction) (TextIndex, error)
	// BeginTransaction starts a new transaction for the domain's underlying storage.
	BeginTransaction(ctx context.Context, mode sop.TransactionMode) (sop.Transaction, error)
	Policies() PolicyEngine
	Classifier() Classifier
	Prompt(ctx context.Context, kind string) (string, error)
	DataPath() string
}

// AskConfig holds configuration options for the Ask method.
type AskConfig struct {
	Values map[string]any
}

// Option defines a function that configures AskConfig.
type Option func(*AskConfig)

// NewAskConfig creates a new AskConfig with the applied options.
func NewAskConfig(opts ...Option) *AskConfig {
	cfg := &AskConfig{Values: make(map[string]any)}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// WithDatabase adds a database instance to the configuration.
// The db parameter should be of type *database.Database.
func WithDatabase(db any) Option {
	return func(c *AskConfig) {
		c.Values["database"] = db
	}
}

// WithDefaultFormat sets the default output format (e.g., "csv", "json") for tools.
func WithDefaultFormat(format string) Option {
	return func(c *AskConfig) {
		c.Values["default_format"] = format
	}
}

// WithDatabaseResolver adds a database resolver to the configuration.
// Deprecated: Use WithSessionPayload instead.
func WithDatabaseResolver(resolver any) Option {
	return func(c *AskConfig) {
		// No-op or legacy support if needed, but we are removing the interface.
	}
}

// Agent defines the interface for an AI agent service.
type Agent[T any] interface {
	// Lifecycle methods
	Open(ctx context.Context) error
	Close(ctx context.Context) error

	Search(ctx context.Context, query string, limit int) ([]Hit[T], error)
	Ask(ctx context.Context, query string, opts ...Option) (string, error)
}

// SessionPayload represents the context and state for an agent session.
// It carries domain-specific artifacts like database connections.
type SessionPayload struct {
	// CurrentDB is the active database name for the session.
	CurrentDB string
	// Transaction holds the active transaction for the session.
	// Deprecated: Use Transactions map instead for multi-db support.
	Transaction any
	// Transactions holds active transactions keyed by database name.
	Transactions map[string]any
	// Variables holds session-scoped variables (e.g. cached store instances).
	Variables map[string]any
	// ExplicitTransaction indicates if the transaction was explicitly started by the user.
	ExplicitTransaction bool
	// LastInteractionSteps tracks the number of steps added/executed in the last user interaction.
	LastInteractionSteps int
}

// GetDatabase returns the effective current Database name.
func (s *SessionPayload) GetDatabase() string {
	return s.CurrentDB
}

// WithSessionPayload adds a session payload to the configuration.
func WithSessionPayload(payload *SessionPayload) Option {
	return func(c *AskConfig) {
		c.Values["payload"] = payload
	}
}

// GetSessionPayload retrieves the session payload from the context.
func GetSessionPayload(ctx context.Context) *SessionPayload {
	if val := ctx.Value("session_payload"); val != nil {
		if p, ok := val.(*SessionPayload); ok {
			return p
		}
	}
	return nil
}

// ToolExecutor defines the interface for the application to expose capabilities to the Agent.
// This allows the Agent to "act" on the application (e.g. query DB, send email).
type ToolExecutor interface {
	// Execute runs a named tool with the provided arguments.
	Execute(ctx context.Context, toolName string, args map[string]any) (string, error)
	// ListTools returns the list of available tools.
	ListTools(ctx context.Context) ([]ToolDefinition, error)
}

// ToolDefinition describes a tool available to the agent.
type ToolDefinition struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	// Schema is a JSON schema string describing the arguments.
	Schema string `json:"schema"`
}

// AgentControl defines methods to manage the agent's lifecycle.
// This allows the Application to control the Agent (e.g. Stop/Pause).
type AgentControl interface {
	// Stop aborts the current operation.
	Stop() error
	// Pause suspends the agent's activities (if supported).
	Pause() error
	// Resume resumes the agent's activities.
	Resume() error
}

// TextIndex defines the interface for a text search index.
type TextIndex interface {
	// Add indexes a document.
	Add(ctx context.Context, docID string, text string) error
	// Search performs a text search.
	Search(ctx context.Context, query string) ([]search.TextSearchResult, error)
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
}

// Script represents a recorded sequence of user interactions or a programmed script.
type Script struct {
	Description string   `json:"description"`
	Parameters  []string `json:"parameters"`         // Input parameters for the script
	Database    string   `json:"database,omitempty"` // Database to run the script against
	Portable    bool     `json:"portable,omitempty"` // If true, allows running on any database
	// TransactionMode specifies how transactions are handled for this script.
	// Values: "none" (default, manual), "single" (one tx for all steps), "per_step" (auto-commit each step)
	TransactionMode string       `json:"transaction_mode,omitempty"`
	Steps           []ScriptStep `json:"steps"`
}

// ScriptStep represents a single instruction in a script.
// It follows a stabilized schema for "Natural Language Programming".
type ScriptStep struct {
	// Type of the step.
	// Valid values: "ask", "set", "if", "loop", "fetch", "say", "command", "call_script" (or "script"), "block"
	Type string `json:"type"`

	// Name acts as a label or identifier for the step.
	// It can be used for UI display or referenced in complex flows.
	Name string `json:"name,omitempty"`

	// --- Fields for "ask" (LLM Interaction) ---
	// Prompt is the question or command to send to the LLM. Supports templating.
	Prompt string `json:"prompt,omitempty"`
	// OutputVariable is where the LLM's response will be stored.
	// If empty, the response is just printed.
	OutputVariable string `json:"output_variable,omitempty"`

	// --- Fields for "set" (Variable Assignment) ---
	// Variable is the name of the variable to set.
	Variable string `json:"variable,omitempty"`
	// Value is the value to assign. Supports templating.
	Value string `json:"value,omitempty"`

	// --- Fields for "if" (Flow Branching) ---
	// Condition is a Go template expression that must evaluate to "true".
	// Example: "{{ gt .count 5 }}"
	Condition string `json:"condition,omitempty"`
	// Then is the list of steps to execute if the condition is true.
	Then []ScriptStep `json:"then,omitempty"`
	// Else is the list of steps to execute if the condition is false.
	Else []ScriptStep `json:"else,omitempty"`

	// --- Fields for "loop" (Iteration) ---
	// List is the variable name or expression evaluating to a list/slice to iterate over.
	List string `json:"list,omitempty"`
	// Iterator is the variable name for the current item in the loop.
	Iterator string `json:"iterator,omitempty"`
	// Steps is the list of steps to execute for each item.
	// Also used for "block" type to hold the sequence of steps.
	Steps []ScriptStep `json:"steps,omitempty"`

	// --- Fields for "fetch" (Data Retrieval) ---
	// Database specifies the database name (optional). If empty, uses the current context database.
	Database string `json:"database,omitempty"`
	// Source specifies the data source type (e.g., "btree", "vector_store").
	Source string `json:"source,omitempty"`
	// Resource specifies the name of the resource (e.g., table name).
	Resource string `json:"resource,omitempty"`
	// Filter is an optional filter expression (not yet fully implemented).
	Filter string `json:"filter,omitempty"`
	// (Uses Variable field to store the result)

	// --- Fields for "say" (Output) ---
	// Message is the text to output to the user. Supports templating.
	Message string `json:"message,omitempty"`

	// --- Fields for "command" (Direct Tool Execution) ---
	// Command is the name of the tool/command to execute.
	Command string `json:"command,omitempty"`
	// Args are the arguments for the command.
	Args map[string]any `json:"args,omitempty"`

	// --- Fields for "call_script" (Nested Script Execution) ---
	// ScriptName is the name of the script to execute.
	ScriptName string `json:"script_name,omitempty"`
	// ScriptArgs are the arguments to pass to the script.
	ScriptArgs map[string]string `json:"script_args,omitempty"`

	// --- Async Execution ---
	// IsAsync specifies if the step should be executed asynchronously.
	// If true, the step runs in a goroutine and the script continues to the next step.
	// All async steps are gathered (waited for) at the end of the script execution.
	IsAsync bool `json:"is_async,omitempty"`

	// ContinueOnError specifies if the script should continue executing if this step fails.
	// If false (default), the script stops and returns the error.
	// For async steps, if this is false and the step fails, it cancels the execution of other steps.
	ContinueOnError bool `json:"continue_on_error,omitempty"`

	// --- Documentation ---
	// Description provides a human-readable explanation of what this step does.
	// It is used for documentation and self-explanation of the script.
	Description string `json:"description,omitempty"`
}

// ScriptRecorder is an interface for recording script steps.
type ScriptRecorder interface {
	RecordStep(ctx context.Context, step ScriptStep)
	// RefactorLastSteps refactors the last N steps into a new structure (script or block).
	// count: number of steps to refactor.
	// mode: "script" (extract to new named script) or "block" (group into block step).
	// name: name of the new script (if mode is "script").
	RefactorLastSteps(count int, mode string, name string) error
}
