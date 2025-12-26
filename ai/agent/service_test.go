package agent

import (
	"context"
	"strings"
	"testing"

	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/obfuscation"
	"github.com/sharedcode/sop/btree"
)

// MockGenerator implements ai.Generator for testing.
type MockGenerator struct {
	Response string
}

func (m *MockGenerator) Name() string {
	return "mock"
}

func (m *MockGenerator) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	return ai.GenOutput{
		Text: m.Response,
	}, nil
}

func (m *MockGenerator) EstimateCost(inTokens, outTokens int) float64 {
	return 0
}

// MockEmbedder implements ai.Embeddings for testing.
type MockEmbedder struct{}

func (m *MockEmbedder) Name() string { return "mock" }
func (m *MockEmbedder) Dim() int     { return 1536 }
func (m *MockEmbedder) EmbedTexts(ctx context.Context, texts []string) ([][]float32, error) {
	return make([][]float32, len(texts)), nil
}

// MockDomain implements ai.Domain for testing.
type MockDomain struct{}

func (m *MockDomain) ID() string              { return "mock" }
func (m *MockDomain) Name() string            { return "Mock Domain" }
func (m *MockDomain) Embedder() ai.Embeddings { return &MockEmbedder{} }
func (m *MockDomain) Index(ctx context.Context, tx sop.Transaction) (ai.VectorStore[map[string]any], error) {
	return &MockVectorStore{}, nil
}
func (m *MockDomain) TextIndex(ctx context.Context, tx sop.Transaction) (ai.TextIndex, error) {
	return nil, nil
}
func (m *MockDomain) BeginTransaction(ctx context.Context, mode sop.TransactionMode) (sop.Transaction, error) {
	return &MockTransaction{}, nil
}
func (m *MockDomain) Policies() ai.PolicyEngine { return nil }
func (m *MockDomain) Classifier() ai.Classifier { return nil }
func (m *MockDomain) Prompt(ctx context.Context, kind string) (string, error) {
	return "System Prompt", nil
}
func (m *MockDomain) DataPath() string { return "/tmp" }

// MockTransaction implements sop.Transaction for testing.
type MockTransaction struct{}

func (m *MockTransaction) Begin(ctx context.Context) error                                        { return nil }
func (m *MockTransaction) Commit(ctx context.Context) error                                       { return nil }
func (m *MockTransaction) Rollback(ctx context.Context) error                                     { return nil }
func (m *MockTransaction) HasBegun() bool                                                         { return true }
func (m *MockTransaction) GetPhasedTransaction() sop.TwoPhaseCommitTransaction                    { return nil }
func (m *MockTransaction) AddPhasedTransaction(otherTransaction ...sop.TwoPhaseCommitTransaction) {}
func (m *MockTransaction) GetStores(ctx context.Context) ([]string, error)                        { return nil, nil }
func (m *MockTransaction) Close() error                                                           { return nil }
func (m *MockTransaction) GetID() sop.UUID                                                        { return sop.UUID{} }
func (m *MockTransaction) CommitMaxDuration() time.Duration                                       { return 0 }
func (m *MockTransaction) OnCommit(callback func(ctx context.Context) error)                      {}

// MockVectorStore implements ai.VectorStore for testing.
type MockVectorStore struct{}

func (m *MockVectorStore) Upsert(ctx context.Context, item ai.Item[map[string]any]) error { return nil }
func (m *MockVectorStore) UpsertBatch(ctx context.Context, items []ai.Item[map[string]any]) error {
	return nil
}
func (m *MockVectorStore) Get(ctx context.Context, id string) (*ai.Item[map[string]any], error) {
	return nil, nil
}
func (m *MockVectorStore) Delete(ctx context.Context, id string) error { return nil }
func (m *MockVectorStore) Query(ctx context.Context, vec []float32, k int, filter func(map[string]any) bool) ([]ai.Hit[map[string]any], error) {
	return nil, nil
}
func (m *MockVectorStore) Count(ctx context.Context) (int64, error)                    { return 0, nil }
func (m *MockVectorStore) AddCentroid(ctx context.Context, vec []float32) (int, error) { return 0, nil }
func (m *MockVectorStore) Optimize(ctx context.Context) error                          { return nil }
func (m *MockVectorStore) SetDeduplication(enabled bool)                               {}
func (m *MockVectorStore) Centroids(ctx context.Context) (btree.BtreeInterface[int, ai.Centroid], error) {
	return nil, nil
}
func (m *MockVectorStore) Vectors(ctx context.Context) (btree.BtreeInterface[ai.VectorKey, []float32], error) {
	return nil, nil
}
func (m *MockVectorStore) Content(ctx context.Context) (btree.BtreeInterface[ai.ContentKey, string], error) {
	return nil, nil
}
func (m *MockVectorStore) Lookup(ctx context.Context) (btree.BtreeInterface[int, string], error) {
	return nil, nil
}
func (m *MockVectorStore) Version(ctx context.Context) (int64, error) { return 0, nil }

// MockExecutor implements ai.ToolExecutor for testing.
type MockExecutor struct {
	LastTool string
	LastArgs map[string]any
}

func (e *MockExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	e.LastTool = toolName
	e.LastArgs = args
	return "Executed", nil
}

func (e *MockExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}

func TestService_Ask_Obfuscation(t *testing.T) {
	// Setup Obfuscator
	obfuscation.GlobalObfuscator = obfuscation.NewMetadataObfuscator()
	obfuscation.GlobalObfuscator.RegisterResource("Python Complex DB", "DB")
	obfuscation.GlobalObfuscator.RegisterResource("My Store", "STORE")

	// Get the hashes
	dbHash := obfuscation.GlobalObfuscator.Obfuscate("Python Complex DB", "DB")
	storeHash := obfuscation.GlobalObfuscator.Obfuscate("My Store", "STORE")

	tests := []struct {
		name           string
		llmResponse    string
		expectedResult string // For text response
		expectedDB     string // For tool call
		expectedStore  string // For tool call
		isTool         bool
	}{
		{
			name:           "Normal Text Response",
			llmResponse:    "I found " + dbHash + " and " + storeHash,
			expectedResult: "I found Python Complex DB and My Store",
			isTool:         false,
		},
		{
			name:           "Markdown Bold Response",
			llmResponse:    "I found **" + dbHash + "** and _" + storeHash + "_",
			expectedResult: "I found **Python Complex DB** and _My Store_",
			isTool:         false,
		},
		{
			name:          "Tool Call Clean",
			llmResponse:   `{"tool": "select", "args": {"database": "` + dbHash + `", "store": "` + storeHash + `"}}`,
			expectedDB:    "Python Complex DB",
			expectedStore: "My Store",
			isTool:        true,
		},
		{
			name:          "Tool Call with Markdown Artifacts",
			llmResponse:   `{"tool": "select", "args": {"database": "**` + dbHash + `**", "store": "_` + storeHash + `_"}}`,
			expectedDB:    "Python Complex DB",
			expectedStore: "My Store",
			isTool:        true,
		},
		{
			name:          "Tool Call with NBSP",
			llmResponse:   `{"tool": "select", "args": {"database": "` + dbHash + `\u00a0", "store": "` + storeHash + `"}}`,
			expectedDB:    "Python Complex DB",
			expectedStore: "My Store",
			isTool:        true,
		},
		{
			name:          "Tool Call with Real Name (Leak) + Artifacts",
			llmResponse:   `{"tool": "select", "args": {"database": "**Python Complex DB**", "store": "My Store"}}`,
			expectedDB:    "Python Complex DB",
			expectedStore: "My Store",
			isTool:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gen := &MockGenerator{Response: tt.llmResponse}
			// Enable obfuscation for tests
			svc := NewService(&MockDomain{}, nil, nil, gen, nil, nil, true)
			executor := &MockExecutor{}

			ctx := context.WithValue(context.Background(), ai.CtxKeyExecutor, executor)

			result, err := svc.Ask(ctx, "query")
			if err != nil {
				t.Fatalf("Ask failed: %v", err)
			}

			if tt.isTool {
				// For tool calls, Ask returns the JSON string.
				// We need to parse it to verify the args were cleaned.
				// OR, if we move execution into Ask, we verify the executor was called.
				// Currently Ask returns the JSON string.

				// But wait, the current implementation in Service.Ask just does string replacement on the JSON.
				// It does NOT parse the JSON to clean specific fields.
				// So if the LLM returns `**DB_1**`, string replacement turns it into `**Python Complex DB**`.
				// It does NOT remove the `**`.
				// The User wants DataAdmin (Service) to handle this "woe".
				// So we expect the JSON returned by Ask to contain CLEAN names, without artifacts.

				// Let's see what we get currently.
				t.Logf("Result: %s", result)

				// If we want to verify the "robustness", we should check if the result contains the CLEAN name
				// AND if it's free of artifacts if we implement that logic.

				// For now, let's just check if the real name is present.
				if !strings.Contains(result, tt.expectedDB) {
					t.Errorf("Expected result to contain '%s', got '%s'", tt.expectedDB, result)
				}

				// Check for artifacts in the result string
				if strings.Contains(result, "**"+tt.expectedDB+"**") {
					t.Errorf("Result still contains markdown artifacts: %s", result)
				}
			} else {
				if result != tt.expectedResult {
					t.Errorf("Expected '%s', got '%s'", tt.expectedResult, result)
				}
			}
		})
	}
}

func TestService_Ask_NoObfuscation(t *testing.T) {
	// Setup
	mockGen := &MockGenerator{Response: "Some response with DB_123"}
	mockDomain := &MockDomain{}

	// Create Service with Obfuscation DISABLED
	svc := NewService(mockDomain, nil, nil, mockGen, nil, nil, false)

	ctx := context.Background()

	// Execute
	result, err := svc.Ask(ctx, "query")
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	// Verify
	// Since obfuscation is disabled, the output should be exactly what the generator returned
	if result != "Some response with DB_123" {
		t.Errorf("Expected raw response 'Some response with DB_123', got '%s'", result)
	}
}
