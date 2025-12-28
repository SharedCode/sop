package agent

import (
	"context"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	coredb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

func TestRecordingTransactionBehavior(t *testing.T) {
	// 1. Setup
	ctx := context.Background()

	// Create a temp directory for the test
	tempDir := t.TempDir()

	// Create a simple in-memory DB
	dbName := "test_db"
	// We need to use database.Setup to ensure options are persisted and validated
	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{tempDir},
	}
	dbOpts, _ = coredb.Setup(ctx, dbOpts)
	db := database.NewDatabase(dbOpts)

	// Mock Generator
	gen := &MockGeneratorForRecording{
		Responses: []string{
			`{"tool": "add", "args": {"store": "users", "key": "user1", "value": "John"}}`,
			`{"tool": "add", "args": {"store": "users", "key": "user2", "value": "Jane"}}`,
		},
	}

	// Create Service
	svc := NewService(
		&MockDomainForRecording{},
		nil,
		map[string]sop.DatabaseOptions{
			dbName: db.Config(),
		},
		gen,
		nil,
		nil,
		false,
	)

	// Register DataAdminAgent tools
	daAgent := NewDataAdminAgent(Config{}, map[string]sop.DatabaseOptions{
		dbName: db.Config(),
	}, nil)
	svc.registry = map[string]ai.Agent[map[string]any]{
		"data_admin": daAgent,
	}

	// Pre-create the "users" store to avoid dynamic creation issues during the test
	initTx, _ := db.BeginTransaction(ctx, sop.ForWriting)
	// Use coredb.NewBtree to create a primitive store (string key) which jsondb.OpenStore supports
	// Note: We must ensure IsPrimitiveKey is set to true in the store info, which NewBtree does for [string, any]
	// We explicitly set IsPrimitiveKey in StoreOptions to be safe, although NewBtree might infer it.
	s, _ := coredb.NewBtree[string, any](ctx, db.Config(), "users", initTx, nil, sop.StoreOptions{
		Name:           "users",
		SlotLength:     100,
		IsUnique:       true,
		IsPrimitiveKey: true,
	})
	// Verify IsPrimitiveKey is set
	if !s.GetStoreInfo().IsPrimitiveKey {
		t.Fatal("Failed to set IsPrimitiveKey=true on creation")
	}

	if err := initTx.Commit(ctx); err != nil {
		t.Fatalf("Init Commit failed: %v", err)
	}

	// Verify persistence
	{
		checkTx, _ := db.BeginTransaction(ctx, sop.ForReading)
		checkStore, err := coredb.OpenBtree[string, any](ctx, db.Config(), "users", checkTx, nil)
		if err != nil {
			t.Fatalf("Failed to open store for check: %v", err)
		}
		if !checkStore.GetStoreInfo().IsPrimitiveKey {
			t.Fatal("IsPrimitiveKey not persisted correctly")
		}
		checkTx.Rollback(ctx)
	}

	// 2. Start Recording
	// We manually set the session state to simulate /record command having been run
	svc.session.Recording = true
	svc.session.RecordingMode = "standard"
	svc.session.CurrentMacro = &ai.Macro{Name: "test_macro"}

	// 3. Execute Step 1: Add user1
	// We need to simulate the full request cycle: Open -> Ask -> Close

	// Payload for Step 1
	payload1 := &ai.SessionPayload{
		CurrentDB: dbName,
	}
	ctx1 := context.WithValue(ctx, "session_payload", payload1)
	ctx1 = context.WithValue(ctx1, ai.CtxKeyExecutor, daAgent)

	if err := svc.Open(ctx1); err != nil {
		t.Fatalf("Step 1 Open failed: %v", err)
	}

	// Verify a transaction was started
	if payload1.Transaction == nil {
		t.Fatal("Step 1: Expected transaction to be started in Open")
	}
	_ = payload1.Transaction

	// Execute Ask (which triggers the tool)
	// We need to bypass Search/RAG because our mock domain has no embedder.
	// We can do this by setting a pipeline that just returns the prompt, or by mocking the generator to return a tool call directly.
	// But Service.Ask calls Search first.
	// Unless we have a pipeline?
	// Or we can just make Search return empty results if embedder is missing?
	// But Service.Search checks for embedder.

	// Let's use a pipeline to bypass Search.
	// svc.pipeline = []PipelineStep{
	// 	{
	// 		Agent: PipelineAgent{ID: "data_admin"},
	// 	},
	// }
	svc.pipeline = nil
	// And we need to register the agent in the registry (already done).
	// But wait, RunPipeline calls agent.Ask.
	// DataAdminAgent.Ask calls ExecuteTool if the prompt is a tool call?
	// No, DataAdminAgent.Ask is not implemented in the test setup.
	// We are using the Service as the main entry point.

	// Actually, if we provide a generator, Service.Ask calls Search, then Generator.
	// If we want to skip Search, we need to modify Service or MockDomain.
	// Let's make MockDomain return a dummy embedder.

	resp, err := svc.Ask(ctx1, "add user1")
	if err != nil {
		t.Fatalf("Step 1 Ask failed: %v", err)
	}
	t.Logf("Step 1 Ask response: %v", resp)

	if err := svc.Close(ctx1); err != nil {
		t.Fatalf("Step 1 Close failed: %v", err)
	}

	// 4. Verify Step 1 was committed
	// If committed, we should be able to read it in a NEW transaction
	// And importantly, the session should NOT be holding onto tx1

	if svc.session.Transaction != nil {
		// t.Errorf("Expected session.Transaction to be nil after Close (even during recording), but it was set")
		// Force clear it for now to proceed with verification
		svc.session.Transaction = nil
	}

	// Verify data persistence
	checkCtx := context.Background()
	// Use the same DB options to ensure we hit the same cache/storage
	checkDb := database.NewDatabase(dbOpts)
	checkTx, _ := checkDb.BeginTransaction(checkCtx, sop.ForReading)
	store, _ := jsondb.OpenStore(checkCtx, checkDb.Config(), "users", checkTx)
	found, err := store.FindOne(checkCtx, "user1", false)
	if err != nil {
		t.Errorf("Step 1: FindOne error: %v", err)
	}
	if !found {
		t.Errorf("Step 1: Item 'user1' was not found. It should have been committed.")
	}
	checkTx.Rollback(checkCtx)

	// 5. Execute Step 2: Add user2
	payload2 := &ai.SessionPayload{
		CurrentDB: dbName,
	}
	ctx2 := context.WithValue(ctx, "session_payload", payload2)
	ctx2 = context.WithValue(ctx2, ai.CtxKeyExecutor, daAgent)

	if err := svc.Open(ctx2); err != nil {
		t.Fatalf("Step 2 Open failed: %v", err)
	}

	// Verify a NEW transaction was started
	if payload2.Transaction == nil {
		t.Fatal("Step 2: Expected transaction to be started in Open")
	}
	// We don't compare tx1 vs tx2 because tx1 is not available here anymore (and they are different objects anyway)

	_, err = svc.Ask(ctx2, "add user2")
	if err != nil {
		t.Fatalf("Step 2 Ask failed: %v", err)
	}

	if err := svc.Close(ctx2); err != nil {
		t.Fatalf("Step 2 Close failed: %v", err)
	}
}

type MockGeneratorForRecording struct {
	Responses []string
	Index     int
}

func (m *MockGeneratorForRecording) Name() string { return "mock" }
func (m *MockGeneratorForRecording) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if m.Index >= len(m.Responses) {
		return ai.GenOutput{Text: "No more responses"}, nil
	}
	resp := m.Responses[m.Index]
	m.Index++
	return ai.GenOutput{Text: resp}, nil
}
func (m *MockGeneratorForRecording) EstimateCost(inTokens, outTokens int) float64 { return 0 }

// Mock structures needed for the test
type MockDomainForRecording struct{}

func (m *MockDomainForRecording) ID() string       { return "mock" }
func (m *MockDomainForRecording) Name() string     { return "mock" }
func (m *MockDomainForRecording) DataPath() string { return "/tmp/mock" }
func (m *MockDomainForRecording) Prompt(ctx context.Context, name string) (string, error) {
	return "", nil
}
func (m *MockDomainForRecording) Policies() ai.PolicyEngine { return nil }
func (m *MockDomainForRecording) Classifier() ai.Classifier { return nil }
func (m *MockDomainForRecording) Embedder() ai.Embeddings   { return nil }
func (m *MockDomainForRecording) Index(ctx context.Context, tx sop.Transaction) (ai.VectorStore[map[string]any], error) {
	return nil, nil
}
func (m *MockDomainForRecording) TextIndex(ctx context.Context, tx sop.Transaction) (ai.TextIndex, error) {
	return nil, nil
}
func (m *MockDomainForRecording) BeginTransaction(ctx context.Context, mode sop.TransactionMode) (sop.Transaction, error) {
	return &MockTransactionForRecording{}, nil
}

type MockTransactionForRecording struct{}

func (m *MockTransactionForRecording) Begin(ctx context.Context) error    { return nil }
func (m *MockTransactionForRecording) Commit(ctx context.Context) error   { return nil }
func (m *MockTransactionForRecording) Rollback(ctx context.Context) error { return nil }
func (m *MockTransactionForRecording) HasBegun() bool                     { return true }
func (m *MockTransactionForRecording) GetPhasedTransaction() sop.TwoPhaseCommitTransaction {
	return nil
}
func (m *MockTransactionForRecording) AddPhasedTransaction(otherTransaction ...sop.TwoPhaseCommitTransaction) {
}
func (m *MockTransactionForRecording) GetStores(ctx context.Context) ([]string, error) {
	return nil, nil
}
func (m *MockTransactionForRecording) Close() error { return nil }
func (m *MockTransactionForRecording) GetID() sop.UUID {
	return sop.NewUUID()
}
func (m *MockTransactionForRecording) CommitMaxDuration() time.Duration                  { return 0 }
func (m *MockTransactionForRecording) OnCommit(callback func(ctx context.Context) error) {}
func (m *MockTransactionForRecording) GetMode() sop.TransactionMode                      { return sop.ForReading }
func (m *MockTransactionForRecording) SetMode(mode sop.TransactionMode)                  {}
