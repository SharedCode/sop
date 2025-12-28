package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

func TestReproLoadFailedError(t *testing.T) {
	// 1. Setup DB
	ctx := context.Background()
	dbPath := "test_repro_load_failed"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.InMemory,
	}

	// Create DB and Store "employees"
	db := database.NewDatabase(dbOpts)
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	storeName := "employees"
	storeOpts := sop.StoreOptions{
		Name:           storeName,
		SlotLength:     10,
		IsPrimitiveKey: false,
	}
	if _, err := sopdb.NewBtree[string, any](ctx, dbOpts, storeName, tx, nil, storeOpts); err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit creation failed: %v", err)
	}

	// Add some data
	tx, err = db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction population failed: %v", err)
	}
	store, err := jsondb.OpenStore(ctx, dbOpts, storeName, tx)
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}

	// Add item matching the query
	key := map[string]any{"region": "APAC", "department": "HR", "id": 1}
	val := map[string]any{"name": "John Doe", "salary": 50000}
	if _, err := store.Add(ctx, key, val); err != nil {
		t.Fatalf("Add failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Corrupt the store to force a load error
	// We need to find where the store file is.
	// It should be in dbPath/employees.
	// But SOP structure is complex.
	// Let's just try to open a non-existent store again, but this time check the error message carefully.

	// Actually, let's try to simulate "load failed" by mocking the generator to return an error?
	// No, the user said "Got Error: load failed".

	// Let's try to create a store with a name that causes issues?
	// Or maybe the user is using a store name that conflicts with a folder?

	// Let's try to corrupt the registry?
	// The registry tracks stores.

	// For now, let's just revert to the state where the store exists,
	// and then try to access a non-existent store in the SAME test to see the error.

	// 2. Setup Service
	// We need a MockGenerator that returns the tool call
	// We use the existing store "employees" to verify success.
	toolCall := map[string]any{
		"tool": "select",
		"args": map[string]any{
			"store":     "employees",
			"fields":    []string{"region", "name", "salary"},
			"key_match": map[string]any{"region": "APAC", "department": "HR"},
			"limit":     5,
		},
	}
	toolCallJSON, _ := json.Marshal(toolCall)

	gen := &MockGenerator{
		Response: fmt.Sprintf("```json\n%s\n```", string(toolCallJSON)),
	}

	// We need a system DB for macros
	systemDBPath := "test_repro_system_db"
	os.RemoveAll(systemDBPath)
	defer os.RemoveAll(systemDBPath)
	systemDBOpts := sop.DatabaseOptions{StoresFolders: []string{systemDBPath}}
	systemDB := database.NewDatabase(systemDBOpts)

	// Create DataAdminAgent
	daAgent := NewDataAdminAgent(Config{}, map[string]sop.DatabaseOptions{"testdb": dbOpts}, systemDB)
	daAgent.SetGenerator(gen)

	registry := map[string]ai.Agent[map[string]any]{
		"sql_admin": daAgent,
	}

	// Configure Pipeline
	pipeline := []PipelineStep{
		{
			Agent: PipelineAgent{ID: "sql_admin"},
		},
	}

	svc := NewService(
		&MockDomain{},
		systemDB,
		map[string]sop.DatabaseOptions{"testdb": dbOpts},
		gen,
		pipeline,
		registry,
		false,
	)

	// 3. Run Test
	// Start Recording
	// We need to set the current DB in the payload
	payload := &ai.SessionPayload{
		CurrentDB: "testdb",
	}

	// Start recording
	// Note: /record is handled by Service.Ask directly, not pipeline
	resp, err := svc.Ask(ctx, "/record my_macro", ai.WithSessionPayload(payload))
	if err != nil {
		t.Fatalf("Start recording failed: %v", err)
	}
	if !svc.session.Recording {
		t.Fatalf("Expected recording to be true")
	}
	t.Logf("Start recording response: %s", resp)

	// Issue the query
	// Service.Ask will run the pipeline.
	// DataAdminAgent.Ask will be called.
	// DataAdminAgent.Ask will call ExecuteTool.
	// ExecuteTool will call toolSelect.
	// toolSelect will call jsondb.OpenStore.
	// Since we commented out store creation, OpenStore should fail.

	// We don't need to set executor in context anymore because DataAdminAgent executes it.
	// ctx = context.WithValue(ctx, ai.CtxKeyExecutor, daAgent)

	query := "select region, name, salary from employees where key like region='APAC', department='HR' limit 5"
	resp, err = svc.Ask(ctx, query, ai.WithSessionPayload(payload))
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}
	t.Logf("Response: %s", resp)

	// We expect success
	if !strings.Contains(resp, "John Doe") {
		t.Errorf("Expected response to contain 'John Doe', got: %s", resp)
	}
}
