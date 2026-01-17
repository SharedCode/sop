package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	core_db "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

func TestToolListStores_SchemaEnrichment(t *testing.T) {
	// 1. Setup Temp Dir
	tmpDir, err := os.MkdirTemp("", "sop_schema_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	dbName := "test_db_schema"
	opts := sop.DatabaseOptions{
		StoresFolders: []string{tmpDir},
	}

	// 2. Create Agent
	dbs := map[string]sop.DatabaseOptions{
		dbName: opts,
	}
	// Need to initialize generic registry if NewDataAdminAgent doesn't do it properly for all tools.
	// But NewDataAdminAgent does initialize registry.
	agent := NewDataAdminAgent(Config{}, dbs, nil)

	// 3. Populate Data directly
	ctx := context.Background()

	// Create DB helper
	db := database.NewDatabase(opts)
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Create Store
	store, err := jsondb.CreateObjectStore(ctx, opts, "users", tx)
	if err != nil {
		// cleanup
		if tx != nil {
			tx.Rollback(ctx)
		}
		t.Fatalf("CreateObjectStore failed: %v", err)
	}

	// Add Data
	// CreateObjectStore defaults to primitive string key
	if _, err := store.Add(ctx, "u1", map[string]interface{}{"first_name": "John", "age": 30}); err != nil {
		tx.Rollback(ctx)
		t.Fatalf("Add failed: %v", err)
	}

	// Create another store with INT keys (Primitive) to simulate "users_by_age" scenario
	// Note: We use database.NewBtree directly to bypass jsondb helpers for this specific test case.
	idxOpts := sop.StoreOptions{
		IsUnique:       false,
		IsPrimitiveKey: true,
	}
	idxStore, err := core_db.NewBtree[int, string](ctx, opts, "users_by_age", tx, nil, idxOpts)
	if err != nil {
		tx.Rollback(ctx)
		t.Fatalf("Failed to create users_by_age: %v", err)
	}
	idxStore.Add(ctx, 30, "u1")

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// Set Payload for agent tools
	payload := &ai.SessionPayload{
		CurrentDB: dbName,
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	// 4. Call list_stores
	// We pass "database" arg explicitly or rely on session payload?
	// toolListStores logic: if dbName == "" { dbName = p.CurrentDB }
	// So we can pass empty map if p.CurrentDB is set.
	res, err := agent.toolListStores(ctx, map[string]any{"database": dbName})
	if err != nil {
		t.Fatalf("toolListStores failed: %v", err)
	}

	// 5. Verify Output
	t.Logf("ListStores Result:\n%s", res)

	if !strings.Contains(res, "users") {
		t.Error("Result should contain store name 'users'")
	}
	// Verification of Schema Enrichment
	// Expected schema: first_name:string, age:number
	if !strings.Contains(res, "first_name") {
		t.Error("Result should contain schema field 'first_name'")
	}
	if !strings.Contains(res, "age") {
		t.Error("Result should contain schema field 'age'")
	}
}
