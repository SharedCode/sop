package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/jsondb"
)

func TestReproSelect_NestedFilter(t *testing.T) {
	// 1. Setup
	ctx := context.Background()
	dbPath := "test_repro_select_nested"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.InMemory,
	}

	// Create DB
	db := database.NewDatabase(dbOpts)
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	storeName := "products"
	store, err := jsondb.CreateObjectStore(ctx, dbOpts, storeName, tx)
	if err != nil {
		t.Fatalf("CreateObjectStore failed: %v", err)
	}

	// Add items
	items := []struct {
		ID    string
		Value map[string]any
	}{
		{ID: "p1", Value: map[string]any{"name": "TV", "category": "Electronics", "price": 500}},
	}

	for _, item := range items {
		if _, err := store.Add(ctx, item.ID, item.Value); err != nil {
			t.Fatalf("Add failed: %v", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 2. Prepare Agent
	agent := &DataAdminAgent{
		databases: map[string]sop.DatabaseOptions{
			"testdb": dbOpts,
		},
	}
	sessionPayload := &ai.SessionPayload{
		CurrentDB: "testdb",
	}
	ctx = context.WithValue(ctx, "session_payload", sessionPayload)

	// 3. Run Select with nested filter
	args := map[string]any{
		"store": "products",
		// The LLM is passing this structure
		"filter": map[string]any{
			"category": "Electronics",
		},
	}

	// Currently, toolSelect treats `filter` as just another field matching argument?
	// It puts "filter" -> {...} into `valueMatch`.
	// Then `matchesKey(itemValue, valueMatch)` is called.
	// If itemValue is {category: Electronics}, and valueMatch is {filter: {category: Electronics}}.
	// matchesKey will look for "filter" field in itemValue. It won't find it.
	// So it returns false. Result is null/empty.

	result, err := agent.toolSelect(ctx, args)
	if err != nil {
		t.Fatalf("toolSelect failed: %v", err)
	}

	t.Logf("Result: %s", result)

	if !strings.Contains(result, "TV") {
		t.Errorf("Expected TV in results. Nested dictionary 'filter' support missing.")
	}
}
