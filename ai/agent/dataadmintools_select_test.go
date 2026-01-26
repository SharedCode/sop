package agent

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

func TestToolSelect_WithFilter(t *testing.T) {
	// 1. Setup
	ctx := context.Background()
	dbPath := "test_dataadmin_select"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.InMemory,
	}

	// Create DB and Store
	db := database.NewDatabase(dbOpts)
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	storeName := "users"
	// Create the store first
	storeOpts := sop.StoreOptions{
		Name:           storeName,
		SlotLength:     10,
		IsPrimitiveKey: false, // JSON keys
	}
	if _, err := sopdb.NewBtree[any, any](ctx, dbOpts, storeName, tx, nil, storeOpts); err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit creation failed: %v", err)
	}

	// Start new transaction for population
	tx, err = db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction population failed: %v", err)
	}

	store, err := jsondb.OpenStore(ctx, dbOpts, storeName, tx)
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}

	// Add items
	// Keys: {id, group, role}
	items := []struct {
		Key   map[string]any
		Value string
	}{
		{Key: map[string]any{"id": 1, "group": "A", "role": "admin"}, Value: "Alice"},
		{Key: map[string]any{"id": 2, "group": "A", "role": "user"}, Value: "Bob"},
		{Key: map[string]any{"id": 3, "group": "B", "role": "admin"}, Value: "Charlie"},
		{Key: map[string]any{"id": 4, "group": "B", "role": "user"}, Value: "Dave"},
		{Key: map[string]any{"id": 5, "group": "A", "role": "guest"}, Value: "Eve"},
	}

	for _, item := range items {
		if _, err := store.Add(ctx, item.Key, item.Value); err != nil {
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

	// 3. Test Cases
	tests := []struct {
		name          string
		filter        map[string]any
		expectedCount int
		expectedNames []string
	}{
		{
			name:          "Filter by Group A",
			filter:        map[string]any{"group": "A"},
			expectedCount: 3, // Alice, Bob, Eve
			expectedNames: []string{"Alice", "Bob", "Eve"},
		},
		{
			name:          "Filter by Role Admin",
			filter:        map[string]any{"role": "admin"},
			expectedCount: 2, // Alice, Charlie
			expectedNames: []string{"Alice", "Charlie"},
		},
		{
			name:          "Filter by Group A and Role Admin",
			filter:        map[string]any{"group": "A", "role": "admin"},
			expectedCount: 1, // Alice
			expectedNames: []string{"Alice"},
		},
		{
			name:          "Filter by Non-existent",
			filter:        map[string]any{"group": "C"},
			expectedCount: 0,
			expectedNames: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sessionPayload.Transaction = nil

			args := map[string]any{
				"store":     storeName,
				"key_match": tt.filter,
			}

			result, err := agent.toolSelect(ctx, args)
			if err != nil {
				t.Fatalf("toolSelect failed: %v", err)
			}

			if tt.expectedCount == 0 {
				// Expect empty JSON array
				var items []map[string]any
				if err := json.Unmarshal([]byte(result), &items); err != nil {
					t.Fatalf("failed to unmarshal result (expected empty array): %v. Result: %s", err, result)
				}
				if len(items) != 0 {
					t.Errorf("expected 0 items, got %d", len(items))
				}
			} else {
				var items []map[string]any
				if err := json.Unmarshal([]byte(result), &items); err != nil {
					t.Fatalf("failed to unmarshal result: %v", err)
				}

				if len(items) != tt.expectedCount {
					t.Errorf("expected %d items, got %d", tt.expectedCount, len(items))
				}

				// Verify names
				for _, name := range tt.expectedNames {
					found := false
					for _, item := range items {
						if item["value"] == name {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expected to find %s in result", name)
					}
				}
			}
		})
	}
}

func TestToolSelect_WithAlias(t *testing.T) {
	// 1. Setup
	ctx := context.Background()
	dbPath := "test_dataadmin_select_alias"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.InMemory,
	}

	// Create DB and Store
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
	if _, err := sopdb.NewBtree[any, any](ctx, dbOpts, storeName, tx, nil, storeOpts); err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit creation failed: %v", err)
	}

	// Populate
	tx, err = db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction population failed: %v", err)
	}

	store, err := jsondb.OpenStore(ctx, dbOpts, storeName, tx)
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}

	// Add item
	// Key: {id: 101}
	// Value: {name: "John Doe", dept: "Engineering", salary: 100000}
	key := map[string]any{"id": 101}
	value := map[string]any{"name": "John Doe", "dept": "Engineering", "salary": 100000}

	if _, err := store.Add(ctx, key, value); err != nil {
		t.Fatalf("Add failed: %v", err)
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

	// 3. Execute Select with Alias
	args := map[string]any{
		"store": storeName,
		"fields": []string{
			"id AS employee_id",
			"name AS full_name",
			"dept", // No alias
		},
	}

	resultJSON, err := agent.toolSelect(ctx, args)
	if err != nil {
		t.Fatalf("toolSelect failed: %v", err)
	}

	// 4. Verify Result
	var result []map[string]any
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		t.Fatalf("Failed to unmarshal result: %v", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(result))
	}

	item := result[0]

	// Check Key Alias (Now flattened)
	if _, ok := item["employee_id"]; !ok {
		// Only fail if we expected it to be at root (flattened)
		// But in this test case, we inserted {"id": 101}, "John Doe", {"dept": "Sales"}
		// Wait, the input data:
		// agent.toolAdd(ctx, map[string]any{"store": "test_alias", "key": map[string]any{"id": 101}, "value": map[string]any{"name": "John Doe", "dept": "Sales"}})
		// So Key IS a map, Value IS a map. Flattening IS happening.
		t.Errorf("Expected key 'employee_id', got keys: %v", item)
	}
	if val, _ := item["employee_id"].(float64); val != 101 {
		t.Errorf("Expected employee_id 101, got %v", item["employee_id"])
	}

	// Check Value Alias (Now flattened)
	if _, ok := item["full_name"]; !ok {
		t.Errorf("Expected value 'full_name', got keys: %v", item)
	}
	if val, _ := item["full_name"].(string); val != "John Doe" {
		t.Errorf("Expected full_name 'John Doe', got %v", item["full_name"])
	}

	// Check No Alias
	if _, ok := item["dept"]; !ok {
		t.Errorf("Expected value 'dept', got keys: %v", item)
	}
}
