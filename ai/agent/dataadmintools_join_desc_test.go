package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
)

func TestToolJoin_OrderBy_RightStoreDirection(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)

	cfg := Config{Name: "TestAgent"}
	dbs := make(map[string]sop.DatabaseOptions)
	agent := NewDataAdminAgent(cfg, dbs, sysDB)

	ctx := context.Background()
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "system"})

	// Create Stores and Data
	t2, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)

	// Left Store: Departments
	leftOpts := sop.StoreOptions{Name: "departments", SlotLength: 10, IsPrimitiveKey: true}
	left, _ := sopdb.NewBtree[string, any](ctx, dbOpts, "departments", t2, nil, leftOpts)
	left.Add(ctx, "D1", map[string]any{"id": "D1", "name": "Sales"})

	// Right Store: Employees
	rightOpts := sop.StoreOptions{Name: "employees", SlotLength: 10, IsPrimitiveKey: true}
	right, _ := sopdb.NewBtree[string, any](ctx, dbOpts, "employees", t2, nil, rightOpts)
	// Add in order E1, E2, E3
	right.Add(ctx, "E1", map[string]any{"id": "E1", "dept_id": "D1", "name": "Alice"})
	right.Add(ctx, "E2", map[string]any{"id": "E2", "dept_id": "D1", "name": "Bob"})
	right.Add(ctx, "E3", map[string]any{"id": "E3", "dept_id": "D1", "name": "Charlie"})

	t2.Commit(ctx)

	// Test DESC
	args := map[string]any{
		"left_store":        "departments",
		"right_store":       "employees",
		"left_join_fields":  []string{"id"},
		"right_join_fields": []string{"dept_id"},
		"order_by":          "key desc",
		"fields":            []string{"employees.id"},
	}
	res, err := agent.Execute(ctx, "join", args)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	var items []map[string]any
	json.Unmarshal([]byte(res), &items)

	// We expect 3 items
	if len(items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(items))
	}

	// Check Order: Should be E3, E2, E1
	getVal := func(item map[string]any) string {
		v := item["key"].(map[string]any)
        // Try "Id" (Title Case) or "id"
        if val, ok := v["Id"]; ok {
            return val.(string)
        }
        return v["id"].(string)
    }

    if getVal(items[0]) != "E3" {
		t.Errorf("Expected first item 'E3', got '%s'", getVal(items[0]))
	}
	if getVal(items[1]) != "E2" {
		t.Errorf("Expected second item 'E2', got '%s'", getVal(items[1]))
	}
	if getVal(items[2]) != "E1" {
		t.Errorf("Expected third item 'E1', got '%s'", getVal(items[2]))
	}
}
