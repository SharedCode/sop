package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	core_database "github.com/sharedcode/sop/database"
)

func TestToolJoin_SuffixHandling(t *testing.T) {
	// 1. Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}

	// Initialize SystemDB
	sysDB := database.NewDatabase(dbOpts)

	// 2. Create Test Stores with Data
	ctx := context.Background()
	tx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Store 1: employees
	b3Left, err := core_database.NewBtree[string, any](ctx, dbOpts, "employees", tx, nil)
	if err != nil {
		t.Fatalf("Failed to create left store: %v", err)
	}
	b3Left.Add(ctx, "emp1", map[string]any{"region": "APAC", "department": "Sales"})

	// Store 2: departments
	b3Right, err := core_database.NewBtree[string, any](ctx, dbOpts, "departments", tx, nil)
	if err != nil {
		t.Fatalf("Failed to create right store: %v", err)
	}
	b3Right.Add(ctx, "d1", map[string]any{"department": "Engineering", "region": "APAC"})

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit setup transaction: %v", err)
	}

	// 3. Setup Agent
	agentCfg := Config{
		ID: "sql_admin",
	}
	adminAgent := &DataAdminAgent{
		Config:    agentCfg,
		databases: map[string]sop.DatabaseOptions{"test_db": dbOpts},
		systemDB:  sysDB,
	}

	// 4. Test Case: Request "Department_1" which doesn't exist, but "department" does.
	// This simulates the LLM trying to rename a colliding column by appending _1.
	args := map[string]any{
		"database":          "test_db",
		"left_store":        "employees",
		"right_store":       "departments",
		"left_join_fields":  []string{"region"},
		"right_join_fields": []string{"region"},
		"fields":            []string{"region", "department_1"}, // Should map to "department"
	}

	// Setup Context with Payload
	payload := &ai.SessionPayload{
		CurrentDB: "test_db",
	}
	ctx = context.WithValue(ctx, "session_payload", payload)

	resp, err := adminAgent.toolJoin(ctx, args)
	if err != nil {
		t.Fatalf("toolJoin failed: %v", err)
	}

	t.Logf("Response: %s", resp)

	// Parse Response (JSON Array)
	var results []map[string]any
	if err := json.Unmarshal([]byte(resp), &results); err != nil {
		t.Fatalf("Failed to parse response: %v", err)
	}

	if len(results) == 0 {
		t.Fatalf("No results returned")
	}

	firstRow := results[0]
	valMap := firstRow["value"].(map[string]any)

	// Check if Department has a value (normalized from department_1)
	if val, ok := valMap["Department"]; ok {
		if val == nil {
			t.Errorf("Expected value for 'Department', got nil")
		} else {
			t.Logf("Got value for Department: %v", val)
		}
	} else {
		t.Errorf("Field 'Department' missing from response. Available keys: %v", keys(valMap))
	}
}

func keys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
