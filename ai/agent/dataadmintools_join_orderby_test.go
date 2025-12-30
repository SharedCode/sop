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

func TestToolJoin_OrderBy(t *testing.T) {
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

	// Left Store (Primitive Key)
	leftOpts := sop.StoreOptions{Name: "left", SlotLength: 10, IsPrimitiveKey: true}
	left, _ := sopdb.NewBtree[string, any](ctx, dbOpts, "left", t2, nil, leftOpts)
	left.Add(ctx, "1", map[string]any{"id": "1", "val": "L1"})
	left.Add(ctx, "2", map[string]any{"id": "2", "val": "L2"})
	left.Add(ctx, "3", map[string]any{"id": "3", "val": "L3"})

	// Right Store (Primitive Key)
	rightOpts := sop.StoreOptions{Name: "right", SlotLength: 10, IsPrimitiveKey: true}
	right, _ := sopdb.NewBtree[string, any](ctx, dbOpts, "right", t2, nil, rightOpts)
	right.Add(ctx, "1", map[string]any{"id": "1", "val": "R1"})
	right.Add(ctx, "2", map[string]any{"id": "2", "val": "R2"})
	right.Add(ctx, "3", map[string]any{"id": "3", "val": "R3"})

	t2.Commit(ctx)

	// Helper to extract key
	getKey := func(item map[string]any) string {
		k := item["key"]
		if s, ok := k.(string); ok {
			return s
		}
		return ""
	}

	// Test 1: Default (ASC)
	args := map[string]any{
		"left_store":        "left",
		"right_store":       "right",
		"left_join_fields":  []string{"id"},
		"right_join_fields": []string{"id"},
	}
	res, err := agent.Execute(ctx, "join", args)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}

	var items []map[string]any
	json.Unmarshal([]byte(res), &items)
	if len(items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(items))
	}
	if getKey(items[0]) != "1" {
		t.Errorf("Expected first item key '1', got '%v'", items[0]["key"])
	}

	// Test 2: DESC
	args = map[string]any{
		"left_store":        "left",
		"right_store":       "right",
		"left_join_fields":  []string{"id"},
		"right_join_fields": []string{"id"},
		"order_by":          "key desc",
	}
	res, err = agent.Execute(ctx, "join", args)
	if err != nil {
		t.Fatalf("Join failed: %v", err)
	}
	json.Unmarshal([]byte(res), &items)
	if len(items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(items))
	}
	if getKey(items[0]) != "3" {
		t.Errorf("Expected first item key '3', got '%v'", items[0]["key"])
	}
	if getKey(items[1]) != "2" {
		t.Errorf("Expected second item key '2', got '%v'", items[1]["key"])
	}
	if getKey(items[2]) != "1" {
		t.Errorf("Expected third item key '1', got '%v'", items[2]["key"])
	}
}
