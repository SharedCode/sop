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

func TestToolSelect_OrderBy(t *testing.T) {
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

	// Create Store and Data using sopdb directly
	t2, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	storeOpts := sop.StoreOptions{
		Name:           "items",
		SlotLength:     10,
		IsPrimitiveKey: true,
	}
	s, _ := sopdb.NewBtree[string, any](ctx, dbOpts, "items", t2, nil, storeOpts)
	s.Add(ctx, "1", map[string]any{"name": "one"})
	s.Add(ctx, "2", map[string]any{"name": "two"})
	s.Add(ctx, "3", map[string]any{"name": "three"})
	t2.Commit(ctx)

	// Helper to extract name from key map
	getKeyName := func(item map[string]any) string {
		k := item["key"]
		if s, ok := k.(string); ok {
			return s
		}
		return ""
	}

	// Test 1: Default (ASC)
	args := map[string]any{
		"store": "items",
	}
	res, err := agent.Execute(ctx, "select", args)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}

	var items []map[string]any
	json.Unmarshal([]byte(res), &items)
	if len(items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(items))
	}

	if getKeyName(items[0]) != "1" {
		t.Errorf("Expected first item key '1', got '%v'", items[0]["key"])
	}

	// Test 2: Explicit ASC
	args = map[string]any{
		"store":    "items",
		"order_by": "key asc",
	}
	res, err = agent.Execute(ctx, "select", args)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	json.Unmarshal([]byte(res), &items)
	if getKeyName(items[0]) != "1" {
		t.Errorf("Expected first item key '1', got '%v'", items[0]["key"])
	}

	// Test 3: DESC
	args = map[string]any{
		"store":    "items",
		"order_by": "key desc",
	}
	res, err = agent.Execute(ctx, "select", args)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	json.Unmarshal([]byte(res), &items)
	if len(items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(items))
	}
	if getKeyName(items[0]) != "3" {
		t.Errorf("Expected first item key '3', got '%v'", items[0]["key"])
	}
	if getKeyName(items[1]) != "2" {
		t.Errorf("Expected second item key '2', got '%v'", items[1]["key"])
	}
	// Test 4: Implicit Key DESC (just "desc")
	args = map[string]any{
		"store":    "items",
		"order_by": "desc",
	}
	res, err = agent.Execute(ctx, "select", args)
	if err != nil {
		t.Fatalf("Select failed: %v", err)
	}
	json.Unmarshal([]byte(res), &items)
	if len(items) != 3 {
		t.Fatalf("Expected 3 items, got %d", len(items))
	}
	if getKeyName(items[0]) != "3" {
		t.Errorf("Expected first item key '3', got '%v'", items[0]["key"])
	}
}
