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

func TestToolJoin_Chained_InnerThenRight(t *testing.T) {
	// Scenario: (Users INNER JOIN Orders) RIGHT JOIN Feedbacks
	// Users: 1 (Alice), 2 (Bob)
	// Orders: 101 (User 1), 102 (User 1). User 2 has no orders.
	// Feedbacks: F1 (Order 101 - Good), F2 (Order 999 - Orphan/External)

	// Expected Result:
	// 1. (U join O) -> { (U1, O101), (U1, O102) }
	// 2. (Result1 right join F) ->
	//    - F1 matches O101 -> { F1, U1, O101 }
	//    - F2 matches O999 -> No Match -> { F2, null, null }

	// 1. Setup DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)

	ctx := context.Background()
	tx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Create Stores
	users, _ := core_database.NewBtree[string, any](ctx, dbOpts, "users_chain", tx, nil)
	users.Add(ctx, "u1", map[string]any{"uid": 1.0, "name": "Alice"})
	users.Add(ctx, "u2", map[string]any{"uid": 2.0, "name": "Bob"})

	orders, _ := core_database.NewBtree[string, any](ctx, dbOpts, "orders_chain", tx, nil)
	orders.Add(ctx, "o1", map[string]any{"oid": 101.0, "uid": 1.0, "item": "Book"})
	orders.Add(ctx, "o2", map[string]any{"oid": 102.0, "uid": 1.0, "item": "Pen"})

	feedbacks, _ := core_database.NewBtree[string, any](ctx, dbOpts, "feedbacks_chain", tx, nil)
	feedbacks.Add(ctx, "f1", map[string]any{"fid": "F1", "oid": 101.0, "msg": "Great"})
	feedbacks.Add(ctx, "f2", map[string]any{"fid": "F2", "oid": 999.0, "msg": "Unknown"})

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 2. Prepare Agent
	agentCfg := Config{ID: "test_admin"}
	agent := &DataAdminAgent{
		Config:    agentCfg,
		databases: map[string]sop.DatabaseOptions{"test_db": dbOpts},
		systemDB:  sysDB,
	}

	// 3. Script
	scriptJSON := `[
		{"op": "open_db", "args": {"name": "test_db"}},
		{"op": "begin_tx", "args": {"database": "test_db", "mode": "read"}, "result_var": "tx"},
		
		{"op": "open_store", "args": {"transaction": "tx", "name": "users_chain"}, "result_var": "s_users"},
		{"op": "open_store", "args": {"transaction": "tx", "name": "orders_chain"}, "result_var": "s_orders"},
		{"op": "open_store", "args": {"transaction": "tx", "name": "feedbacks_chain"}, "result_var": "s_feedbacks"},

		{"op": "scan", "args": {"store": "s_users"}, "result_var": "stream_users"},
		
		{"op": "join", "args": {"store": "@s_orders", "type": "inner", "on": {"uid": "uid"}}, "input_var": "stream_users", "result_var": "users_orders"},
		
		{"op": "join", "args": {"store": "@s_feedbacks", "type": "right", "on": {"s_orders.oid": "oid"}}, "input_var": "users_orders", "result_var": "output"},

		{"op": "commit_tx", "args": {"transaction": "tx"}}
	]`

	sessionPayload := &ai.SessionPayload{CurrentDB: "test_db"}
	ctx = context.WithValue(ctx, "session_payload", sessionPayload)

	resp, err := agent.toolExecuteScript(ctx, map[string]any{"script": scriptJSON})
	if err != nil {
		t.Fatalf("Script failed: %v", err)
	}

	t.Logf("Response: %s", resp)

	// 4. Validate
	var results []map[string]any
	if err := json.Unmarshal([]byte(resp), &results); err != nil {
		t.Fatalf("Parse error: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	resultMap := make(map[string]map[string]any)
	for _, r := range results {
		fid := r["fid"].(string)
		resultMap[fid] = r
	}

	// Check F1 (Matched)
	if r, ok := resultMap["F1"]; !ok {
		t.Error("Missing F1")
	} else {
		if r["name"] != "Alice" {
			t.Errorf("F1 should map to Alice, got %v", r["name"])
		}
		if r["s_orders.item"] != "Book" {
			t.Errorf("F1 should map to Book, got %v", r["s_orders.item"])
		}
	}

	// Check F2 (Unmatched)
	if r, ok := resultMap["F2"]; !ok {
		t.Error("Missing F2")
	} else {
		if r["name"] != nil {
			t.Errorf("F2 should have nil name, got %v", r["name"])
		}
		if r["s_orders.item"] != nil {
			t.Errorf("F2 should have nil item, got %v", r["s_orders.item"])
		}
		if r["msg"] != "Unknown" {
			t.Errorf("F2 msg mismatch")
		}
	}
}
