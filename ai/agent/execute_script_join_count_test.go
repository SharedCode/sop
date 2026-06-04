package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	core_database "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

// TestExecuteScript_JoinCountIsolation validates that join operations produce the correct number of results
// by comparing script execution against direct btree operations.
//
// Test Setup:
// - 25 users named "John" (u1-u25)
// - 10 users named "Jane" (u26-u35)
// - 30 orders (o1-o30)
// - 22 users named John have orders with total_amount > 500 (should match)
// - 3 users named John have orders with total_amount <= 500 (should be filtered out)
// - Jane's orders are varied but irrelevant to this query
//
// Expected Result: 22 matching records
func TestExecuteScript_JoinCountIsolation(t *testing.T) {
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}

	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()
	tx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("begin setup tx: %v", err)
	}

	// Create stores
	users, err := core_database.NewBtree[any, any](ctx, dbOpts, "users", tx, nil)
	if err != nil {
		t.Fatalf("create users store: %v", err)
	}

	orders, err := core_database.NewBtree[any, any](ctx, dbOpts, "orders", tx, nil)
	if err != nil {
		t.Fatalf("create orders store: %v", err)
	}

	usersOrders, err := core_database.NewBtree[any, any](ctx, dbOpts, "users_orders", tx, nil)
	if err != nil {
		t.Fatalf("create users_orders store: %v", err)
	}

	// Generate test data
	johnUsersWithHighOrders := 0
	johnUsersWithLowOrders := 0

	// Add 25 users named "John"
	for i := 1; i <= 25; i++ {
		userKey := fmt.Sprintf("u%d", i)
		user := map[string]any{
			"first_name": "John",
			"last_name":  fmt.Sprintf("Doe%d", i),
			"age":        20 + i,
			"email":      fmt.Sprintf("john.doe%d@example.com", i),
		}
		if _, err := users.Add(ctx, userKey, user); err != nil {
			t.Fatalf("add user %s: %v", userKey, err)
		}
	}

	// Add 10 users named "Jane" (control group)
	for i := 26; i <= 35; i++ {
		userKey := fmt.Sprintf("u%d", i)
		user := map[string]any{
			"first_name": "Jane",
			"last_name":  fmt.Sprintf("Smith%d", i),
			"age":        20 + i,
			"email":      fmt.Sprintf("jane.smith%d@example.com", i),
		}
		if _, err := users.Add(ctx, userKey, user); err != nil {
			t.Fatalf("add user %s: %v", userKey, err)
		}
	}

	orderIdx := 1

	// Link first 22 John users to high-value orders (> 500)
	for i := 1; i <= 22; i++ {
		userKey := fmt.Sprintf("u%d", i)
		orderKey := fmt.Sprintf("o%d", orderIdx)
		orderIdx++

		order := map[string]any{
			"total_amount": 500 + (i * 50), // 550, 600, 650, ..., 1600
			"status":       "completed",
			"order_date":   fmt.Sprintf("2026-01-%02d", i),
		}
		if _, err := orders.Add(ctx, orderKey, order); err != nil {
			t.Fatalf("add order %s: %v", orderKey, err)
		}

		// Link user to order
		if _, err := usersOrders.Add(ctx, userKey, orderKey); err != nil {
			t.Fatalf("add user-order link %s->%s: %v", userKey, orderKey, err)
		}
		johnUsersWithHighOrders++
	}

	// Link next 3 John users to low-value orders (<= 500)
	for i := 23; i <= 25; i++ {
		userKey := fmt.Sprintf("u%d", i)
		orderKey := fmt.Sprintf("o%d", orderIdx)
		orderIdx++

		order := map[string]any{
			"total_amount": 100 + (i * 10), // 330, 340, 350
			"status":       "completed",
			"order_date":   fmt.Sprintf("2026-01-%02d", i),
		}
		if _, err := orders.Add(ctx, orderKey, order); err != nil {
			t.Fatalf("add order %s: %v", orderKey, err)
		}

		// Link user to order
		if _, err := usersOrders.Add(ctx, userKey, orderKey); err != nil {
			t.Fatalf("add user-order link %s->%s: %v", userKey, orderKey, err)
		}
		johnUsersWithLowOrders++
	}

	// Link some Jane users to orders (mix of high and low values) - these should all be filtered out by first_name filter
	for i := 26; i <= 30; i++ {
		userKey := fmt.Sprintf("u%d", i)
		orderKey := fmt.Sprintf("o%d", orderIdx)
		orderIdx++

		order := map[string]any{
			"total_amount": 200 + (i * 30), // Various amounts
			"status":       "completed",
			"order_date":   fmt.Sprintf("2026-01-%02d", i),
		}
		if _, err := orders.Add(ctx, orderKey, order); err != nil {
			t.Fatalf("add order %s: %v", orderKey, err)
		}

		// Link user to order
		if _, err := usersOrders.Add(ctx, userKey, orderKey); err != nil {
			t.Fatalf("add user-order link %s->%s: %v", userKey, orderKey, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit setup tx: %v", err)
	}

	t.Logf("Test data created:")
	t.Logf("  - John users with orders > 500: %d (expected matches)", johnUsersWithHighOrders)
	t.Logf("  - John users with orders <= 500: %d (should be filtered out)", johnUsersWithLowOrders)
	t.Logf("  - Total John users: %d", 25)
	t.Logf("  - Total Jane users: %d (control, all filtered)", 10)
	t.Logf("  - Total orders: %d", orderIdx-1)

	// Verify data with direct btree queries
	verifyTx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		t.Fatalf("begin verify tx: %v", err)
	}

	verifyUsers, err := core_database.NewBtree[any, any](ctx, dbOpts, "users", verifyTx, nil)
	if err != nil {
		t.Fatalf("open verify users: %v", err)
	}

	// Count John users directly
	johnCount := 0
	found, err := verifyUsers.First(ctx)
	if err != nil {
		t.Fatalf("users.First: %v", err)
	}
	for found {
		key := verifyUsers.GetCurrentKey()
		value, err := verifyUsers.GetCurrentValue(ctx)
		if err != nil {
			t.Fatalf("users.GetCurrentValue: %v", err)
		}
		if value != nil {
			valueMap, ok := value.(map[string]any)
			if ok && valueMap["first_name"] == "John" {
				johnCount++
				t.Logf("  Direct scan found John user: key=%v, age=%v", key, valueMap["age"])
			}
		}
		found, err = verifyUsers.Next(ctx)
		if err != nil {
			t.Fatalf("users.Next: %v", err)
		}
	}

	verifyTx.Rollback(ctx)

	if johnCount != 25 {
		t.Fatalf("Direct btree scan: expected 25 John users, found %d", johnCount)
	}
	t.Logf("✓ Direct btree scan verified: %d John users", johnCount)

	// Now execute the script
	agent := &CopilotAgent{
		systemDB: sysDB,
		databases: map[string]sop.DatabaseOptions{
			"test_db": dbOpts,
		},
		StoreOpener: func(ctx context.Context, dbOpts sop.DatabaseOptions, storeName string, tx sop.Transaction) (jsondb.StoreAccessor, error) {
			s, err := core_database.NewBtree[any, any](ctx, dbOpts, storeName, tx, nil)
			if err != nil {
				return nil, err
			}
			return &testStoreWrapper{BtreeInterface: s}, nil
		},
	}

	script := []map[string]any{
		{"args": map[string]any{"mode": "read"}, "op": "begin_tx", "result_var": "tx"},
		{"args": map[string]any{"name": "users", "transaction": "tx"}, "op": "open_store", "result_var": "users_store"},
		{"args": map[string]any{"name": "users_orders", "transaction": "tx"}, "op": "open_store", "result_var": "users_orders_store"},
		{"args": map[string]any{"name": "orders", "transaction": "tx"}, "op": "open_store", "result_var": "orders_store"},
		{"args": map[string]any{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}, "store": "users_store"}, "op": "select", "result_var": "matched_users"},
		{"args": map[string]any{"on": map[string]any{"key": "key"}, "store": "users_orders_store"}, "input_var": "matched_users", "op": "join", "result_var": "user_order_links"},
		{"args": map[string]any{"on": map[string]any{"users_orders.value": "key"}, "store": "orders_store"}, "input_var": "user_order_links", "op": "join", "result_var": "joined_orders"},
		{"args": map[string]any{"condition": map[string]any{"orders.total_amount": map[string]any{"$gt": 500}}}, "input_var": "joined_orders", "op": "filter", "result_var": "filtered_orders"},
		{"args": map[string]any{"transaction": "tx"}, "op": "commit_tx"},
		{"input_var": "filtered_orders", "op": "return"},
	}

	scriptBytes, err := json.Marshal(script)
	if err != nil {
		t.Fatalf("marshal script: %v", err)
	}

	ctxWithPayload := context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "test_db"})
	result, err := agent.toolExecuteScript(ctxWithPayload, map[string]any{"script": string(scriptBytes)})
	if err != nil {
		t.Fatalf("toolExecuteScript failed: %v", err)
	}

	t.Logf("Script execution result (first 500 chars): %s", truncate(result, 500))

	// Parse result to count records
	var resultData []map[string]any
	if err := json.Unmarshal([]byte(result), &resultData); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}

	resultCount := len(resultData)
	expectedCount := johnUsersWithHighOrders

	t.Logf("Result count: %d", resultCount)
	t.Logf("Expected count: %d", expectedCount)

	if resultCount != expectedCount {
		t.Errorf("MISMATCH: Script returned %d records, expected %d", resultCount, expectedCount)
		t.Errorf("Missing %d records - likely issue in join/materialization logic", expectedCount-resultCount)

		// Show what we got
		for i, record := range resultData {
			t.Logf("  Record %d: first_name=%v, total_amount=%v", i+1, record["first_name"], record["total_amount"])
		}
	} else {
		t.Logf("✓ Script execution returned correct count: %d records", resultCount)
	}

	// Verify all results are John with amount > 500
	for i, record := range resultData {
		firstName, ok1 := record["first_name"].(string)
		totalAmount, ok2 := record["total_amount"].(float64)

		if !ok1 || !ok2 {
			t.Errorf("Record %d: invalid types - first_name=%T, total_amount=%T", i, record["first_name"], record["total_amount"])
			continue
		}

		if firstName != "John" {
			t.Errorf("Record %d: expected first_name=John, got %s", i, firstName)
		}

		if totalAmount <= 500 {
			t.Errorf("Record %d: expected total_amount > 500, got %.2f", i, totalAmount)
		}
	}
}

// TestExecuteScript_JoinCountDebug provides detailed logging at each join stage
// to identify exactly where records are being lost
func TestExecuteScript_JoinCountDebug(t *testing.T) {
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}

	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()
	tx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("begin setup tx: %v", err)
	}

	// Simplified data: 10 John users, all with high-value orders
	users, _ := core_database.NewBtree[any, any](ctx, dbOpts, "users", tx, nil)
	orders, _ := core_database.NewBtree[any, any](ctx, dbOpts, "orders", tx, nil)
	usersOrders, _ := core_database.NewBtree[any, any](ctx, dbOpts, "users_orders", tx, nil)

	for i := 1; i <= 10; i++ {
		userKey := fmt.Sprintf("u%d", i)
		orderKey := fmt.Sprintf("o%d", i)

		users.Add(ctx, userKey, map[string]any{
			"first_name": "John",
			"last_name":  fmt.Sprintf("Doe%d", i),
			"age":        20 + i,
		})

		orders.Add(ctx, orderKey, map[string]any{
			"total_amount": 1000 + (i * 100),
			"status":       "completed",
		})

		usersOrders.Add(ctx, userKey, orderKey)
	}

	tx.Commit(ctx)

	agent := &CopilotAgent{
		systemDB: sysDB,
		databases: map[string]sop.DatabaseOptions{
			"test_db": dbOpts,
		},
		StoreOpener: func(ctx context.Context, dbOpts sop.DatabaseOptions, storeName string, tx sop.Transaction) (jsondb.StoreAccessor, error) {
			s, _ := core_database.NewBtree[any, any](ctx, dbOpts, storeName, tx, nil)
			return &testStoreWrapper{BtreeInterface: s}, nil
		},
	}

	// Stage 1: Select John users
	script1 := []map[string]any{
		{"args": map[string]any{"mode": "read"}, "op": "begin_tx", "result_var": "tx"},
		{"args": map[string]any{"name": "users", "transaction": "tx"}, "op": "open_store", "result_var": "users_store"},
		{"args": map[string]any{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}, "store": "users_store"}, "op": "select", "result_var": "matched_users"},
		{"args": map[string]any{"transaction": "tx"}, "op": "commit_tx"},
		{"input_var": "matched_users", "op": "return"},
	}

	result1 := executeScriptHelper(t, agent, ctx, script1)
	var stage1Data []map[string]any
	json.Unmarshal([]byte(result1), &stage1Data)
	t.Logf("STAGE 1 (select): %d John users found", len(stage1Data))

	// Stage 2: After first join (users -> users_orders)
	script2 := []map[string]any{
		{"args": map[string]any{"mode": "read"}, "op": "begin_tx", "result_var": "tx"},
		{"args": map[string]any{"name": "users", "transaction": "tx"}, "op": "open_store", "result_var": "users_store"},
		{"args": map[string]any{"name": "users_orders", "transaction": "tx"}, "op": "open_store", "result_var": "users_orders_store"},
		{"args": map[string]any{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}, "store": "users_store"}, "op": "select", "result_var": "matched_users"},
		{"args": map[string]any{"on": map[string]any{"key": "key"}, "store": "users_orders_store"}, "input_var": "matched_users", "op": "join", "result_var": "user_order_links"},
		{"args": map[string]any{"transaction": "tx"}, "op": "commit_tx"},
		{"input_var": "user_order_links", "op": "return"},
	}

	result2 := executeScriptHelper(t, agent, ctx, script2)
	var stage2Data []map[string]any
	json.Unmarshal([]byte(result2), &stage2Data)
	t.Logf("STAGE 2 (after 1st join): %d records", len(stage2Data))
	if len(stage2Data) < len(stage1Data) {
		t.Errorf("❌ LOST %d records in first join!", len(stage1Data)-len(stage2Data))
	}

	// Stage 3: After second join (users_orders -> orders)
	script3 := []map[string]any{
		{"args": map[string]any{"mode": "read"}, "op": "begin_tx", "result_var": "tx"},
		{"args": map[string]any{"name": "users", "transaction": "tx"}, "op": "open_store", "result_var": "users_store"},
		{"args": map[string]any{"name": "users_orders", "transaction": "tx"}, "op": "open_store", "result_var": "users_orders_store"},
		{"args": map[string]any{"name": "orders", "transaction": "tx"}, "op": "open_store", "result_var": "orders_store"},
		{"args": map[string]any{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}, "store": "users_store"}, "op": "select", "result_var": "matched_users"},
		{"args": map[string]any{"on": map[string]any{"key": "key"}, "store": "users_orders_store"}, "input_var": "matched_users", "op": "join", "result_var": "user_order_links"},
		{"args": map[string]any{"on": map[string]any{"users_orders.value": "key"}, "store": "orders_store"}, "input_var": "user_order_links", "op": "join", "result_var": "joined_orders"},
		{"args": map[string]any{"transaction": "tx"}, "op": "commit_tx"},
		{"input_var": "joined_orders", "op": "return"},
	}

	result3 := executeScriptHelper(t, agent, ctx, script3)
	var stage3Data []map[string]any
	json.Unmarshal([]byte(result3), &stage3Data)
	t.Logf("STAGE 3 (after 2nd join): %d records", len(stage3Data))
	if len(stage3Data) < len(stage2Data) {
		t.Errorf("❌ LOST %d records in second join!", len(stage2Data)-len(stage3Data))
	}

	// Stage 4: After filter
	script4 := []map[string]any{
		{"args": map[string]any{"mode": "read"}, "op": "begin_tx", "result_var": "tx"},
		{"args": map[string]any{"name": "users", "transaction": "tx"}, "op": "open_store", "result_var": "users_store"},
		{"args": map[string]any{"name": "users_orders", "transaction": "tx"}, "op": "open_store", "result_var": "users_orders_store"},
		{"args": map[string]any{"name": "orders", "transaction": "tx"}, "op": "open_store", "result_var": "orders_store"},
		{"args": map[string]any{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}, "store": "users_store"}, "op": "select", "result_var": "matched_users"},
		{"args": map[string]any{"on": map[string]any{"key": "key"}, "store": "users_orders_store"}, "input_var": "matched_users", "op": "join", "result_var": "user_order_links"},
		{"args": map[string]any{"on": map[string]any{"users_orders.value": "key"}, "store": "orders_store"}, "input_var": "user_order_links", "op": "join", "result_var": "joined_orders"},
		{"args": map[string]any{"condition": map[string]any{"orders.total_amount": map[string]any{"$gt": 500}}}, "input_var": "joined_orders", "op": "filter", "result_var": "filtered_orders"},
		{"args": map[string]any{"transaction": "tx"}, "op": "commit_tx"},
		{"input_var": "filtered_orders", "op": "return"},
	}

	result4 := executeScriptHelper(t, agent, ctx, script4)
	var stage4Data []map[string]any
	json.Unmarshal([]byte(result4), &stage4Data)
	t.Logf("STAGE 4 (after filter): %d records", len(stage4Data))

	if len(stage4Data) != 10 {
		t.Errorf("FINAL RESULT: Expected 10 records, got %d", len(stage4Data))
	}
}

func executeScriptHelper(t *testing.T, agent *CopilotAgent, ctx context.Context, script []map[string]any) string {
	scriptBytes, _ := json.Marshal(script)
	ctxWithPayload := context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "test_db"})
	result, err := agent.toolExecuteScript(ctxWithPayload, map[string]any{"script": string(scriptBytes)})
	if err != nil {
		t.Fatalf("toolExecuteScript failed: %v", err)
	}
	return result
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
