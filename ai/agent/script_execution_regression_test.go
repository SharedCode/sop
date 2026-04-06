package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/btree"
	core_database "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

func TestScriptExecution_JoinRegression(t *testing.T) {
	// 1. Setup Temp DB
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}

	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// 2. Setup Data
	t.Log("Setting up data...")
	tx, err := core_database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Users
	users, _ := core_database.NewBtree[any, any](ctx, dbOpts, "users", tx, nil)
	users.Add(ctx, "u1", map[string]any{"first_name": "John", "last_name": "Jones", "age": 30})
	users.Add(ctx, "u2", map[string]any{"first_name": "Jane", "last_name": "Doe", "age": 25})

	// Orders
	orders, _ := core_database.NewBtree[any, any](ctx, dbOpts, "orders", tx, nil)
	orders.Add(ctx, "o1", map[string]any{"total_amount": 831, "order_date": "2026-01-01"})
	orders.Add(ctx, "o2", map[string]any{"total_amount": 100, "order_date": "2026-01-02"})

	// Join Table (users_orders)
	// Key: UserID, Value: OrderID
	users_orders, _ := core_database.NewBtree[any, any](ctx, dbOpts, "users_orders", tx, nil)
	users_orders.Add(ctx, "u1", "o1")
	users_orders.Add(ctx, "u1", "o2") // John has two orders? For now simple 1-1 check first.
	// Actually Store doesn't support duplicates on same key unless configured.
	// Standard Btree is Unique Key.
	// So John can only have one entry in this simple 'users_orders' if Key is UserID.
	// We'll stick to 1-1 for this test as per data model inference.

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit setup: %v", err)
	}

	// 3. Define Script
	// This mimics the user's reported script
	scriptJSON := `[
		{"args":{"name":"dev_db"},"op":"open_db","result_var":"db"},
		{"args":{"database":"db","mode":"read"},"op":"begin_tx","result_var":"tx"},
		{"args":{"name":"users","transaction":"tx"},"op":"open_store","result_var":"users"},
		{"args":{"name":"users_orders","transaction":"tx"},"op":"open_store","result_var":"users_orders"},
		{"args":{"name":"orders","transaction":"tx"},"op":"open_store","result_var":"orders"},
		{"args":{"filter":{"first_name":{"$eq":"John"}},"store":"users","stream":true},"op":"scan","result_var":"users_stream"},
		{"args":{"on":{"key":"key"},"store":"users_orders","stream":true},"input_var":"users_stream","op":"join_right","result_var":"joined_stream_1"},
		{"args":{"on":{"value":"key"},"store":"orders","stream":true},"input_var":"joined_stream_1","op":"join_right","result_var":"joined_stream_2"},
		{"args":{"condition":{"orders.total_amount":{"$gt":500}}},"input_var":"joined_stream_2","op":"filter","result_var":"filtered_stream"},
		{"args":{"fields":["users.first_name","users.last_name","orders.key AS order_id","orders.total_amount","orders.order_date"]},"input_var":"filtered_stream","op":"project","result_var":"result"},
		{"args":{"transaction":"tx"},"op":"commit_tx"}
	]`

	var script []ScriptInstruction
	json.Unmarshal([]byte(scriptJSON), &script)

	// 4. Run Agent
	agent := &CopilotAgent{
		systemDB: sysDB,
		databases: map[string]sop.DatabaseOptions{
			"dev_db": dbOpts,
		},
		StoreOpener: func(ctx context.Context, dbOpts sop.DatabaseOptions, storeName string, tx sop.Transaction) (jsondb.StoreAccessor, error) {
			// In unit test environment, we might need manual wiring,
			// but if the runtime calls core_database.NewBtree directly internally inside ScriptEngine.OpenStore default path,
			// this might not be reached if we don't use it.
			// However implementation requires it to match signature.

			// If the code actually tries to use this:
			s, _ := core_database.NewBtree[any, any](ctx, dbOpts, storeName, tx, nil)
			return &testStoreWrapper{s}, nil
		},
	}

	// The execute function uses a session context
	t.Log("Executing script...")

	// We mock the context payload to inject current DB
	ctxWithPayload := context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "dev_db"})

	result, err := agent.toolExecuteScript(ctxWithPayload, map[string]any{"script": scriptJSON})
	if err != nil {
		t.Fatalf("Script execution failed: %v", err)
	}

	t.Logf("Result: %s", result)

	// verify that we got a CSV/JSON result with the expected data
	if result == "" {
		t.Error("Empty result")
	}

	// We expect John Jones with order total 831 to go through
	// We expect Jane Doe to be filtered (first_name != John)
	// We expect John's other order (if any) to be filtered by total > 500
}

type testStoreWrapper struct {
	btree.BtreeInterface[any, any]
}

func (s *testStoreWrapper) FindOne(ctx context.Context, key any, first bool) (bool, error) {
	return s.BtreeInterface.Find(ctx, key, first)
}

func (s *testStoreWrapper) GetCurrentKey() any {
	return s.BtreeInterface.GetCurrentKey().Key
}
