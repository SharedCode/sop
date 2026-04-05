package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/stretchr/testify/assert"
)

func TestRealDBIntegration_JoinFlow(t *testing.T) {
	tempDir := t.TempDir()
	// Configure "dev_db"
	ec := sop.ErasureCodingConfig{
		DataShardsCount:             1,
		ParityShardsCount:           1,
		BaseFolderPathsAcrossDrives: []string{tempDir + "/db1", tempDir + "/db2"},
	}

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{tempDir + "/db"},
		Type:          sop.Clustered,
		RedisConfig:   nil,
		ErasureConfig: map[string]sop.ErasureCodingConfig{"": ec},
	}
	databases := map[string]sop.DatabaseOptions{
		"dev_db": dbOpts,
	}

	cfg := Config{}
	agent := NewDataAdminAgent(cfg, databases, nil)

	ctx := context.Background()
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{
		Variables: make(map[string]any),
		CurrentDB: "dev_db",
	})
	ctx = context.WithValue(ctx, "verbose", true)

	// Script
	scriptRaw := `[
		{"op": "open_db", "args": {"name": "dev_db"}},
{"op": "begin_tx", "args": {"database": "dev_db", "mode": "write"}, "result_var": "tx"},

		{"op": "open_store", "args": {"name": "users", "transaction": "tx", "create": true}, "result_var": "users"},
		{"op": "open_store", "args": {"name": "users_orders", "transaction": "tx", "create": true}, "result_var": "users_orders"},
		{"op": "open_store", "args": {"name": "orders", "transaction": "tx", "create": true}, "result_var": "orders"},

		{"op": "scan", "args": {"store": "users", "filter": {"first_name": "John"}}, "result_var": "a"},

		{"op": "join", "input_var": "a", "args": {"store": "users_orders", "alias": "b", "on": {"key": "key"}}, "result_var": "b_joined"},

		{"op": "join", "input_var": "b_joined", "args": {"store": "orders", "alias": "c", "on": {"value": "key"}}, "result_var": "final"},

		{"op": "project", "input_var": "final", "args": {"fields": [{"Dst": "Customer", "Src": "a.first_name"}, {"Dst": "Order", "Src": "c.key"}, {"Dst": "Total", "Src": "c.total_amount"}]}, "result_var": "view"},
		
		{"op": "return", "args": {"value": "view"}}
	]`

	resStr, err := agent.toolExecuteScript(ctx, map[string]any{"script": scriptRaw})
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	fmt.Printf("Query Result:\n%s\n", resStr)

	var results []any
	err = json.Unmarshal([]byte(resStr), &results)
	assert.NoError(t, err)
	t.Logf("Found %d records", len(results))
}
