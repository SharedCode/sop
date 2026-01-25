package agent

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

func TestMultiDBScriptExecution(t *testing.T) {
	// 1. Setup Databases
	ctx := context.Background()
	dbPath1 := "test_multidb_1"
	dbPath2 := "test_multidb_2"
	os.RemoveAll(dbPath1)
	os.RemoveAll(dbPath2)
	defer os.RemoveAll(dbPath1)
	defer os.RemoveAll(dbPath2)

	dbOpts1 := sop.DatabaseOptions{StoresFolders: []string{dbPath1}, CacheType: sop.InMemory}
	dbOpts2 := sop.DatabaseOptions{StoresFolders: []string{dbPath2}, CacheType: sop.InMemory}

	// Create Store in DB1
	{
		db := database.NewDatabase(dbOpts1)
		tx, _ := db.BeginTransaction(ctx, sop.ForWriting)
		sopdb.NewBtree[string, any](ctx, dbOpts1, "store1", tx, nil, sop.StoreOptions{Name: "store1", SlotLength: 10})
		tx.Commit(ctx)

		tx, _ = db.BeginTransaction(ctx, sop.ForWriting)
		s, _ := jsondb.OpenStore(ctx, dbOpts1, "store1", tx)
		s.Add(ctx, "key1", "val1")
		tx.Commit(ctx)
	}

	// Create Store in DB2
	{
		db := database.NewDatabase(dbOpts2)
		tx, _ := db.BeginTransaction(ctx, sop.ForWriting)
		sopdb.NewBtree[string, any](ctx, dbOpts2, "store2", tx, nil, sop.StoreOptions{Name: "store2", SlotLength: 10})
		tx.Commit(ctx)

		tx, _ = db.BeginTransaction(ctx, sop.ForWriting)
		s, _ := jsondb.OpenStore(ctx, dbOpts2, "store2", tx)
		s.Add(ctx, "key2", "val2")
		tx.Commit(ctx)
	}

	// 2. Setup Agent
	systemDBPath := "test_multidb_system"
	os.RemoveAll(systemDBPath)
	defer os.RemoveAll(systemDBPath)
	systemDBOpts := sop.DatabaseOptions{StoresFolders: []string{systemDBPath}}
	systemDB := database.NewDatabase(systemDBOpts)

	daAgent := NewDataAdminAgent(Config{}, map[string]sop.DatabaseOptions{
		"db1": dbOpts1,
		"db2": dbOpts2,
	}, systemDB)

	ctx = context.WithValue(context.Background(), "session_payload", &ai.SessionPayload{CurrentDB: "db1"})
	daAgent.Open(ctx)

	// 3. Define Script
	script := ai.Script{
		Name: "multidb_script",
		Steps: []ai.ScriptStep{
			{
				Type:    "command",
				Command: "select",
				Args: map[string]any{
					"database": "db1",
					"store":    "store1",
				},
			},
			{
				Type:    "command",
				Command: "select",
				Args: map[string]any{
					"database": "db2",
					"store":    "store2",
				},
			},
		},
	}

	// Save Script
	{
		tx, _ := systemDB.BeginTransaction(ctx, sop.ForWriting)
		store, _ := systemDB.OpenModelStore(ctx, "scripts", tx)
		store.Save(ctx, "general", script.Name, &script)
		tx.Commit(ctx)
	}

	// 4. Execute Script via ExecuteTool (simulating LLM call)
	// We need to simulate a session where a transaction is active on "db1" (default)

	// Case A: No active transaction in payload (Auto-commit mode)
	// Each step should start its own transaction on the correct DB.
	t.Run("AutoCommit", func(t *testing.T) {
		payload := &ai.SessionPayload{
			CurrentDB: "db1",
		}
		ctxWithPayload := context.WithValue(ctx, "session_payload", payload)

		// We call ExecuteTool directly for the script
		// But wait, ExecuteTool for a script calls runScript, which calls ExecuteTool for steps.
		res, err := daAgent.Execute(ctxWithPayload, "multidb_script", map[string]any{})
		if err != nil {
			t.Fatalf("Script execution failed: %v", err)
		}
		t.Logf("Result: %s", res)
	})

	// Case B: Active transaction on DB1
	t.Run("ActiveTransaction_DB1", func(t *testing.T) {
		db1 := database.NewDatabase(dbOpts1)
		tx, _ := db1.BeginTransaction(ctx, sop.ForReading)
		defer tx.Rollback(ctx)

		payload := &ai.SessionPayload{
			CurrentDB:   "db1",
			Transaction: tx,
		}
		ctxWithPayload := context.WithValue(ctx, "session_payload", payload)

		// This should succeed now that we handle cross-db transactions
		res, err := daAgent.Execute(ctxWithPayload, "multidb_script", map[string]any{})
		if err != nil {
			t.Errorf("Expected success, but got error: %v", err)
		} else {
			t.Logf("Result: %s", res)
		}
	})
}
