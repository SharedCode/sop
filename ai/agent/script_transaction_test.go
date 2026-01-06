package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	core_database "github.com/sharedcode/sop/database"
)

func TestScript_Transactions(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		// Use NoCache to ensure data is flushed to disk and visible across DB instances
		CacheType: sop.NoCache,
	}
	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// Create a user database
	userDBName := "userdb"
	userDB := database.NewDatabase(dbOpts)

	// Initialize "employees" store in userDB using core_database.NewBtree
	tx, _ := userDB.BeginTransaction(ctx, sop.ForWriting)
	storeOpts := sop.StoreOptions{
		IsPrimitiveKey: true,
	}
	_, err := core_database.NewBtree[string, any](ctx, dbOpts, "employees", tx, nil, storeOpts)
	if err != nil {
		t.Fatalf("Failed to create employees store: %v", err)
	}
	tx.Commit(ctx)

	databases := map[string]sop.DatabaseOptions{
		userDBName: dbOpts,
	}

	// 2. Define Scripts

	// Script 1: Implicit Transaction (Auto-commit)
	// Should succeed and persist data.
	scriptImplicit := ai.Script{
		Name:     "implicit_tx",
		Database: userDBName,
		Steps: []ai.ScriptStep{
			{
				Type:    "command",
				Command: "add",
				Args: map[string]any{
					"store": "employees",
					"key":   "emp_implicit",
					"value": "Implicit Data",
				},
			},
		},
	}

	// Script 2: Explicit Transaction (Commit)
	// Should succeed and persist data.
	scriptExplicitCommit := ai.Script{
		Name:     "explicit_commit",
		Database: userDBName,
		Steps: []ai.ScriptStep{
			{
				Type:    "command",
				Command: "manage_transaction",
				Args:    map[string]any{"action": "begin"},
			},
			{
				Type:    "command",
				Command: "add",
				Args: map[string]any{
					"store": "employees",
					"key":   "emp_explicit_commit",
					"value": "Explicit Commit Data",
				},
			},
			{
				Type:    "command",
				Command: "manage_transaction",
				Args:    map[string]any{"action": "commit"},
			},
		},
	}

	// Script 3: Explicit Transaction (Rollback)
	// Should NOT persist data.
	scriptExplicitRollback := ai.Script{
		Name:     "explicit_rollback",
		Database: userDBName,
		Steps: []ai.ScriptStep{
			{
				Type:    "command",
				Command: "manage_transaction",
				Args:    map[string]any{"action": "begin"},
			},
			{
				Type:    "command",
				Command: "add",
				Args: map[string]any{
					"store": "employees",
					"key":   "emp_explicit_rollback",
					"value": "Explicit Rollback Data",
				},
			},
			{
				Type:    "command",
				Command: "manage_transaction",
				Args:    map[string]any{"action": "rollback"},
			},
		},
	}

	// Script 4: Uncommitted Explicit Transaction (Safety Rollback)
	// Should NOT persist data because the session closer should rollback uncommitted explicit txs.
	scriptUncommitted := ai.Script{
		Name:     "uncommitted",
		Database: userDBName,
		Steps: []ai.ScriptStep{
			{
				Type:    "command",
				Command: "manage_transaction",
				Args:    map[string]any{"action": "begin"},
			},
			{
				Type:    "command",
				Command: "add",
				Args: map[string]any{
					"store": "employees",
					"key":   "emp_uncommitted",
					"value": "Uncommitted Data",
				},
			},
		},
	}

	// Save Scripts
	tx, _ = sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "scripts", tx)
	store.Save(ctx, "general", "implicit_tx", scriptImplicit)
	store.Save(ctx, "general", "explicit_commit", scriptExplicitCommit)
	store.Save(ctx, "general", "explicit_rollback", scriptExplicitRollback)
	store.Save(ctx, "general", "uncommitted", scriptUncommitted)
	tx.Commit(ctx)

	// 3. Initialize Service
	// We need a DataAdminAgent in the registry to handle the commands
	registry := make(map[string]ai.Agent[map[string]any])
	cfg := Config{
		ID:   "data_admin",
		Name: "Data Admin",
	}
	dataAdmin := NewDataAdminAgent(cfg, databases, sysDB)
	registry["data_admin"] = dataAdmin

	svc := NewService(nil, sysDB, databases, nil, nil, registry, false)

	// Helper to check if key exists
	checkKey := func(key string, shouldExist bool) {
		tx, err := userDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			t.Fatalf("CheckKey: BeginTransaction failed: %v", err)
		}
		// Use the same userDB object to open the store, to ensure consistency
		store, err := userDB.OpenBtreeCursor(ctx, "employees", tx)
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("CheckKey: OpenBtreeCursor failed: %v", err)
		}

		found, err := store.Find(ctx, key, false)
		if err != nil {
			tx.Rollback(ctx)
			t.Fatalf("CheckKey: Find failed: %v", err)
		}

		tx.Commit(ctx)
		if found != shouldExist {
			t.Errorf("Key '%s' existence mismatch. Expected: %v, Got: %v", key, shouldExist, found)
		}
	}

	// Test 1: Implicit
	if _, err := svc.Ask(ctx, "/play implicit_tx"); err != nil {
		t.Fatalf("Implicit script failed: %v", err)
	}
	checkKey("emp_implicit", true)

	// Test 2: Explicit Commit
	if _, err := svc.Ask(ctx, "/play explicit_commit"); err != nil {
		t.Fatalf("Explicit Commit script failed: %v", err)
	}
	checkKey("emp_explicit_commit", true)

	// Test 3: Explicit Rollback
	if _, err := svc.Ask(ctx, "/play explicit_rollback"); err != nil {
		t.Fatalf("Explicit Rollback script failed: %v", err)
	}
	checkKey("emp_explicit_rollback", false)

	// Test 4: Uncommitted (Safety Rollback)
	if _, err := svc.Ask(ctx, "/play uncommitted"); err != nil {
		t.Fatalf("Uncommitted script failed: %v", err)
	}
	checkKey("emp_uncommitted", false)
}
