package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func TestService_ExecuteScript_StringDB(t *testing.T) {
	// 1. Setup System DB
	tmpDir := t.TempDir()
	sysDB := database.NewDatabase(sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{tmpDir},
	})

	// 2. Seed Script
	ctx := context.Background()
	tx, err := sysDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	store, err := sysDB.OpenModelStore(ctx, "scripts", tx)
	if err != nil {
		t.Fatalf("Failed to open model store: %v", err)
	}
	script := ai.Script{
		Name:  "test_script",
		Steps: []ai.ScriptStep{{Type: "say", Message: "Hello"}},
	}
	if err := store.Save(ctx, "general", "test_script", script); err != nil {
		t.Fatalf("Failed to save script: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// 3. Setup Service
	dbs := make(map[string]sop.DatabaseOptions)
	dbs["test_db"] = sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{tmpDir}, // Reuse same dir for simplicity
	}

	svc := NewService(&MockDomain{}, sysDB, dbs, &MockGenerator{}, nil, nil, false)

	// 4. Call Ask with /play and String DB in payload
	payload := &ai.SessionPayload{
		CurrentDB: "test_db", // String!
	}

	ctx = context.WithValue(ctx, "session_payload", payload)

	// This should NOT panic
	resp, err := svc.Ask(ctx, "/play test_script")
	if err != nil {
		t.Fatalf("Ask failed: %v", err)
	}

	// The response is now a JSON array of steps/results
	if !strings.Contains(resp, "\"type\": \"say\"") {
		t.Errorf("Expected response to contain '\"type\": \"say\"', got: %s", resp)
	}
	if !strings.Contains(resp, "\"result\": \"Hello\"") {
		t.Errorf("Expected response to contain '\"result\": \"Hello\"', got: %s", resp)
	}
}
