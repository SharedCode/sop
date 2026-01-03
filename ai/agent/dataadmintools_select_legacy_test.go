package agent

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/jsondb"
)

func TestToolSelect_LegacyOrderedOutput(t *testing.T) {
	// 1. Setup
	ctx := context.Background()
	dbPath := "test_dataadmin_select_legacy"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.NoCache,
	}

	// Create DB and Store
	db := database.NewDatabase(dbOpts)
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	storeName := "users_legacy"

	// Define Index Spec: "role" then "group" then "id"
	idxSpec := jsondb.NewIndexSpecification([]jsondb.IndexFieldSpecification{
		{FieldName: "role", AscendingSortOrder: true},
		{FieldName: "group", AscendingSortOrder: true},
		{FieldName: "id", AscendingSortOrder: true},
	})
	idxSpecBytes, _ := encoding.DefaultMarshaler.Marshal(idxSpec)

	// Simulate legacy store by putting spec in CELexpression and leaving MapKeyIndexSpecification empty
	storeOpts := sop.StoreOptions{
		Name:                     storeName,
		SlotLength:               10,
		IsPrimitiveKey:           false,
		CELexpression:            string(idxSpecBytes), // Legacy field
		MapKeyIndexSpecification: "",                   // Empty
		CacheConfig: &sop.StoreCacheConfig{
			StoreInfoCacheDuration: 1 * time.Nanosecond, // Force expire immediately
		},
	}

	// Note: We use NewBtree directly to bypass some of the jsondb helpers that might auto-populate MapKeyIndexSpecification
	if _, err := sopdb.NewBtree[string, any](ctx, dbOpts, storeName, tx, nil, storeOpts); err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit creation failed: %v", err)
	}

	// TAMPERING: Manually modify storeinfo.txt to simulate legacy state
	// (Empty MapKeyIndexSpecification, populated LegacyCELexpression)
	storeInfoPath := dbPath + "/users_legacy/storeinfo.txt"
	infoBytes, err := os.ReadFile(storeInfoPath)
	if err != nil {
		t.Fatalf("Failed to read storeinfo: %v", err)
	}

	// We need to manipulate the JSON directly because unmarshaling into StoreInfo might normalize it.
	// Or we can unmarshal to map[string]any.
	var infoMap map[string]any
	if err := encoding.DefaultMarshaler.Unmarshal(infoBytes, &infoMap); err != nil {
		t.Fatalf("Failed to unmarshal storeinfo: %v", err)
	}

	// Move mapkey_index_spec to cel_expression if needed, or just ensure state.
	if spec, ok := infoMap["mapkey_index_spec"]; ok {
		infoMap["cel_expression"] = spec
		infoMap["mapkey_index_spec"] = ""
	}

	newInfoBytes, _ := encoding.DefaultMarshaler.Marshal(infoMap)
	if err := os.WriteFile(storeInfoPath, newInfoBytes, 0644); err != nil {
		t.Fatalf("Failed to write tampered storeinfo: %v", err)
	}

	// Start new transaction for population
	tx, err = db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction population failed: %v", err)
	}

	// We must use OpenJsonBtreeMapKey to ensure it reads the legacy spec correctly?
	// Or just OpenStore.
	store, err := jsondb.OpenStore(ctx, dbOpts, storeName, tx)
	if err != nil {
		t.Fatalf("OpenStore failed: %v", err)
	}

	// Add item
	itemKey := map[string]any{"id": 1, "group": "A", "role": "admin"}
	if _, err := store.Add(ctx, itemKey, "Alice"); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit failed: %v", err)
	}

	// 2. Prepare Agent
	agent := &DataAdminAgent{
		databases: map[string]sop.DatabaseOptions{
			"testdb": dbOpts,
		},
	}
	sessionPayload := &ai.SessionPayload{
		CurrentDB: "testdb",
	}
	ctx = context.WithValue(ctx, "session_payload", sessionPayload)

	// 3. Test
	args := map[string]any{
		"store": storeName,
	}

	result, err := agent.toolSelect(ctx, args)
	if err != nil {
		t.Fatalf("toolSelect failed: %v", err)
	}

	// 4. Verify Order
	// Expected: Alphabetical order (default for maps)
	// JSON: {"group":"A","id":1,"role":"admin"}

	// Check if "group" appears before "role"
	roleIdx := strings.Index(result, "\"role\"")
	groupIdx := strings.Index(result, "\"group\"")

	if roleIdx == -1 || groupIdx == -1 {
		t.Fatalf("Missing keys in result: %s", result)
	}

	if groupIdx > roleIdx {
		t.Errorf("Expected 'group' before 'role' (alphabetical), but got: %s", result)
	}
}
