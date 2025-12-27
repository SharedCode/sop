package agent

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/jsondb"
)

func TestToolSelect_OrderedOutput(t *testing.T) {
	// 1. Setup
	ctx := context.Background()
	dbPath := "test_dataadmin_select_ordered"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.InMemory,
	}

	// Create DB and Store
	db := database.NewDatabase(dbOpts)
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	storeName := "users_ordered"

	// Define Index Spec: "role" then "group" then "id"
	// Note: Alphabetical is group, id, role.
	// We want role first.
	idxSpec := jsondb.NewIndexSpecification([]jsondb.IndexFieldSpecification{
		{FieldName: "role", AscendingSortOrder: true},
		{FieldName: "group", AscendingSortOrder: true},
		{FieldName: "id", AscendingSortOrder: true},
	})
	idxSpecBytes, _ := encoding.DefaultMarshaler.Marshal(idxSpec)

	storeOpts := sop.StoreOptions{
		Name:                     storeName,
		SlotLength:               10,
		IsPrimitiveKey:           false,
		MapKeyIndexSpecification: string(idxSpecBytes),
	}

	if _, err := sopdb.NewBtree[string, any](ctx, dbOpts, storeName, tx, nil, storeOpts); err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit creation failed: %v", err)
	}

	// Start new transaction for population
	tx, err = db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction population failed: %v", err)
	}

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

	// Check order in JSON string
	// Expected order: role, group, id
	// "role": "admin" ... "group": "A" ... "id": 1
	// Note: json.MarshalIndent adds spaces after colons.

	roleIdx := strings.Index(result, "\"role\": \"admin\"")
	groupIdx := strings.Index(result, "\"group\": \"A\"")
	idIdx := strings.Index(result, "\"id\": 1")

	if roleIdx == -1 || groupIdx == -1 || idIdx == -1 {
		// Fallback to check without spaces if indentation changes
		roleIdx = strings.Index(result, "\"role\":\"admin\"")
		groupIdx = strings.Index(result, "\"group\":\"A\"")
		idIdx = strings.Index(result, "\"id\":1")
	}

	if roleIdx == -1 || groupIdx == -1 || idIdx == -1 {
		t.Fatalf("Missing fields in result: %s", result)
	}

	if roleIdx > groupIdx {
		t.Errorf("Expected role before group, got role at %d, group at %d", roleIdx, groupIdx)
	}
	if groupIdx > idIdx {
		t.Errorf("Expected group before id, got group at %d, id at %d", groupIdx, idIdx)
	}

	// Test case 2: Partial match
	// Index: role, group, id
	// Item: id=2, role=user (missing group)
	// Expected: role, id (group skipped)

	// We need a new transaction to add more items
	tx, err = db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction 2 failed: %v", err)
	}
	store, err = jsondb.OpenStore(ctx, dbOpts, storeName, tx)
	if err != nil {
		t.Fatalf("OpenStore 2 failed: %v", err)
	}

	itemKey2 := map[string]any{"id": 2, "role": "user"}
	if _, err := store.Add(ctx, itemKey2, "Bob"); err != nil {
		t.Fatalf("Add 2 failed: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit 2 failed: %v", err)
	}

	// Select again
	result, err = agent.toolSelect(ctx, args)
	if err != nil {
		t.Fatalf("toolSelect 2 failed: %v", err)
	}

	// Check Bob's entry
	roleUserIdx := strings.Index(result, "\"role\": \"user\"")
	id2Idx := strings.Index(result, "\"id\": 2")

	if roleUserIdx == -1 {
		roleUserIdx = strings.Index(result, "\"role\":\"user\"")
	}
	if id2Idx == -1 {
		id2Idx = strings.Index(result, "\"id\":2")
	}

	if roleUserIdx == -1 || id2Idx == -1 {
		t.Fatalf("Missing fields for Bob: %s", result)
	}

	if roleUserIdx > id2Idx {
		t.Errorf("Expected role before id for Bob, got role at %d, id at %d", roleUserIdx, id2Idx)
	}
}
