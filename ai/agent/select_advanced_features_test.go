package agent

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/jsondb"
)

// TestOrderedMap_MarshalJSON verifies that OrderedMap preserves insertion order in JSON output.
func TestOrderedMap_MarshalJSON(t *testing.T) {
	om := OrderedMap{
		m:    make(map[string]any),
		keys: make([]string, 0),
	}

	// Insert in specific order (not alphabetical)
	fields := []string{"zebra", "apple", "middle", "zoo"}
	for _, f := range fields {
		om.keys = append(om.keys, f)
		om.m[f] = "value_" + f
	}

	b, err := json.Marshal(om)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	jsonStr := string(b)

	// Verify order by checking index of keys in the string
	lastIdx := -1
	for _, f := range fields {
		idx := strings.Index(jsonStr, "\""+f+"\"")
		if idx == -1 {
			t.Errorf("Key %s not found in JSON", f)
		}
		if idx < lastIdx {
			t.Errorf("Key %s appeared out of order. Expected after previous key. JSON: %s", f, jsonStr)
		}
		lastIdx = idx
	}
}

// TestFilterFields_LateBinding verifies that filterFields works for both Key/Value wrappers and flat JSON.
func TestFilterFields_LateBinding(t *testing.T) {
	// Case 1: Standard Key/Value wrapper
	itemKV := map[string]any{
		"key":   map[string]any{"id": 1, "name": "Alice"},
		"value": map[string]any{"role": "admin", "salary": 1000},
	}
	fieldsKV := []string{"name", "role"}
	resKV := filterFields(itemKV, fieldsKV)

	// Verify structure is preserved but filtered
	resMapKV, ok := resKV.(map[string]any)
	if !ok {
		t.Fatalf("Expected map result for KV input")
	}
	if _, ok := resMapKV["key"]; !ok {
		t.Error("Expected 'key' wrapper in result")
	}
	if _, ok := resMapKV["value"]; !ok {
		t.Error("Expected 'value' wrapper in result")
	}

	// Case 2: Flat JSON (e.g. from Macro/Join)
	itemFlat := map[string]any{
		"id":     1,
		"name":   "Alice",
		"role":   "admin",
		"salary": 1000,
	}
	fieldsFlat := []string{"name", "salary"}
	resFlat := filterFields(itemFlat, fieldsFlat)

	// Verify it returns an OrderedMap (or map) with just those fields
	// Since filterFields returns OrderedMap for flat input now:
	om, ok := resFlat.(OrderedMap)
	if !ok {
		t.Fatalf("Expected OrderedMap result for flat input, got %T", resFlat)
	}
	if len(om.keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(om.keys))
	}
	if om.keys[0] != "name" || om.keys[1] != "salary" {
		t.Errorf("Expected keys [name, salary], got %v", om.keys)
	}
}

// TestToolSelect_ValueMatch verifies filtering by value fields.
func TestToolSelect_ValueMatch(t *testing.T) {
	ctx := context.Background()
	dbPath := "test_dataadmin_select_valuematch"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.InMemory,
	}

	// Setup DB
	db := database.NewDatabase(dbOpts)
	tx, _ := db.BeginTransaction(ctx, sop.ForWriting)

	storeName := "employees"
	sopdb.NewBtree[string, any](ctx, dbOpts, storeName, tx, nil, sop.StoreOptions{Name: storeName, SlotLength: 10, IsPrimitiveKey: true})
	tx.Commit(ctx)

	tx, _ = db.BeginTransaction(ctx, sop.ForWriting)
	store, _ := jsondb.OpenStore(ctx, dbOpts, storeName, tx)

	// Add items: Key=ID, Value={region, department, name}
	store.Add(ctx, "1", map[string]any{"region": "APAC", "department": "HR", "name": "Alice"})
	store.Add(ctx, "2", map[string]any{"region": "EMEA", "department": "HR", "name": "Bob"})
	store.Add(ctx, "3", map[string]any{"region": "APAC", "department": "IT", "name": "Charlie"})
	store.Add(ctx, "4", map[string]any{"region": "APAC", "department": "HR", "name": "Dave"})

	tx.Commit(ctx)

	agent := &DataAdminAgent{
		databases: map[string]sop.DatabaseOptions{"testdb": dbOpts},
	}
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "testdb"})

	// Test: Select where region='APAC' and department='HR'
	args := map[string]any{
		"store": "employees",
		"value_match": map[string]any{
			"region":     "APAC",
			"department": "HR",
		},
	}

	resJSON, err := agent.toolSelect(ctx, args)
	if err != nil {
		t.Fatalf("toolSelect failed: %v", err)
	}

	var results []map[string]any
	if err := json.Unmarshal([]byte(resJSON), &results); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 results (Alice, Dave), got %d", len(results))
	}

	// Verify names
	names := make(map[string]bool)
	for _, r := range results {
		val := r["value"].(map[string]any)
		names[val["name"].(string)] = true
	}
	if !names["Alice"] || !names["Dave"] {
		t.Errorf("Expected Alice and Dave, got %v", names)
	}
}

// TestToolSelect_MacroView verifies using a Macro as a data source.
func TestToolSelect_MacroView(t *testing.T) {
	ctx := context.Background()
	dbPath := "test_dataadmin_select_macro"
	os.RemoveAll(dbPath)
	defer os.RemoveAll(dbPath)

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
		CacheType:     sop.InMemory,
	}

	// 1. Setup System DB (for Macros)
	systemDB := database.NewDatabase(dbOpts)
	// No need to manually create "macros" store, model.New/Save will handle it with correct types.

	// 2. Setup User DB (for Data)
	userDB := database.NewDatabase(dbOpts)
	tx, _ := userDB.BeginTransaction(ctx, sop.ForWriting)
	sopdb.NewBtree[string, any](ctx, dbOpts, "users", tx, nil, sop.StoreOptions{Name: "users", SlotLength: 10, IsPrimitiveKey: true})
	tx.Commit(ctx)

	// Populate User Data
	tx, _ = userDB.BeginTransaction(ctx, sop.ForWriting)
	userStore, _ := jsondb.OpenStore(ctx, dbOpts, "users", tx)
	userStore.Add(ctx, "u1", map[string]any{"name": "Alice", "active": true})
	userStore.Add(ctx, "u2", map[string]any{"name": "Bob", "active": false})
	userStore.Add(ctx, "u3", map[string]any{"name": "Charlie", "active": true})
	tx.Commit(ctx)

	// 3. Create a Macro "active_users"
	tx, _ = systemDB.BeginTransaction(ctx, sop.ForWriting)
	macroStore, _ := systemDB.OpenModelStore(ctx, "macros", tx)

	macro := ai.Macro{
		Name: "active_users",
		Steps: []ai.MacroStep{
			{
				Type:    "command",
				Command: "select",
				Args: map[string]any{
					"store":       "users",
					"value_match": map[string]any{"active": true},
				},
				Database: "testdb", // Run against user DB
			},
		},
	}
	if err := macroStore.Save(ctx, "general", "active_users", macro); err != nil {
		t.Fatalf("Failed to save macro: %v", err)
	}
	tx.Commit(ctx)

	// 4. Run Select against Macro
	agent := &DataAdminAgent{
		databases: map[string]sop.DatabaseOptions{"testdb": dbOpts},
		systemDB:  systemDB,
	}
	agent.registerTools()
	ctx = context.WithValue(ctx, "session_payload", &ai.SessionPayload{CurrentDB: "testdb"})

	// Query: Select name from 'active_users'
	args := map[string]any{
		"store":  "active_users",
		"fields": []string{"name"},
	}

	resJSON, err := agent.toolSelect(ctx, args)
	if err != nil {
		t.Fatalf("toolSelect failed: %v", err)
	}

	// The result from macro select (which calls select internally) will be wrapped in Key/Value
	// But our filterFields for flat JSON (if macro returned flat) would handle it.
	// However, the macro "select" returns [{"key":..., "value":...}, ...].
	// So the "View" sees items with Key/Value.
	// If we request "name", and "name" is in Value, filterFields needs to find it.
	// Wait, filterFields logic for Key/Value wrapper:
	// It looks for "name" in Key OR Value.
	// Let's check filterFields implementation again.
	// It checks if requested field is in Key map, then in Value map.
	// So `select name from active_users` should work if `name` is in the value map of the macro result.

	var results []any
	if err := json.Unmarshal([]byte(resJSON), &results); err != nil {
		t.Fatalf("Failed to parse result: %v", err)
	}

	if len(results) != 2 {
		t.Errorf("Expected 2 active users, got %d", len(results))
	}

	// Verify content
	// Since we requested "name", and "name" is in Value, the result should be:
	// {"key": nil, "value": {"name": "Alice"}} (OrderedMap)
	// Or if filterFields flattens it? No, filterFields preserves Key/Value structure if input has it.

	// Let's inspect first result
	r1 := results[0].(map[string]any)
	val := r1["value"].(map[string]any)
	if val["name"] != "Alice" && val["name"] != "Charlie" {
		t.Errorf("Unexpected name: %v", val["name"])
	}
}
