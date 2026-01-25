package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/jsondb"
)

func TestScanUsesCanonicalStoreNameForPrefix(t *testing.T) {
	// 1. Setup Mock Store with reliable "Name"
	// The store Name is "users"
	// The data contains keys. Note that in a MockStore, GetCurrentValue returns the value directly.
	// For JSONDB, Scan expects the value to be flattened or managed.
	// But Scan implementation calls `store.First/Next` and `store.GetCurrentKey/Value`.
	// Then it calls `flattenItem` likely.

	usersStore := NewMockStore("users", []MockItem{
		{
			Key: "u1",
			Value: map[string]any{
				"id":          "u1",
				"name":        "Alice",
				"totalamount": 100,
			},
		},
	})

	// 2. Setup ScriptEngine
	// We map the store to a variable name "users_store"
	engineCtx := &ScriptContext{
		Variables: make(map[string]any),
		Stores: map[string]jsondb.StoreAccessor{
			"users_store": usersStore,
		},
	}

	// Create engine with a dummy resolver (we don't need it for this test)
	engine := &ScriptEngine{
		Context: engineCtx,
	}

	// 3. Call Scan
	// We ask to scan the store variable "users_store"
	ctx := context.Background()
	args := map[string]any{
		"store": "users_store",
	}

	result, err := engine.Scan(ctx, args)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	// 4. Verify Keys in Result
	rows, ok := result.([]map[string]any) // Scan returns []map[string]any
	if !ok {
		// Fallback for more robust testing if return type changes
		if rowsAny, okAny := result.([]any); okAny {
			// Convert to []map[string]any if possible
			t.Logf("Warning: Scan returned []any instead of []map[string]any")
			rows = make([]map[string]any, len(rowsAny))
			for i, r := range rowsAny {
				if m, ok := r.(map[string]any); ok {
					rows[i] = m
				} else {
					t.Fatalf("Row %d is not map[string]any", i)
				}
			}
		} else {
			t.Fatalf("Result is not []map[string]any, got %T", result)
		}
	}

	if len(rows) == 0 {
		t.Fatal("No rows returned")
	}

	// Scan flattens results.
	// The result items should be map[string]any with prefixed keys.
	firstRow := rows[0]

	// We expect keys to be prefixed with "users." (the store name), NOT "users_store." (the var name)
	expectedKey := "users.id"
	if _, found := firstRow[expectedKey]; !found {
		t.Logf("Available keys: %v", getKeysTest(firstRow))
		t.Errorf("Expected key '%s' not found. Prefixing is likely incorrect.", expectedKey)
	}

	// Ensure the incorrect key is NOT there
	badKey := "users_store.id"
	if _, found := firstRow[badKey]; found {
		t.Errorf("Found incorrect key '%s'. Variable name was used as prefix instead of Store Name.", badKey)
	}
}

func getKeysTest(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
