package agent

import (
	"context"
	"encoding/json"
	"testing"
	"github.com/stretchr/testify/assert"
)

func TestReproProjectEmptyResult(t *testing.T) {
	// 1. Setup Data
	orders := []MockItem{
		{Key: "1", Value: map[string]any{"id": "1", "status": "delivered", "user_id": "u1", "total": 100}},
		{Key: "2", Value: map[string]any{"id": "2", "status": "pending", "user_id": "u2", "total": 50}},
		{Key: "3", Value: map[string]any{"id": "3", "status": "delivered", "user_id": "u1", "total": 200}},
	}
	users := []MockItem{
		{Key: "u1", Value: map[string]any{"id": "u1", "name": "Alice"}},
		{Key: "u2", Value: map[string]any{"id": "u2", "name": "Bob"}},
	}

	mockOrders := NewMockStore("orders", orders)
	mockUsers := NewMockStore("users", users)

	// 2. Init Engine
	ctx := context.Background()
	scriptCtx := NewScriptContext()
	scriptCtx.Stores["orders"] = mockOrders
	scriptCtx.Stores["users"] = mockUsers

	// Mock DB Resolver
	resolver := func(name string) (Database, error) {
		return nil, nil
	}

	engine := NewScriptEngine(scriptCtx, resolver)

	// 3. Define Script - Aligned with User Flow (Scan -> Join -> Filter)
	// User reported "Filter" failing to see fields on joined result if qualified
	// Hypothesis: Missing explicit 'alias' in Join causes flattening, but Filter uses qualified name.
	scriptJSON := `[
		{"op": "scan", "args": {"store": "users"}, "result_var": "u"},
		{"op": "join", "input_var": "u", "args": {"store": "orders", "on": {"id": "user_id"}}, "result_var": "j"},
		{"op": "filter", "input_var": "j", "args": {"condition": {"orders.status": "delivered"}}, "result_var": "f"},
		{"op": "project", "input_var": "f", "result_var": "final", "args": {"fields": [
			{"Dst": "Customer", "Src": "name"},
			{"Dst": "Status", "Src": "orders.status"}
		]}}
	]`

	var script []ScriptInstruction
	err := json.Unmarshal([]byte(scriptJSON), &script)
	assert.NoError(t, err)

	// normalize script (sanitize)
	script = sanitizeScript(script)

	// 4. Execute
	err = engine.Execute(ctx, script)
	assert.NoError(t, err)

	// 5. Verify Result
	res := engine.LastResult
	assert.NotNil(t, res)

	// Must be a Cursor
	cursor, ok := res.(ScriptCursor)
	assert.True(t, ok, "Result should be a ScriptCursor")

	// Iterate and Collect
	var results []map[string]any
	for {
		item, ok, err := cursor.Next(ctx)
		assert.NoError(t, err)
		if !ok {
			break
		}
		if m, ok := item.(map[string]any); ok {
			results = append(results, m)
		} else if om, ok := item.(*OrderedMap); ok {
			results = append(results, om.m)
		} else {
			// fallback
		}
	}

	// Expect 2 items (Order 1 and 3 are delivered, match u1)
	assert.Len(t, results, 2)
	if len(results) > 0 {
		// Verify Projection
		first := results[0]
		assert.Contains(t, first, "Customer")
		assert.Contains(t, first, "Status")
		assert.Equal(t, "Alice", first["Customer"])
		assert.Equal(t, "delivered", first["Status"])
	}
}
