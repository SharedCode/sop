package agent

import (
	"context"
	"testing"
)

func TestProjectionBug(t *testing.T) {
	ctx := context.Background()
	// Setup Engine
	engine := NewScriptEngine(NewScriptContext(), nil)

	// 1. Setup Data
	// users_by_age (Index) -> Key: 30, Value: "user1"
	usersByAge := NewMockStore("users_by_age", []MockItem{
		{Key: 30, Value: "user1"},
	})

	// users (Main) -> Key: "user1", Value: map{"name":"John", ...}
	users := NewMockStore("users", []MockItem{
		{Key: "user1", Value: map[string]any{"name": "John", "age": 30}},
	})

	// Register Stores
	// Note: MockStore must satisfy jsondb.StoreAccessor interface expected by engine.Context.Stores
	engine.Context.Stores["users_by_age"] = usersByAge
	engine.Context.Stores["users"] = users

	// 2. Define Script
	steps := []ScriptInstruction{
		{
			Op: "scan",
			Args: map[string]any{
				"store": "users_by_age",
			},
		},
		{
			Op: "join_right",
			Args: map[string]any{
				"store": "users",
				"on":    map[string]any{"value": "key"},
			},
		},
		{
			Op: "project",
			Args: map[string]any{
				"fields": []string{"users_by_age.key as Age", "users.*"},
			},
			ResultVar: "final_output",
		},
	}

	// 3. Execute
	err := engine.Execute(ctx, steps)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// 4. Verify Output
	val, ok := engine.Context.Variables["final_output"]
	if !ok {
		t.Fatalf("final_output variable not found")
	}

	var list []any
	if cursor, ok := val.(ScriptCursor); ok {
		for {
			item, ok, err := cursor.Next(ctx)
			if err != nil {
				t.Fatalf("Cursor error: %v", err)
			}
			if !ok {
				break
			}
			list = append(list, item)
		}
	} else if l, ok := val.([]any); ok {
		list = l
	} else {
		t.Fatalf("Unknown result type: %T", val)
	}

	if len(list) == 0 {
		t.Errorf("Expected rows, got 0")
		return
	}

	item0 := list[0]
	t.Logf("Result Item 0: %+v", item0)

	var age any
	var name any

	if om, ok := item0.(*OrderedMap); ok {
		age = om.m["Age"]
		name = om.m["name"]
	} else if m, ok := item0.(map[string]any); ok {
		age = m["Age"]
		name = m["name"]
	} else {
		t.Fatalf("Item 0 not a map? %T", item0)
	}

	if age != 30 {
		t.Errorf("Expected Age=30, got %v", age)
	}
	if name != "John" {
		t.Errorf("Expected name=John, got %v", name)
	}
}
