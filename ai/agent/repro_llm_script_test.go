package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/sharedcode/sop/jsondb"
	"github.com/stretchr/testify/assert"
)

func TestRepro_LLMScript(t *testing.T) {
	// 1. Setup Environment
	agent := &DataAdminAgent{
		registry: NewRegistry(),
	}
	agent.registerTools()

	// 2. Mock Data
	// Left Store: "department"
	// User Logic: Department has 1 record
	storeA := NewMockStore("department", []MockItem{
		{
			Key: "d1",
			Value: map[string]any{
				"department": "HR",
				"region":     "APAC",
				"director":   "Joe Petit",
				"color":      "Blue",
			},
		},
	})

	// Right Store: "employees"
	// User Logic: Employees has ~4000 records. We'll simulate 5 matching ones.
	// Key: "employee_id"
	itemsB := []MockItem{}
	for i := 0; i < 5; i++ {
		itemsB = append(itemsB, MockItem{
			Key: fmt.Sprintf("e%d", i),
			Value: map[string]any{
				"name":       fmt.Sprintf("Employee %d", i),
				"department": "HR",
				"region":     "APAC",
				"active":     true,
				"salary":     50000 + (i * 1000),
			},
		})
	}
	storeB := NewMockStore("employees", itemsB)

	// 3. Prepare Context with Pre-registered Stores (simulating open_db/open_store effects if we mocked them)
	// But the script calls open_db/open_store.
	// We need to inject a "Database Resolver" or similar that returns our mocks.

	// The execute_script implementation uses `e.OpenDB` -> `e.ResolveDatabase`.
	// We need to configure the ScriptEngine that `toolExecuteScript` creates.
	// `toolExecuteScript` creates a new ScriptEngine internally.

	// Use `InjectScriptEngineConfig` or similar if available?
	// Or we can manually run `toolExecuteScript` logic but with our configured engine?
	// `toolExecuteScript` is a wrapper.
	// Let's look at `execute_script` implementation. It calls `engine.Execute`.

	// To intercept `open_db`, we might need to modify `toolExecuteScript` to allow injection?
	// Or we can modify the Agent's state.
	// DataAdminAgent has `databases` map.
	// `toolExecuteScript` uses `a.databases` to pass to the engine?
	// Let's check `toolExecuteScript` implementation again.

	// Actually, `toolExecuteScript` creates `ScriptEngine` and sets `ResolveDatabase`.
	// `ResolveDatabase` looks up `a.databases`.
	// Mocks need to be in `a.databases`.
	// But `a.databases` maps name -> `sop.DatabaseOptions`.
	// It doesn't map to *instances*.

	// However, `ScriptEngine` also has `Strategies`.
	// If `open_store` is called, it might look into `Context.Stores`.

	// WORKAROUND:
	// We can manually create the ScriptEngine and run the script body,
	// skipping the `open_db` / `open_store` steps if we pre-populate the context.
	// But the User wants to run the *exact* script.

	// If we want to run exact script, we need `open_store` to work.
	// `open_store` calls `transaction.GetPhasedTransaction().GetStore(...)`.
	// This requires a MockTransaction that matches `sop.Transaction` interface.

	// Our `MockTransaction` in repro_mocks.go needs to support `OpenStore`.
	// But `sop.Transaction` is complex.

	// ALTERNATIVE:
	// Run the script starting from `scan`, assuming stores are already opened.
	// This is a valid reproduction of the `join_right` failure, which is the core issue.
	// The `open_db` / `open_store` part is just setup.

	// User Script modified to remove setup steps, and we inject stores into variables "a" and "b".
	scriptJSON := `[
      {
        "args": {
          "store": "department",
          "stream": true
        },
        "op": "scan",
        "result_var": "a"
      },
      {
        "args": {
          "on": {
            "department": "department",
            "region": "region"
          },
          "store": "employees",
          "stream": true
        },
        "input_var": "a",
        "op": "join_right",
        "result_var": "b"
      },
      {
        "args": {
          "fields": [
            "a.",
            "b.name AS employee"
          ]
        },
        "input_var": "b",
        "op": "project",
        "result_var": "c"
      },
      {
        "args": {
          "limit": 2
        },
        "input_var": "c",
        "op": "limit",
        "result_var": "output"
      },
      {
        "args": {
          "value": "@output"
        },
        "op": "return"
      }
    ]`

	var script []ScriptInstruction
	if err := json.Unmarshal([]byte(scriptJSON), &script); err != nil {
		t.Fatalf("JSON parse error: %v", err)
	}

	// 4.1 Apply Sanitization (The Fix)
	// The core issue is that the LLM/User provides a script without explicit aliases,
	// relying on implicit behavior that fails.
	// Our fix is in sanitizeScript, which infers the alias from the projection.
	// We MUST run this step to verify the fix works.
	script = sanitizeScript(script)

	// 4. Manual Engine Setup (skip tool wrapper to inject mocks)
	engine := &ScriptEngine{
		Context: &ScriptContext{
			Variables: make(map[string]any),
			Stores:    make(map[string]jsondb.StoreAccessor),
		},
	}

	// Inject Stores into Variables and Stores map (matching user's script variable names)
	engine.Context.Variables["department"] = storeA
	engine.Context.Stores["department"] = storeA

	engine.Context.Variables["employees"] = storeB
	engine.Context.Stores["employees"] = storeB

	// 5. Execute
	err := engine.Execute(context.Background(), script)
	assert.NoError(t, err)

	// 6. Verify Output
	output, ok := engine.Context.Variables["output"]
	assert.True(t, ok, "Output variable should exist")

	cursor, ok := output.(ScriptCursor)
	assert.True(t, ok, "Output should be a cursor")

	item, ok, err := cursor.Next(context.Background())
	assert.NoError(t, err)
	assert.True(t, ok, "Should have 1 result")

	om := item.(*OrderedMap)
	t.Logf("Result Keys: %v", om.keys)
	t.Logf("Result Map: %v", om.m)

	// Check Required Fields
	// IMPORTANT: User expects "employee" (from "b.name as employee")
	assert.Contains(t, om.m, "employee")
	assert.Equal(t, "Employee 0", om.m["employee"])
}
