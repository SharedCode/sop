package agent

import (
	"context"
	"testing"
)

func TestEngineLastResultPropagation(t *testing.T) {
	// 1. Mock Steps that construct a chain
	// We can't use "scan" etc easily without a real DB/Store, so we'll use "assign" or custom ops if possible.
	// CompileScript parses ops. Let's use simple ops if available. "list_new", "list_append".

	script := []ScriptInstruction{
		{
			Op: "list_new",
		},
		{
			Op: "list_append",
			Args: map[string]any{
				"value": "item1",
			},
		},
		{
			Op: "list_append",
			Args: map[string]any{
				"value": "item2",
			},
		},
	}

	// 2. Compile
	compiled, err := CompileScript(script)
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	// 3. Execute
	ctx := context.Background()
	engine := NewScriptEngine(NewScriptContext(), nil)

	err = compiled(ctx, engine)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// 4. Verify LastResult
	// "list_append" returns the list.
	// Step 1: list_new -> []
	// Step 2: list_append -> ["item1"]
	// Step 3: list_append -> ["item1", "item2"]

	list, ok := engine.LastResult.([]any)
	if !ok {
		t.Fatalf("LastResult is not a list: %T", engine.LastResult)
	}

	if len(list) != 2 {
		t.Errorf("Expected list length 2, got %d", len(list))
	}
	if list[0] != "item1" || list[1] != "item2" {
		t.Errorf("Unexpected list content: %v", list)
	}
}
