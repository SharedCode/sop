package agent

import (
	"context"
	"testing"
)

func TestCallFunction_AcceptsAliasKeys(t *testing.T) {
	engine := NewScriptEngine(NewScriptContext(), nil)
	engine.FunctionHandler = func(ctx context.Context, name string, params map[string]any) (any, error) {
		if name != "seed_tasks" {
			t.Fatalf("expected function name seed_tasks, got %q", name)
		}
		if params == nil || params["count"] != 5 {
			t.Fatalf("expected params to include count=5, got %#v", params)
		}
		return "ok", nil
	}

	result, err := engine.CallFunction(context.Background(), map[string]any{
		"function":  "seed_tasks",
		"arguments": map[string]any{"count": 5},
	})
	if err != nil {
		t.Fatalf("CallFunction failed: %v", err)
	}
	if result != "ok" {
		t.Fatalf("expected result ok, got %#v", result)
	}
}
