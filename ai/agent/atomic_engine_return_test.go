package agent

import "testing"

func TestSelectExecuteScriptReturn_UsesLastUpdatedVar(t *testing.T) {
	ctx := NewScriptContext()
	ctx.LastUpdatedVar = "f2"
	ctx.Variables["f2"] = []map[string]any{{"users.first_name": "John", "orders.total_amount": 831}}

	choice := selectExecuteScriptReturn(&ScriptEngine{}, ctx, []ScriptInstruction{{Op: "defer"}})

	if choice.Source != "last_updated_var" {
		t.Fatalf("expected last_updated_var source, got %#v", choice)
	}
	if choice.Detail != "f2" {
		t.Fatalf("expected last updated var detail f2, got %#v", choice)
	}
	if choice.Value == nil {
		t.Fatalf("expected selected value, got %#v", choice)
	}
}

func TestSelectExecuteScriptReturn_PrefersOutputVariable(t *testing.T) {
	ctx := NewScriptContext()
	ctx.LastUpdatedVar = "f2"
	ctx.Variables["output"] = []map[string]any{{"first_name": "John"}}
	ctx.Variables["f2"] = []map[string]any{{"first_name": "Jane"}}

	choice := selectExecuteScriptReturn(&ScriptEngine{}, ctx, []ScriptInstruction{{Op: "defer"}})

	if choice.Source != "output" {
		t.Fatalf("expected output source, got %#v", choice)
	}
	rows, ok := choice.Value.([]map[string]any)
	if !ok || len(rows) != 1 || rows[0]["first_name"] != "John" {
		t.Fatalf("expected output rows, got %#v", choice.Value)
	}
}

func TestSelectExecuteScriptReturn_ExplicitReturnNilUsesSuccessMessage(t *testing.T) {
	choice := selectExecuteScriptReturn(&ScriptEngine{}, NewScriptContext(), []ScriptInstruction{{Op: "return"}})

	if choice.Source != "success_message" {
		t.Fatalf("expected success_message source, got %#v", choice)
	}
	if choice.Detail != "explicit_return_nil" {
		t.Fatalf("expected explicit_return_nil detail, got %#v", choice)
	}
	if choice.SuccessMessage == "" {
		t.Fatalf("expected success message, got %#v", choice)
	}
}
