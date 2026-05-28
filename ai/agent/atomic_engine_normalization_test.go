package agent

import "testing"

func TestNormalizeScriptStepForCompatibility_SortLegacyShape(t *testing.T) {
	step := map[string]any{
		"op": "sort",
		"args": map[string]any{
			"pipe": "scanned_users",
			"key":  "age",
			"desc": true,
		},
	}

	normalizeScriptStepForCompatibility(step)

	if got, _ := step["input_var"].(string); got != "scanned_users" {
		t.Fatalf("expected input_var to be normalized from pipe, got %q", got)
	}
	args := step["args"].(map[string]any)
	fields, ok := args["fields"].([]any)
	if !ok || len(fields) != 1 || fields[0] != "age desc" {
		t.Fatalf("expected sort fields to be normalized, got %#v", args["fields"])
	}
	if _, ok := args["pipe"]; ok {
		t.Fatalf("expected legacy pipe arg to be removed")
	}
	if _, ok := args["key"]; ok {
		t.Fatalf("expected legacy key arg to be removed")
	}
	if _, ok := args["desc"]; ok {
		t.Fatalf("expected legacy desc arg to be removed")
	}
}

func TestNormalizeScriptStepForCompatibility_SortFieldDescendingShape(t *testing.T) {
	step := map[string]any{
		"op": "sort",
		"args": map[string]any{
			"field":      "age",
			"descending": true,
		},
	}

	normalizeScriptStepForCompatibility(step)

	args := step["args"].(map[string]any)
	fields, ok := args["fields"].([]any)
	if !ok || len(fields) != 1 || fields[0] != "age desc" {
		t.Fatalf("expected sort fields to be normalized, got %#v", args["fields"])
	}
	if _, ok := args["field"]; ok {
		t.Fatalf("expected legacy field arg to be removed")
	}
	if _, ok := args["descending"]; ok {
		t.Fatalf("expected legacy descending arg to be removed")
	}
}

func TestPreserveLastResultOnNil(t *testing.T) {
	if !preserveLastResultOnNil("commit_tx") {
		t.Fatalf("expected commit_tx to preserve last result")
	}
	if !preserveLastResultOnNil("rollback_tx") {
		t.Fatalf("expected rollback_tx to preserve last result")
	}
	if preserveLastResultOnNil("return") {
		t.Fatalf("did not expect return to preserve last result on nil")
	}
}

func TestIsInternalScriptHandle_NilAndPlainValues(t *testing.T) {
	if isInternalScriptHandle(nil) {
		t.Fatalf("nil should not be treated as an internal handle")
	}
	if isInternalScriptHandle("plain text") {
		t.Fatalf("plain text should not be treated as an internal handle")
	}
	if isInternalScriptHandle([]any{"a", "b"}) {
		t.Fatalf("lists should not be treated as internal handles")
	}
}

func TestSanitizeScript_CapturesImplicitOutputBeforeCommit(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "begin_tx", Args: map[string]any{"database": "dev_db", "mode": "read"}, ResultVar: "tx1"},
		{Op: "open_store", Args: map[string]any{"name": "users", "transaction": "tx1"}, ResultVar: "users_store"},
		{Op: "scan", Args: map[string]any{"store": "users_store"}},
		{Op: "sort", Args: map[string]any{"fields": []any{"age desc"}}},
		{Op: "commit_tx", Args: map[string]any{"transaction": "tx1"}},
	}

	sanitized := sanitizeScript(script)
	if got := sanitized[3].ResultVar; got != "output" {
		t.Fatalf("expected last data-producing step to capture implicit output, got %q", got)
	}
}
