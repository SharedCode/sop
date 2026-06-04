package agent

import (
	"testing"
)

// TestSanitizeThenGrammarValidation verifies that the order of operations
// (sanitize before grammar validation) works correctly.
func TestSanitizeThenGrammarValidation_HoistCommitFixesUnreachable(t *testing.T) {
	// This script has commit AFTER return, which looks like unreachable code
	// BUT sanitizeScript.hoistCommitBeforeReturn should fix it BEFORE grammar validation
	script := []ScriptInstruction{
		{Op: "scan", Args: map[string]any{"store": "users"}, ResultVar: "cursor"},
		{Op: "return", InputVar: "cursor"},
		{Op: "commit_tx", Args: map[string]any{"transaction": "tx"}}, // Looks unreachable, but sanitize will hoist it
	}

	// Sanitize should hoist commit before return
	sanitized := sanitizeScript(script)

	// Now grammar validation should NOT complain about unreachable code
	err := validateScriptGrammar(sanitized)
	if err != nil {
		t.Errorf("Expected no error after sanitize hoists commit, got: %s", err)
	}

	// Verify commit was actually moved before return
	commitIdx := -1
	returnIdx := -1
	for i, instr := range sanitized {
		if instr.Op == "commit_tx" {
			commitIdx = i
		}
		if instr.Op == "return" {
			returnIdx = i
		}
	}

	if commitIdx < 0 || returnIdx < 0 {
		t.Fatal("Expected both commit_tx and return in sanitized script")
	}

	if commitIdx >= returnIdx {
		t.Errorf("Expected commit_tx (idx=%d) to be before return (idx=%d) after sanitize", commitIdx, returnIdx)
	}
}

// TestSanitizeThenGrammarValidation_DefaultResultVars verifies that sanitize
// adds default result_vars before grammar validation checks variable flow.
func TestSanitizeThenGrammarValidation_DefaultResultVars(t *testing.T) {
	// Script with missing result_var on open_store
	script := []ScriptInstruction{
		{Op: "begin_tx", Args: map[string]any{"mode": "read"}},                         // Missing result_var, sanitize adds "tx"
		{Op: "open_store", Args: map[string]any{"name": "users", "transaction": "tx"}}, // Missing result_var, sanitize adds "users"
	}

	// Sanitize should add default result_vars
	sanitized := sanitizeScript(script)

	// Grammar validation should NOT complain about missing variables
	err := validateScriptGrammar(sanitized)
	if err != nil {
		t.Errorf("Expected no error after sanitize adds defaults, got: %s", err)
	}

	// Verify result_vars were added
	if sanitized[0].ResultVar == "" {
		t.Error("Expected sanitize to add result_var to begin_tx")
	}
	if sanitized[1].ResultVar == "" {
		t.Error("Expected sanitize to add result_var to open_store")
	}
}

// TestGrammarValidation_StillCatchesDuplicateJoinsAfterSanitize verifies that
// moving grammar validation after sanitize doesn't break duplicate join detection.
func TestGrammarValidation_StillCatchesDuplicateJoinsAfterSanitize(t *testing.T) {
	// Duplicate joins that sanitize won't fix
	script := []ScriptInstruction{
		{Op: "select", Args: map[string]any{"store": "users"}, ResultVar: "matched_users"},
		{Op: "join", Args: map[string]any{"store": "orders"}, InputVar: "matched_users", ResultVar: "joined1"},
		{Op: "join", Args: map[string]any{"store": "orders"}, InputVar: "joined1", ResultVar: "joined2"}, // Duplicate
	}

	// Sanitize won't fix duplicate joins
	sanitized := sanitizeScript(script)

	// Grammar validation should still catch this
	err := validateScriptGrammar(sanitized)
	if err == nil {
		t.Error("Expected grammar validation to catch duplicate consecutive joins after sanitize")
	}
}
