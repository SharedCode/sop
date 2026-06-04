package agent

import (
	"strings"
	"testing"
)

func TestValidateScriptGrammar_DuplicateConsecutiveJoin(t *testing.T) {
	// This is the actual problematic script from the logs
	script := []ScriptInstruction{
		{Op: "begin_tx", Args: map[string]any{"database": "dev_db", "mode": "read"}, ResultVar: "tx"},
		{Op: "open_store", Args: map[string]any{"database": "dev_db", "name": "users", "transaction": "tx"}, ResultVar: "users_store"},
		{Op: "open_store", Args: map[string]any{"database": "dev_db", "name": "users_orders", "transaction": "tx"}, ResultVar: "users_orders_store"},
		{Op: "open_store", Args: map[string]any{"database": "dev_db", "name": "orders", "transaction": "tx"}, ResultVar: "orders_store"},
		{Op: "select", Args: map[string]any{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}, "store": "users_store"}, ResultVar: "matched_users"},

		// First join to users_orders_store
		{Op: "join", Args: map[string]any{"on": map[string]any{"key": "key"}, "store": "users_orders_store", "type": "inner"}, InputVar: "matched_users", ResultVar: "user_order_links__bridge"},

		// DUPLICATE: Second join to SAME store (users_orders_store) immediately after
		{Op: "join", Args: map[string]any{"on": map[string]any{"value": "key"}, "store": "users_orders_store", "type": "inner"}, InputVar: "user_order_links__bridge", ResultVar: "user_order_links"},

		// Third join to orders_store
		{Op: "join", Args: map[string]any{"on": map[string]any{"key": "key"}, "store": "orders_store", "type": "inner"}, InputVar: "user_order_links", ResultVar: "joined_orders__bridge"},

		// DUPLICATE: Fourth join to SAME store (orders_store) immediately after
		{Op: "join", Args: map[string]any{"on": map[string]any{"value": "key"}, "store": "orders_store", "type": "inner"}, InputVar: "joined_orders__bridge", ResultVar: "joined_orders"},

		{Op: "filter", Args: map[string]any{"condition": map[string]any{"orders.total_amount": map[string]any{"$gt": 500}}}, InputVar: "joined_orders", ResultVar: "filtered_orders"},
		{Op: "commit_tx", Args: map[string]any{"transaction": "tx"}},
		{Op: "return", InputVar: "filtered_orders"},
	}

	err := validateScriptGrammar(script)
	if err == nil {
		t.Fatal("Expected grammar validation error for duplicate consecutive joins, got nil")
	}

	errMsg := err.Error()

	// Should detect duplicate join issues (checking for the actual error text)
	if !strings.Contains(errMsg, "Joining to store") || !strings.Contains(errMsg, "already joined to") {
		t.Errorf("Expected duplicate consecutive join error description, got: %s", errMsg)
	}

	// Should mention both problematic stores
	if !strings.Contains(errMsg, "users_orders") {
		t.Errorf("Expected error to mention 'users_orders' store, got: %s", errMsg)
	}
	if !strings.Contains(errMsg, "orders") {
		t.Errorf("Expected error to mention 'orders' store, got: %s", errMsg)
	}

	// Check error count
	grammarErr, ok := err.(*grammarValidationErrors)
	if !ok {
		t.Fatalf("Expected *grammarValidationErrors, got %T", err)
	}

	duplicateJoinErrors := 0
	for _, e := range grammarErr.Errors {
		if e.Category == "duplicate_consecutive_join" {
			duplicateJoinErrors++
		}
	}

	if duplicateJoinErrors != 2 {
		t.Errorf("Expected 2 duplicate_consecutive_join errors, got %d", duplicateJoinErrors)
	}
}

func TestValidateScriptGrammar_ValidJoinSequence(t *testing.T) {
	// Valid script: joins to DIFFERENT stores
	script := []ScriptInstruction{
		{Op: "begin_tx", Args: map[string]any{"mode": "read"}, ResultVar: "tx"},
		{Op: "open_store", Args: map[string]any{"name": "users", "transaction": "tx"}, ResultVar: "users_store"},
		{Op: "open_store", Args: map[string]any{"name": "users_orders", "transaction": "tx"}, ResultVar: "users_orders_store"},
		{Op: "open_store", Args: map[string]any{"name": "orders", "transaction": "tx"}, ResultVar: "orders_store"},
		{Op: "select", Args: map[string]any{"store": "users_store"}, ResultVar: "matched_users"},

		// Join to users_orders_store
		{Op: "join", Args: map[string]any{"store": "users_orders_store"}, InputVar: "matched_users", ResultVar: "user_orders"},

		// Join to orders_store (DIFFERENT store, should be valid)
		{Op: "join", Args: map[string]any{"store": "orders_store"}, InputVar: "user_orders", ResultVar: "final_orders"},

		{Op: "commit_tx", Args: map[string]any{"transaction": "tx"}},
		{Op: "return", InputVar: "final_orders"},
	}

	err := validateScriptGrammar(script)
	if err != nil {
		t.Errorf("Expected no grammar validation error for valid join sequence, got: %s", err)
	}
}

func TestValidateScriptGrammar_UndefinedVariable(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "filter", Args: map[string]any{"condition": map[string]any{"x": 1}}, InputVar: "undefined_cursor", ResultVar: "output"},
	}

	err := validateScriptGrammar(script)
	if err == nil {
		t.Fatal("Expected grammar validation error for undefined variable, got nil")
	}

	if !strings.Contains(err.Error(), "undefined variable") {
		t.Errorf("Expected 'undefined variable' error, got: %s", err.Error())
	}
}

func TestValidateScriptGrammar_UnreachableCode(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "scan", Args: map[string]any{"store": "users"}, ResultVar: "cursor"},
		{Op: "return", InputVar: "cursor"},
		{Op: "filter", Args: map[string]any{"condition": map[string]any{"x": 1}}, InputVar: "cursor", ResultVar: "filtered"}, // Unreachable
	}

	err := validateScriptGrammar(script)
	if err == nil {
		t.Fatal("Expected grammar validation error for unreachable code, got nil")
	}

	if !strings.Contains(err.Error(), "unreachable") {
		t.Errorf("Expected 'unreachable' error, got: %s", err.Error())
	}
}

func TestValidateScriptGrammar_MissingJoinInputVar(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "join", Args: map[string]any{"store": "orders"}, ResultVar: "output"}, // Missing input_var
	}

	err := validateScriptGrammar(script)
	if err == nil {
		t.Fatal("Expected grammar validation error for missing join input_var, got nil")
	}

	if !strings.Contains(err.Error(), "requires input_var") {
		t.Errorf("Expected 'requires input_var' error, got: %s", err.Error())
	}
}

func TestValidateScriptGrammar_MissingJoinStore(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "scan", Args: map[string]any{"store": "users"}, ResultVar: "cursor"},
		{Op: "join", Args: map[string]any{"on": map[string]any{"key": "key"}}, InputVar: "cursor", ResultVar: "output"}, // Missing store ref
	}

	err := validateScriptGrammar(script)
	if err == nil {
		t.Fatal("Expected grammar validation error for missing join store, got nil")
	}

	if !strings.Contains(err.Error(), "requires a store reference") {
		t.Errorf("Expected 'requires a store reference' error, got: %s", err.Error())
	}
}
