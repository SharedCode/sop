package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

// TestMacroParameterizationWorkflow demonstrates how a recorded macro with hardcoded values
// is converted into a parameterized macro using the /macro parameterize command.
func TestMacroParameterizationWorkflow(t *testing.T) {
	// 1. Setup Infrastructure
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// 2. Simulate a "Recorded" Macro
	// Imagine the user recorded: "select * from employees where department = 'Sales'"
	macroName := "find_sales_employees"
	originalMacro := ai.Macro{
		Name: macroName,
		Steps: []ai.MacroStep{
			{
				Type:    "command",
				Command: "select",
				Args: map[string]any{
					"query": "select * from employees where department = 'Sales'",
				},
			},
			{
				Type:   "ask",
				Prompt: "Summarize the performance of the Sales team",
			},
		},
	}

	// Save this "recorded" macro to the system DB
	tx, err := sysDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("Failed to begin tx: %v", err)
	}
	store, err := sysDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		t.Fatalf("Failed to open store: %v", err)
	}
	if err := store.Save(ctx, "general", macroName, originalMacro); err != nil {
		t.Fatalf("Failed to save macro: %v", err)
	}
	tx.Commit(ctx)

	// 3. Initialize Service
	// We only need the systemDB to be working for this test
	svc := NewService(nil, sysDB, nil, nil, nil, nil, false)

	// 4. Execute Parameterization Command
	// User wants to replace 'Sales' with a parameter named 'dept'
	// Command: /macro parameterize <macro_name> <param_name> <value_to_replace>
	cmd := "/macro parameterize find_sales_employees dept Sales"

	response, err := svc.Ask(ctx, cmd)
	if err != nil {
		t.Fatalf("Command failed: %v", err)
	}
	t.Logf("Command Response: %s", response)

	// 5. Verify the Transformation
	// Reload the macro
	tx, _ = sysDB.BeginTransaction(ctx, sop.ForReading)
	store, _ = sysDB.OpenModelStore(ctx, "macros", tx)
	var updatedMacro ai.Macro
	store.Load(ctx, "general", macroName, &updatedMacro)
	tx.Commit(ctx)

	// Check Parameters list
	foundParam := false
	for _, p := range updatedMacro.Parameters {
		if p == "dept" {
			foundParam = true
			break
		}
	}
	if !foundParam {
		t.Errorf("Expected 'dept' in Parameters list, got: %v", updatedMacro.Parameters)
	}

	// Check Step 1 (Command Arg)
	argQuery := updatedMacro.Steps[0].Args["query"].(string)
	expectedQuery := "select * from employees where department = '{{.dept}}'"
	if argQuery != expectedQuery {
		t.Errorf("Step 1 replacement failed.\nGot:      %s\nExpected: %s", argQuery, expectedQuery)
	}

	// Check Step 2 (Prompt)
	prompt := updatedMacro.Steps[1].Prompt
	expectedPrompt := "Summarize the performance of the {{.dept}} team"
	if prompt != expectedPrompt {
		t.Errorf("Step 2 replacement failed.\nGot:      %s\nExpected: %s", prompt, expectedPrompt)
	}
}
