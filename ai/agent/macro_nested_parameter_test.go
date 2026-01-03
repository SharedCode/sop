package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func TestMacroParameterization_Nested(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// 2. Create Parent Macro that calls a Child Macro
	// Parent Macro: "Audit Sales"
	// Step 1: Call "Find Employees" with dept="Sales"
	macroName := "audit_department"
	parentMacro := ai.Macro{
		Name: macroName,
		Steps: []ai.MacroStep{
			{
				Type:      "macro",
				MacroName: "find_employees",
				MacroArgs: map[string]string{
					"dept": "Sales",
					"mode": "verbose",
				},
			},
		},
	}

	// Save Parent Macro
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "macros", tx)
	store.Save(ctx, "general", macroName, parentMacro)
	tx.Commit(ctx)

	// 3. Initialize Service
	svc := NewService(nil, sysDB, nil, nil, nil, nil, false)

	// 4. Parameterize "Sales" -> "target_dept"
	cmd := "/macro parameterize audit_department target_dept Sales"
	resp, err := svc.Ask(ctx, cmd)
	if err != nil {
		t.Fatalf("Command failed: %v", err)
	}
	t.Logf("Response: %s", resp)

	// 5. Verify
	tx, _ = sysDB.BeginTransaction(ctx, sop.ForReading)
	store, _ = sysDB.OpenModelStore(ctx, "macros", tx)
	var updatedMacro ai.Macro
	store.Load(ctx, "general", macroName, &updatedMacro)
	tx.Commit(ctx)

	// Check Parameters
	hasParam := false
	for _, p := range updatedMacro.Parameters {
		if p == "target_dept" {
			hasParam = true
			break
		}
	}
	if !hasParam {
		t.Errorf("Expected 'target_dept' in parameters, got: %v", updatedMacro.Parameters)
	}

	// Check Nested Macro Arg
	step := updatedMacro.Steps[0]
	if step.Type != "macro" {
		t.Fatalf("Expected step type 'macro', got '%s'", step.Type)
	}

	val, ok := step.MacroArgs["dept"]
	if !ok {
		t.Fatalf("Missing 'dept' arg in nested macro step")
	}

	expected := "{{.target_dept}}"
	if val != expected {
		t.Errorf("Nested macro arg replacement failed.\nGot:      %s\nExpected: %s", val, expected)
	}

	// Ensure other args are untouched
	if step.MacroArgs["mode"] != "verbose" {
		t.Errorf("Expected 'mode' to remain 'verbose', got '%s'", step.MacroArgs["mode"])
	}
}
