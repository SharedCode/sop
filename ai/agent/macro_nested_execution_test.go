package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func TestMacro_Play_Nested_Execution(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// 2. Define Child Macro: "echo_msg"
	// It simply "says" the message.
	childMacro := ai.Macro{
		Name:       "echo_msg",
		Parameters: []string{"msg"},
		Steps: []ai.MacroStep{
			{
				Type:    "say",
				Message: "Child says: {{.msg}}",
			},
		},
	}

	// 3. Define Parent Macro: "greet_user"
	// It takes "user" and calls "echo_msg" with "Hello {{.user}}"
	parentMacro := ai.Macro{
		Name:       "greet_user",
		Parameters: []string{"user"},
		Steps: []ai.MacroStep{
			{
				Type:      "macro",
				MacroName: "echo_msg",
				MacroArgs: map[string]string{
					"msg": "Hello {{.user}}",
				},
			},
		},
	}

	// 4. Save Macros
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "macros", tx)
	store.Save(ctx, "general", "echo_msg", childMacro)
	store.Save(ctx, "general", "greet_user", parentMacro)
	tx.Commit(ctx)

	// 5. Initialize Service
	svc := NewService(nil, sysDB, nil, nil, nil, nil, false)

	// 6. Execute Parent Macro
	// /play greet_user user=Alice
	cmd := "/play greet_user user=Alice"
	resp, err := svc.Ask(ctx, cmd)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// 7. Verify Output
	// We expect the output to contain the JSON structure with the "say" result
	// The "say" step in the child macro should produce "Child says: Hello Alice"
	
	// The output format is a JSON array of StepExecutionResult
	// We look for the string "Child says: Hello Alice" in the response.
	expected := "Child says: Hello Alice"
	if !strings.Contains(resp, expected) {
		t.Errorf("Nested macro execution failed.\nExpected output to contain: %s\nGot: %s", expected, resp)
	}
}
