package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func TestScript_Play_Nested_Execution(t *testing.T) {
	// 1. Setup
	tmpDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Clustered,
		StoresFolders: []string{tmpDir},
		CacheType:     sop.InMemory,
	}
	sysDB := database.NewDatabase(dbOpts)
	ctx := context.Background()

	// 2. Define Child Script: "echo_msg"
	// It simply "says" the message.
	childScript := ai.Script{
		Name:       "echo_msg",
		Parameters: []string{"msg"},
		Steps: []ai.ScriptStep{
			{
				Type:    "say",
				Message: "Child says: {{.msg}}",
			},
		},
	}

	// 3. Define Parent Script: "greet_user"
	// It takes "user" and calls "echo_msg" with "Hello {{.user}}"
	parentScript := ai.Script{
		Name:       "greet_user",
		Parameters: []string{"user"},
		Steps: []ai.ScriptStep{
			{
				Type:       "call_script",
				ScriptName: "echo_msg",
				ScriptArgs: map[string]string{
					"msg": "Hello {{.user}}",
				},
			},
		},
	}

	// 4. Save Scripts
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	store, _ := sysDB.OpenModelStore(ctx, "scripts", tx)
	store.Save(ctx, "general", "echo_msg", childScript)
	store.Save(ctx, "general", "greet_user", parentScript)
	tx.Commit(ctx)

	// 5. Initialize Service
	svc := NewService(nil, sysDB, nil, nil, nil, nil, false)

	// 6. Execute Parent Script
	// /run greet_user user=Alice
	cmd := "/run greet_user user=Alice"
	resp, err := svc.Ask(ctx, cmd)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// 7. Verify Output
	// We expect the output to contain the JSON structure with the "say" result
	// The "say" step in the child script should produce "Child says: Hello Alice"

	// The output format is a JSON array of StepExecutionResult
	// We look for the string "Child says: Hello Alice" in the response.
	expected := "Child says: Hello Alice"
	if !strings.Contains(resp, expected) {
		t.Errorf("Nested script execution failed.\nExpected output to contain: %s\nGot: %s", expected, resp)
	}
}
