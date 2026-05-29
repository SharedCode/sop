package agent

import (
	"strings"
	"testing"
)

func TestBuildExecuteScriptArgsSchema_IncludesResearchAndOrchestrationGuidance(t *testing.T) {
	schema := buildExecuteScriptArgsSchema()

	checks := []string{
		`multi-step store orchestration`,
		`call list_stores first`,
		`schema=... and optional relations=[...]`,
		`read schema=... for exact field names`,
		`read relations=[...] for related-store and join-field semantics`,
		`returned relations=[...] entries as the source of truth`,
		`call gettoolinfo('execute_script')`,
		`filter expects {condition: object}`,
		`{\"first_name\":{\"$eq\":\"John\"}}`,
		`{\"orders.total_amount\":{\"$gt\":500}}`,
		`join and join_right expect a right-side store plus an on map derived from the researched relation mapping.`,
	}

	for _, check := range checks {
		if !strings.Contains(schema, check) {
			t.Fatalf("expected execute_script schema to contain %q\nSchema: %s", check, schema)
		}
	}
}

func TestExecuteScriptInstruction_MentionsResearchAndConcreteShapes(t *testing.T) {
	checks := []string{"begin a transaction", "result_var/input_var", "list_stores", "schema=...", "relations=[...]", "join-field semantics", "gettoolinfo('execute_script')", "concrete predicate objects", "concrete join mappings"}
	for _, check := range checks {
		if !strings.Contains(ExecuteScriptInstruction, check) {
			t.Fatalf("expected ExecuteScriptInstruction to contain %q\nInstruction: %s", check, ExecuteScriptInstruction)
		}
	}
}
