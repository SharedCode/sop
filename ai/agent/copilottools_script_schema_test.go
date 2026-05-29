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
		`emit a combined flat record by default`,
		`call gettoolinfo('execute_script')`,
		`filter expects {condition: object}`,
		`{\"first_name\":{\"$eq\":\"John\"}}`,
		`{\"orders.total_amount\":{\"$gt\":500}}`,
		`join and join_right expect a right-side store plus an on map derived from the researched relation mapping and emit a combined flat record unless a later project step reshapes it.`,
	}

	for _, check := range checks {
		if !strings.Contains(schema, check) {
			t.Fatalf("expected execute_script schema to contain %q\nSchema: %s", check, schema)
		}
	}
}

func TestExecuteScriptInstruction_MentionsResearchAndConcreteShapes(t *testing.T) {
	checks := []string{"full ordered JSON AST", "{op, args?, input_var?, result_var?}", "begin a transaction", "result_var/input_var", "list_stores", "schema=...", "relations=[...]", "join-field semantics", "combined flat record by default", "gettoolinfo('execute_script')", "concrete predicate objects", "concrete join mappings"}
	for _, check := range checks {
		if !strings.Contains(ExecuteScriptInstruction, check) {
			t.Fatalf("expected ExecuteScriptInstruction to contain %q\nInstruction: %s", check, ExecuteScriptInstruction)
		}
	}
}

func TestStoresAndSpacesInstructions_AreRichEnoughForToolContext(t *testing.T) {
	storesChecks := []string{"stores:[...]", "schema=...", "relations=[...]"}
	for _, check := range storesChecks {
		if !strings.Contains(ListStoresInstruction, check) {
			t.Fatalf("expected ListStoresInstruction to contain %q\nInstruction: %s", check, ListStoresInstruction)
		}
	}
	spacesChecks := []string{"persist beyond the current chat", "exact kb_name", "semantic refresh"}
	combinedSpaces := strings.Join([]string{MintToSpaceInstruction, ReadSpaceConfigInstruction, VectorizeSpaceInstruction}, "\n")
	for _, check := range spacesChecks {
		if !strings.Contains(combinedSpaces, check) {
			t.Fatalf("expected combined Spaces instructions to contain %q\nInstructions: %s", check, combinedSpaces)
		}
	}
}
