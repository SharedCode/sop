package agent

import (
	"encoding/json"
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
		`join and join_right must reuse researched relation mappings instead of inventing field pairs.`,
		`Relation-target store variable for relation-driven joins`,
		`Concrete left-to-right join mapping.`,
		`Do not invent placeholder keys or ambiguous store-qualified names.`,
	}

	for _, check := range checks {
		if !strings.Contains(schema, check) {
			t.Fatalf("expected execute_script schema to contain %q\nSchema: %s", check, schema)
		}
	}
}

func TestBuildExecuteScriptArgsSchema_DeclaresJoinSpecificArgs(t *testing.T) {
	schema := buildExecuteScriptArgsSchema()

	var root map[string]any
	if err := json.Unmarshal([]byte(schema), &root); err != nil {
		t.Fatalf("unmarshal schema: %v", err)
	}

	rootProps, ok := root["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected root properties")
	}
	scriptProp, ok := rootProps["script"].(map[string]any)
	if !ok {
		t.Fatalf("expected script property")
	}
	items, ok := scriptProp["items"].(map[string]any)
	if !ok {
		t.Fatalf("expected script items")
	}
	itemProps, ok := items["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected step properties")
	}
	argsProp, ok := itemProps["args"].(map[string]any)
	if !ok {
		t.Fatalf("expected args property")
	}
	argsProps, ok := argsProp["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected args.properties")
	}

	for _, field := range []string{"store", "with", "target", "relation", "on", "type", "condition", "fields"} {
		if _, ok := argsProps[field]; !ok {
			t.Fatalf("expected args.properties to declare %q, got keys %#v", field, argsProps)
		}
	}

	onProp, ok := argsProps["on"].(map[string]any)
	if !ok {
		t.Fatalf("expected on property object")
	}
	desc, _ := onProp["description"].(string)
	if !strings.Contains(desc, `{"value":"key"}`) {
		t.Fatalf("expected on description to contain grounded example, got %q", desc)
	}
}

func TestExecuteScriptInstruction_MentionsResearchAndConcreteShapes(t *testing.T) {
	checks := []string{"full ordered JSON AST", "{op, args?, input_var?, result_var?}", "begin a transaction", "result_var/input_var", "list_stores", "schema=...", "relations=[...]", "join-field semantics", "prefer relation + target for join repair", "rewrite only the invalid join slice", "combined flat record by default", "gettoolinfo('execute_script')", "concrete predicate objects", "concrete join mappings"}
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

func TestStoreInstructions_MentionTransactionBehavior(t *testing.T) {
	checks := map[string]string{
		"SelectInstruction":      SelectInstruction,
		"ExplainJoinInstruction": ExplainJoinInstruction,
		"AddInstruction":         AddInstruction,
		"UpdateInstruction":      UpdateInstruction,
		"DeleteInstruction":      DeleteInstruction,
	}
	for name, instruction := range checks {
		if !strings.Contains(instruction, "explicit transaction") {
			t.Fatalf("expected %s to mention explicit transaction reuse, got %s", name, instruction)
		}
		if !strings.Contains(instruction, "local") {
			t.Fatalf("expected %s to mention local transaction fallback, got %s", name, instruction)
		}
	}
	if !strings.Contains(ManageTransactionInstruction, "explicit transaction control") {
		t.Fatalf("expected ManageTransactionInstruction to describe explicit transaction control, got %s", ManageTransactionInstruction)
	}
}

func TestStoresAndSpacesSchemas_CaptureScopedOperationalGuidance(t *testing.T) {
	storesChecks := []string{"exact store names", "keep research compact", "users_orders"}
	for _, check := range storesChecks {
		if !strings.Contains(listStoresArgsSchema, check) {
			t.Fatalf("expected listStoresArgsSchema to contain %q\nSchema: %s", check, listStoresArgsSchema)
		}
	}

	spacesSchemas := []string{
		mintToSpaceArgsSchema,
		readSpaceConfigArgsSchema,
		vectorizeSpaceArgsSchema,
		vectorizeSpaceCategoriesArgsSchema,
		vectorizeSpaceItemsArgsSchema,
	}
	spacesChecks := []string{"Exact target knowledge base name", "explicitly asks", "Prefer this over", "semantic refresh"}
	combined := strings.Join(spacesSchemas, "\n")
	for _, check := range spacesChecks {
		if !strings.Contains(combined, check) {
			t.Fatalf("expected combined Spaces schemas to contain %q\nSchemas: %s", check, combined)
		}
	}
}
