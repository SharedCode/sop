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
		`JSON object with stores:[{name,schema,description,relations,empty}]`,
		`Use store.schema to choose the field path and value type`,
		`Read each store.schema object for exact field names`,
		`align those researched field names with the user's criteria values`,
		`Read each store.relations entry literally`,
		`source_fields are the current-store field paths`,
		`target_store is the joined store`,
		`target_fields are the target-store join fields`,
		`Worked example:`,
		`users_orders`,
		`users.schema.first_name:string`,
		`align expression names to first_name and orders.total_amount`,
		`align literal values to string John and number 500`,
		`Example AST:`,
		`begin_tx`,
		`matched_users`,
		`filtered_orders`,
		`emit a combined flat record by default`,
		`call gettoolinfo('execute_script')`,
		`filter expects {condition: object}`,
		`{\"first_name\":{\"$eq\":\"John\"}}`,
		`{\"orders.total_amount\":{\"$gt\":500}}`,
		`match the literal type to the schema type`,
		`Do not emit booleans such as {\"first_name\":true}`,
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
	checks := []string{"full ordered JSON AST", "{op, args?, input_var?, result_var?}", "begin a transaction", "result_var/input_var", "list_stores", "JSON object with stores:[{name,schema,description,relations,empty}]", "write the condition expression the engine should execute", "align the expression field name and literal value with that exact schema", "completed expressions with the operator and literal value already assigned", "source_fields are the current-store field paths", "target_store is the joined store", "target_fields are the target-store join fields", "Worked example: for the prompt Find orders for users with first_name 'John' with total amount > 500", "stores:[\"users\",\"users_orders\",\"orders\"]", "align expression names to those exact fields", "align literal values to those exact types", "the next AST can be {\"script\":[{\"op\":\"begin_tx\"", "\"condition\":{\"first_name\":{\"$eq\":\"John\"}}", "\"condition\":{\"orders.total_amount\":{\"$gt\":500}}", "Prefer relation + target for join repair", "rewrite only the invalid join slice", "combined flat record by default", "gettoolinfo('execute_script')", "concrete predicate objects", "concrete join mappings", "boolean placeholders"}
	for _, check := range checks {
		if !strings.Contains(ExecuteScriptInstruction, check) {
			t.Fatalf("expected ExecuteScriptInstruction to contain %q\nInstruction: %s", check, ExecuteScriptInstruction)
		}
	}
	if !strings.Contains(ExecuteScriptInstruction, "durability boundary") || !strings.Contains(ExecuteScriptInstruction, "50 to 250 CRUD operations per transaction") {
		t.Fatalf("expected ExecuteScriptInstruction to carry transaction boundary and batching guidance, got %s", ExecuteScriptInstruction)
	}
}

func TestStoresAndSpacesInstructions_AreRichEnoughForToolContext(t *testing.T) {
	storesChecks := []string{"stores:[...]", "stores:[{name,schema,description,relations,empty}]", "expression name and literal value", "exact data type", "infer likely store names", "singular/plural", "Worked example: Find orders for users with first_name 'John' with total amount > 500.", "align expression names to first_name and orders.total_amount", "align literal values to string John and number 500", "{\"first_name\":{\"$eq\":\"John\"}}", "{\"orders.total_amount\":{\"$gt\":500}}"}
	for _, check := range storesChecks {
		if !strings.Contains(ListStoresInstruction, check) {
			t.Fatalf("expected ListStoresInstruction to contain %q\nInstruction: %s", check, ListStoresInstruction)
		}
	}
	for _, check := range []string{"source_fields are the current-store field paths", "target_store is the joined store", "target_fields are the target-store join fields", "compose the join AST from the returned relation fields"} {
		if !strings.Contains(ListStoresInstruction, check) {
			t.Fatalf("expected ListStoresInstruction to explain concrete relations consumption with %q\nInstruction: %s", check, ListStoresInstruction)
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
	if !strings.Contains(SelectInstruction, "native pipeline tools") {
		t.Fatalf("expected SelectInstruction to steer clear chained reads toward native pipeline tools, got %s", SelectInstruction)
	}
	if !strings.Contains(JoinInstruction, "join_right") {
		t.Fatalf("expected JoinInstruction to mention join_right-native chaining guidance, got %s", JoinInstruction)
	}
	if !strings.Contains(ManageTransactionInstruction, "explicit transaction control") {
		t.Fatalf("expected ManageTransactionInstruction to describe explicit transaction control, got %s", ManageTransactionInstruction)
	}
	if !strings.Contains(ManageTransactionInstruction, "durability boundary") || !strings.Contains(ManageTransactionInstruction, "50 to 250 CRUD operations per transaction") {
		t.Fatalf("expected ManageTransactionInstruction to carry transaction boundary and batching guidance, got %s", ManageTransactionInstruction)
	}
	if !strings.Contains(ManageTransactionInstruction, "native pipeline tools") {
		t.Fatalf("expected ManageTransactionInstruction to mention native pipeline tools, got %s", ManageTransactionInstruction)
	}
}

func TestStoresAndSpacesSchemas_CaptureScopedOperationalGuidance(t *testing.T) {
	storesChecks := []string{"keep research compact", "users_orders", "Infer likely targets from the user's ask", "Close singular/plural forms are narrowed internally"}
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
