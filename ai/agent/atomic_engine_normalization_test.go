package agent

import "testing"

func TestNormalizeScriptStepForCompatibility_SortLegacyShape(t *testing.T) {
	step := map[string]any{
		"op": "sort",
		"args": map[string]any{
			"pipe": "scanned_users",
			"key":  "age",
			"desc": true,
		},
	}

	normalizeScriptStepForCompatibility(step)

	if got, _ := step["input_var"].(string); got != "scanned_users" {
		t.Fatalf("expected input_var to be normalized from pipe, got %q", got)
	}
	args := step["args"].(map[string]any)
	fields, ok := args["fields"].([]any)
	if !ok || len(fields) != 1 || fields[0] != "age desc" {
		t.Fatalf("expected sort fields to be normalized, got %#v", args["fields"])
	}
	if _, ok := args["pipe"]; ok {
		t.Fatalf("expected legacy pipe arg to be removed")
	}
	if _, ok := args["key"]; ok {
		t.Fatalf("expected legacy key arg to be removed")
	}
	if _, ok := args["desc"]; ok {
		t.Fatalf("expected legacy desc arg to be removed")
	}
}

func TestNormalizeScriptStepForCompatibility_SortFieldDescendingShape(t *testing.T) {
	step := map[string]any{
		"op": "sort",
		"args": map[string]any{
			"field":      "age",
			"descending": true,
		},
	}

	normalizeScriptStepForCompatibility(step)

	args := step["args"].(map[string]any)
	fields, ok := args["fields"].([]any)
	if !ok || len(fields) != 1 || fields[0] != "age desc" {
		t.Fatalf("expected sort fields to be normalized, got %#v", args["fields"])
	}
	if _, ok := args["field"]; ok {
		t.Fatalf("expected legacy field arg to be removed")
	}
	if _, ok := args["descending"]; ok {
		t.Fatalf("expected legacy descending arg to be removed")
	}
}

func TestNormalizeScriptStepForCompatibility_FilterStringPredicateShape(t *testing.T) {
	step := map[string]any{
		"op": "filter",
		"args": map[string]any{
			"condition": map[string]any{
				"first_name": "$eq:John",
				"orders":     "total_amount>500",
			},
		},
	}

	normalizeScriptStepForCompatibility(step)

	args := step["args"].(map[string]any)
	condition := args["condition"].(map[string]any)
	firstName := condition["first_name"].(map[string]any)
	if firstName["$eq"] != "John" {
		t.Fatalf("expected first_name eq condition, got %#v", firstName)
	}
	ordersAmount := condition["orders.total_amount"].(map[string]any)
	if ordersAmount["$gt"] != 500 {
		t.Fatalf("expected orders.total_amount gt condition, got %#v", ordersAmount)
	}
}

func TestNormalizeScriptStepForCompatibility_JoinStringOnShape(t *testing.T) {
	step := map[string]any{
		"op": "join",
		"args": map[string]any{
			"on": map[string]any{
				"users_orders": "users.key=key",
				"orders":       "users_orders.value=key",
			},
		},
	}

	normalizeScriptStepForCompatibility(step)

	args := step["args"].(map[string]any)
	on := args["on"].(map[string]any)
	if on["users.key"] != "key" {
		t.Fatalf("expected users.key join mapping, got %#v", on)
	}
	if on["users_orders.value"] != "key" {
		t.Fatalf("expected users_orders.value join mapping, got %#v", on)
	}
}

func TestNormalizeScriptStepForCompatibility_PreservesExplicitDottedFieldPaths(t *testing.T) {
	step := map[string]any{
		"op": "filter",
		"args": map[string]any{
			"condition": map[string]any{
				"orders.total_amount": map[string]any{"$gt": 500},
			},
		},
	}

	normalizeScriptStepForCompatibility(step)

	args := step["args"].(map[string]any)
	condition := args["condition"].(map[string]any)
	if _, ok := condition["orders.total_amount"]; !ok {
		t.Fatalf("expected explicit dotted condition field to be preserved, got %#v", condition)
	}
	if _, ok := condition["orders_total_amount"]; ok {
		t.Fatalf("did not expect underscore field fallback, got %#v", condition)
	}

	joinStep := map[string]any{
		"op": "join",
		"args": map[string]any{
			"on": map[string]any{"users_orders.value": "key"},
		},
	}

	normalizeScriptStepForCompatibility(joinStep)
	joinArgs := joinStep["args"].(map[string]any)
	on := joinArgs["on"].(map[string]any)
	if on["users_orders.value"] != "key" {
		t.Fatalf("expected explicit dotted join field to be preserved, got %#v", on)
	}
	if _, ok := on["users_orders_value"]; ok {
		t.Fatalf("did not expect underscore join field fallback, got %#v", on)
	}
}

func TestNormalizeScriptStepForCompatibilityWithQuery_InfersBooleanFilterPredicates(t *testing.T) {
	step := map[string]any{
		"op": "filter",
		"args": map[string]any{
			"condition": map[string]any{
				"first_name":          true,
				"orders.total_amount": true,
			},
		},
	}

	normalizeScriptStepForCompatibilityWithQuery(step, "Find orders for users with first_name 'John' with total amount > 500")

	args := step["args"].(map[string]any)
	condition := args["condition"].(map[string]any)
	firstName := condition["first_name"].(map[string]any)
	if firstName["$eq"] != "John" {
		t.Fatalf("expected first_name boolean placeholder to infer John, got %#v", firstName)
	}
	amount := condition["orders.total_amount"].(map[string]any)
	if amount["$gt"] != 500 {
		t.Fatalf("expected orders.total_amount boolean placeholder to infer > 500, got %#v", amount)
	}
	if _, ok := condition["orders_total_amount"]; ok {
		t.Fatalf("did not expect underscore amount field fallback, got %#v", condition)
	}
}

func TestNormalizeScriptStepForCompatibilityWithQuery_InfersAliasFieldPredicateFromLeafHint(t *testing.T) {
	step := map[string]any{
		"op": "filter",
		"args": map[string]any{
			"condition": map[string]any{
				"orders": "total_amount",
			},
		},
	}

	normalizeScriptStepForCompatibilityWithQuery(step, "Find orders for users with first_name 'John' with total amount > 500")

	args := step["args"].(map[string]any)
	condition := args["condition"].(map[string]any)
	amount := condition["orders.total_amount"].(map[string]any)
	if amount["$gt"] != 500 {
		t.Fatalf("expected orders.total_amount predicate inferred from query, got %#v", amount)
	}
	if _, ok := condition["orders"]; ok {
		t.Fatalf("expected alias-only field to be normalized, got %#v", condition)
	}
}

func TestNormalizeScriptStepForCompatibilityWithQuery_InfersAliasBooleanPredicateFromQuery(t *testing.T) {
	step := map[string]any{
		"op": "filter",
		"args": map[string]any{
			"condition": map[string]any{
				"orders": true,
			},
		},
	}

	normalizeScriptStepForCompatibilityWithQuery(step, "Find orders for users with first_name 'John' with total amount > 500")

	args := step["args"].(map[string]any)
	condition := args["condition"].(map[string]any)
	amount := condition["orders.total_amount"].(map[string]any)
	if amount["$gt"] != 500 {
		t.Fatalf("expected alias boolean placeholder to infer orders.total_amount > 500, got %#v", amount)
	}
	if _, ok := condition["orders"]; ok {
		t.Fatalf("expected alias boolean placeholder to be replaced, got %#v", condition)
	}
}

func TestNormalizeScriptStepForCompatibilityWithQueryAndState_QualifiesUniqueJoinedLeafField(t *testing.T) {
	state := &scriptCompatibilityNormalizerState{
		storeVars: map[string]string{
			"users_store":        "users",
			"users_orders_store": "users_orders",
			"orders_store":       "orders",
		},
		resultAliases: map[string][]string{
			"joined_orders": {"users", "users_orders", "orders"},
		},
		storeFields: map[string]map[string]struct{}{
			"users":        {"first_name": {}},
			"users_orders": {"user_id": {}},
			"orders":       {"total_amount": {}},
		},
	}

	step := map[string]any{
		"op":        "filter",
		"input_var": "joined_orders",
		"args": map[string]any{
			"condition": map[string]any{
				"total_amount": map[string]any{"$gt": 500},
			},
		},
	}

	normalizeScriptStepForCompatibilityWithQueryAndState(step, "Find orders for users with first_name 'John' with total amount > 500", state)

	args := step["args"].(map[string]any)
	condition := args["condition"].(map[string]any)
	amount := condition["orders.total_amount"].(map[string]any)
	if amount["$gt"] != 500 {
		t.Fatalf("expected orders.total_amount condition to be preserved, got %#v", amount)
	}
	if _, ok := condition["total_amount"]; ok {
		t.Fatalf("expected unprefixed total_amount to be qualified, got %#v", condition)
	}
}

func TestQualifyCompatibilityConditionField_PicksFirstMatchingStoreField(t *testing.T) {
	qualified := qualifyCompatibilityConditionField(
		"status",
		[]string{"users", "orders"},
		map[string]map[string]struct{}{
			"users":  {"status": {}},
			"orders": {"status": {}},
		},
	)
	if qualified != "users.status" {
		t.Fatalf("expected first matching store field to win, got %q", qualified)
	}
}

func TestNormalizeScriptStepForCompatibilityWithQueryAndState_CollapsesMalformedSelectPredicateObject(t *testing.T) {
	state := &scriptCompatibilityNormalizerState{
		resultAliases: map[string][]string{
			"users_store": {"users"},
		},
		storeFields: map[string]map[string]struct{}{
			"users": {"first_name": {}, "country": {}, "email": {}, "gender": {}, "key": {}, "last_name": {}},
		},
	}

	step := map[string]any{
		"op": "select",
		"args": map[string]any{
			"store": "users_store",
			"condition": map[string]any{
				"first_name": map[string]any{
					"Format":           nil,
					"age":              nil,
					"country":          "John",
					"email":            nil,
					"first_name":       nil,
					"first_name_match": nil,
					"gender":           nil,
					"key":              nil,
					"last_name":        nil,
					"value":            nil,
				},
				"first_name_match":       nil,
				"first_name_value":       nil,
				"matched_users":          nil,
				"user_first_name":        nil,
				"users_first_name":       map[string]any{"format": nil, "value": "John"},
				"users_store_first_name": nil,
			},
		},
	}

	normalizeScriptStepForCompatibilityWithQueryAndState(step, "Find orders for users with first_name 'John' with total amount > 500", state)

	condition := step["args"].(map[string]any)["condition"].(map[string]any)
	firstName := condition["first_name"].(map[string]any)
	if firstName["$eq"] != "John" {
		t.Fatalf("expected first_name predicate to collapse to eq John, got %#v", firstName)
	}
	if _, ok := condition["users.first_name"]; ok {
		t.Fatalf("did not expect secondary alias field to survive, got %#v", condition)
	}
	for _, key := range []string{"first_name_match", "first_name_value", "matched_users", "user_first_name", "users_store_first_name"} {
		if _, ok := condition[key]; ok {
			t.Fatalf("expected placeholder field %q to be dropped, got %#v", key, condition)
		}
	}
}

func TestNormalizeScriptStepForCompatibilityWithQueryAndState_CollapsesMalformedJoinedFilterPredicateObject(t *testing.T) {
	state := &scriptCompatibilityNormalizerState{
		resultAliases: map[string][]string{
			"joined_orders": {"users", "users_orders", "orders"},
		},
		storeFields: map[string]map[string]struct{}{
			"users":        {"first_name": {}},
			"users_orders": {"user_id": {}},
			"orders":       {"total_amount": {}},
		},
	}

	step := map[string]any{
		"op":        "filter",
		"input_var": "joined_orders",
		"args": map[string]any{
			"condition": map[string]any{
				"orders_total_amount": map[string]any{
					"format": nil,
					"value":  500,
				},
			},
		},
	}

	normalizeScriptStepForCompatibilityWithQueryAndState(step, "Find orders for users with first_name 'John' with total amount > 500", state)

	condition := step["args"].(map[string]any)["condition"].(map[string]any)
	amount := condition["orders.total_amount"].(map[string]any)
	if amount["$gt"] != 500 {
		t.Fatalf("expected joined malformed predicate to collapse to orders.total_amount > 500, got %#v", amount)
	}
	if _, ok := condition["orders_total_amount"]; ok {
		t.Fatalf("expected underscore key to be normalized away, got %#v", condition)
	}
}

func TestNormalizeScriptStepForCompatibility_InfersAliasJoinLeafOnShape(t *testing.T) {
	step := map[string]any{
		"op": "join",
		"args": map[string]any{
			"on": map[string]any{
				"users_orders": "value",
			},
		},
	}

	normalizeScriptStepForCompatibility(step)

	args := step["args"].(map[string]any)
	on := args["on"].(map[string]any)
	if on["users_orders.value"] != "key" {
		t.Fatalf("expected alias-only join leaf to normalize to users_orders.value=key, got %#v", on)
	}
	if _, ok := on["users_orders"]; ok {
		t.Fatalf("expected alias-only join field to be removed, got %#v", on)
	}
}

func TestPreserveLastResultOnNil(t *testing.T) {
	if !preserveLastResultOnNil("commit_tx") {
		t.Fatalf("expected commit_tx to preserve last result")
	}
	if !preserveLastResultOnNil("rollback_tx") {
		t.Fatalf("expected rollback_tx to preserve last result")
	}
	if preserveLastResultOnNil("return") {
		t.Fatalf("did not expect return to preserve last result on nil")
	}
}

func TestIsInternalScriptHandle_NilAndPlainValues(t *testing.T) {
	if isInternalScriptHandle(nil) {
		t.Fatalf("nil should not be treated as an internal handle")
	}
	if isInternalScriptHandle("plain text") {
		t.Fatalf("plain text should not be treated as an internal handle")
	}
	if isInternalScriptHandle([]any{"a", "b"}) {
		t.Fatalf("lists should not be treated as internal handles")
	}
}

func TestSanitizeScript_CapturesImplicitOutputBeforeCommit(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "begin_tx", Args: map[string]any{"database": "dev_db", "mode": "read"}, ResultVar: "tx1"},
		{Op: "open_store", Args: map[string]any{"name": "users", "transaction": "tx1"}, ResultVar: "users_store"},
		{Op: "scan", Args: map[string]any{"store": "users_store"}},
		{Op: "sort", Args: map[string]any{"fields": []any{"age desc"}}},
		{Op: "commit_tx", Args: map[string]any{"transaction": "tx1"}},
	}

	sanitized := sanitizeScript(script)
	if got := sanitized[3].ResultVar; got != "output" {
		t.Fatalf("expected last data-producing step to capture implicit output, got %q", got)
	}
}

func TestSanitizeScript_InjectsImplicitTransactionWiring(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "begin_tx", Args: map[string]any{"mode": "read"}},
		{Op: "open_store", Args: map[string]any{"name": "users"}},
		{Op: "commit_tx", Args: map[string]any{}},
	}

	sanitized := sanitizeScript(script)
	if got := sanitized[0].ResultVar; got != "tx" {
		t.Fatalf("expected implicit begin_tx result_var to normalize to tx, got %q", got)
	}
	if got, _ := sanitized[1].Args["transaction"].(string); got != "tx" {
		t.Fatalf("expected open_store to inherit implicit tx, got %#v", sanitized[1].Args)
	}
	if got, _ := sanitized[1].Args["name"].(string); got != "users" {
		t.Fatalf("expected open_store name to remain intact, got %#v", sanitized[1].Args)
	}
	if got, _ := sanitized[2].Args["transaction"].(string); got != "tx" {
		t.Fatalf("expected commit_tx to inherit implicit tx, got %#v", sanitized[2].Args)
	}
	if got := sanitized[1].ResultVar; got != "users" {
		t.Fatalf("expected open_store result_var to normalize to store name, got %q", got)
	}
}

func TestSanitizeScript_PreservesExplicitCommitBeforeReturn(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "begin_tx", Args: map[string]any{"mode": "read"}, ResultVar: "tx"},
		{Op: "open_store", Args: map[string]any{"name": "users", "transaction": "tx"}, ResultVar: "users_store"},
		{Op: "scan", Args: map[string]any{"store": "users_store"}, ResultVar: "users_cursor"},
		{Op: "filter", InputVar: "users_cursor", Args: map[string]any{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}}, ResultVar: "filtered_orders"},
		{Op: "commit_tx", Args: map[string]any{"transaction": "tx"}},
		{Op: "return", InputVar: "filtered_orders"},
	}

	sanitized := sanitizeScript(script)
	if got := sanitized[4].Op; got != "commit_tx" {
		t.Fatalf("expected explicit commit to remain before return, got %q", got)
	}
	if got := sanitized[5].Op; got != "return" {
		t.Fatalf("expected return to remain terminal op, got %q", got)
	}
}
