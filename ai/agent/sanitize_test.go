package agent

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/sharedcode/sop/ai"
	"github.com/stretchr/testify/assert"
)

func TestSanitizeScript(t *testing.T) {
	tests := []struct {
		name     string
		input    []ScriptInstruction
		expected []ScriptInstruction
	}{
		{
			name: "Join - Default to Inner",
			input: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"dataset": "users",
					},
				},
			},
			expected: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"dataset": "users",
						"type":    "inner",
					},
				},
			},
		},
		{
			name: "Join - Normalize Type Case",
			input: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"type": "LEFT",
					},
				},
			},
			expected: []ScriptInstruction{
				{
					Op: "join",
					Args: map[string]interface{}{
						"type": "left",
					},
				},
			},
		},
		{
			name: "CommitTx - Rewrite to Defer if Cursor Produced",
			input: []ScriptInstruction{
				{Op: "scan", Args: map[string]interface{}{"store": "users"}},
				{Op: "commit_tx", Args: map[string]interface{}{"transaction": "tx"}},
			},
			expected: []ScriptInstruction{
				{Op: "scan", Args: map[string]interface{}{"store": "users"}, ResultVar: "output"},
				{
					Op: "defer",
					Args: map[string]interface{}{
						"command": map[string]interface{}{
							"op":          "commit_tx",
							"transaction": "tx",
						},
					},
				},
			},
		},
		{
			name: "Filter - Normalize Flat Args",
			input: []ScriptInstruction{
				{Op: "filter", Args: map[string]interface{}{"field": "first_name", "op": "==", "value": "John"}},
			},
			expected: []ScriptInstruction{
				{Op: "filter", Args: map[string]interface{}{"condition": map[string]any{"first_name": map[string]any{"$eq": "John"}}}},
			},
		},
		{
			name: "OpenStore - Default ResultVar To Store Name",
			input: []ScriptInstruction{
				{Op: "open_store", Args: map[string]interface{}{"name": "users", "transaction": "tx"}},
			},
			expected: []ScriptInstruction{
				{Op: "open_store", Args: map[string]interface{}{"name": "users", "transaction": "tx"}, ResultVar: "users"},
			},
		},
		{
			name: "Return - Normalize $var Wrapper",
			input: []ScriptInstruction{
				{Op: "return", Args: map[string]interface{}{"value": map[string]any{"$var": "final_orders"}}},
			},
			expected: []ScriptInstruction{
				{Op: "return", Args: map[string]interface{}{"value": "@final_orders"}},
			},
		},
		{
			name: "Join - Expand Relation Target Shape",
			input: []ScriptInstruction{
				{Op: "begin_tx", Args: map[string]interface{}{"database": "dev_db", "mode": "read"}, ResultVar: "tx"},
				{Op: "open_store", Args: map[string]interface{}{"name": "orders", "transaction": "tx"}, ResultVar: "orders_store"},
				{Op: "join", Args: map[string]interface{}{"relation": "users_orders", "target": "orders_store"}, InputVar: "users_rows", ResultVar: "joined_rows"},
			},
			expected: []ScriptInstruction{
				{Op: "begin_tx", Args: map[string]interface{}{"database": "dev_db", "mode": "read"}, ResultVar: "tx"},
				{Op: "open_store", Args: map[string]interface{}{"name": "orders", "transaction": "tx"}, ResultVar: "orders_store"},
				{Op: "open_store", Args: map[string]interface{}{"name": "users_orders", "transaction": "tx"}, ResultVar: "users_orders"},
				{Op: "join", Args: map[string]interface{}{"store": "users_orders", "type": "inner", "on": map[string]any{"key": "key"}}, InputVar: "users_rows", ResultVar: "joined_rows__bridge"},
				{Op: "join", Args: map[string]interface{}{"store": "orders_store", "type": "inner", "on": map[string]any{"value": "key"}}, InputVar: "joined_rows__bridge", ResultVar: "joined_rows"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sanitizeScript(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSanitizeScript_Project(t *testing.T) {
	// this is harder to test with strict equality because parseProjectionFields returns []ProjectionField
	// and we don't want to couple strictly to the internal struct if we can avoid it,
	// but since we are in the same package we can import the types or just check behavior.

	input := []ScriptInstruction{
		{
			Op: "project",
			Args: map[string]interface{}{
				"fields": []string{"a", "b.c"},
			},
		},
	}

	result := sanitizeScript(input)

	fields := result[0].Args["fields"]
	// expected is a slice of ProjectionField
	// But let's just assert it is NOT []string
	_, isStringSlice := fields.([]string)
	assert.False(t, isStringSlice, "Fields should be converted from []string to []ProjectionField")

	// We can trust parseProjectionFields does the right thing if we successfully called it.
}

func TestSanitizeToolCallArgs_StripsQuotedKeysAndValues(t *testing.T) {
	args := map[string]any{
		"script": []any{
			map[string]any{
				"op": "filter",
				"args": map[string]any{
					"condition": map[string]any{
						"'orders.total_amount'": map[string]any{"'$gt'": 500},
						"'first_name'":          map[string]any{"'$eq'": "'John'"},
					},
				},
			},
		},
	}

	sanitizeToolCallArgs(args, false)

	script := args["script"].([]any)
	step := script[0].(map[string]any)
	stepArgs := step["args"].(map[string]any)
	condition := stepArgs["condition"].(map[string]any)

	ordersCond, ok := condition["orders.total_amount"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, 500, ordersCond["$gt"])

	nameCond, ok := condition["first_name"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "John", nameCond["$eq"])
	_, hasQuotedKey := condition["'orders.total_amount'"]
	assert.False(t, hasQuotedKey)
}

func TestValidateExecuteScriptPlaceholders_RejectsBooleanFilterWithCategoryAndExample(t *testing.T) {
	ctx := context.WithValue(context.Background(), "session_payload", &ai.SessionPayload{CurrentUserQuery: "Find orders for users with first_name 'John' with total amount > 500"})
	script := []ScriptInstruction{
		{Op: "filter", Args: map[string]any{"condition": map[string]any{"first_name": true}}},
	}

	err := validateExecuteScriptPlaceholders(ctx, script)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute_script validation error [invalid_filter_placeholder]")
	assert.Contains(t, err.Error(), `"first_name":{"$eq":"<value>"}`)
	assert.Contains(t, err.Error(), "current query")
}

func TestValidateExecuteScriptPlaceholders_RejectsBooleanJoinWithCategoryAndExample(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "join", Args: map[string]any{"store": "users_orders", "on": map[string]any{"users.key": true}}},
	}

	err := validateExecuteScriptPlaceholders(context.Background(), script)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute_script validation error [invalid_join_on_placeholder]")
	assert.Contains(t, err.Error(), `"relation":"users_orders","target":"orders_store"`)
}

func TestValidateExecuteScriptPlaceholders_AllowsBooleanFilterForBooleanField(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "filter", Args: map[string]any{"condition": map[string]any{"active": true}}},
	}

	err := validateExecuteScriptPlaceholders(context.Background(), script)
	assert.NoError(t, err)
}

func TestValidateExecuteScriptPlaceholders_RejectsOperatorOnlyFilterPlaceholder(t *testing.T) {
	ctx := context.WithValue(context.Background(), "session_payload", &ai.SessionPayload{CurrentUserQuery: "Find orders for users with first_name 'John' with total amount > 500"})
	script := []ScriptInstruction{
		{Op: "filter", Args: map[string]any{"condition": map[string]any{"orders.total_amount": "$gt"}}},
	}

	err := validateExecuteScriptPlaceholders(ctx, script)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute_script validation error [invalid_filter_operator_placeholder]")
	assert.Contains(t, err.Error(), `"orders.total_amount":{"$gt":"<value>"}`)
}

func TestValidateExecuteScriptPlaceholders_RejectsNullFilterValuePlaceholder(t *testing.T) {
	ctx := context.WithValue(context.Background(), "session_payload", &ai.SessionPayload{CurrentUserQuery: "Find orders for users with first_name 'John' with total amount > 500"})
	script := []ScriptInstruction{
		{Op: "filter", Args: map[string]any{"condition": map[string]any{"first_name": nil}}},
	}

	err := validateExecuteScriptPlaceholders(ctx, script)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute_script validation error [invalid_filter_placeholder]")
	assert.Contains(t, err.Error(), `"first_name":{"$eq":"<value>"}`)
}

func TestValidateExecuteScriptPlaceholders_RejectsNullFilterFieldPlaceholder(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "filter", Args: map[string]any{"condition": map[string]any{"null": nil}}},
	}

	err := validateExecuteScriptPlaceholders(context.Background(), script)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute_script validation error [invalid_filter_field_placeholder]")
	assert.Contains(t, err.Error(), `"first_name":{"$eq":"<value>"}`)
}

func TestValidateExecuteScriptPlaceholders_RejectsNullJoinFieldPlaceholder(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "join_right", Args: map[string]any{"store": "users_orders", "on": map[string]any{"null": "key"}}},
	}

	err := validateExecuteScriptPlaceholders(context.Background(), script)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute_script validation error [invalid_join_on_field_placeholder]")
	assert.Contains(t, err.Error(), `"relation":"users_orders","target":"orders_store"`)
}

func TestValidateExecuteScriptPlaceholders_RejectsStringJoinValuePlaceholder(t *testing.T) {
	script := []ScriptInstruction{
		{Op: "join", Args: map[string]any{"store": "users_orders", "on": map[string]any{"users.key": "undefined"}}},
	}

	err := validateExecuteScriptPlaceholders(context.Background(), script)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "execute_script validation error [invalid_join_on_placeholder]")
	assert.Contains(t, err.Error(), `"relation":"users_orders","target":"orders_store"`)
}

func TestSanitizeScript_NormalizesLiveJohnQueryShapes(t *testing.T) {
	rawSteps := []map[string]any{
		{"op": "begin_tx", "args": map[string]any{"database": "dev_db", "mode": "read"}, "result_var": "tx"},
		{"op": "open_store", "args": map[string]any{"name": "users", "transaction": "$tx"}, "result_var": "users_store"},
		{"op": "scan", "input_var": "users_store", "result_var": "users_scan"},
		{"op": "filter", "input_var": "users_scan", "args": map[string]any{"condition": map[string]any{"first_name": "John"}}, "result_var": "john_users"},
		{"op": "join", "input_var": "john_users", "args": map[string]any{"store": "users_orders", "on": map[string]any{"key": "key"}}, "result_var": "joined_link"},
		{"op": "join", "input_var": "joined_link", "args": map[string]any{"store": "orders", "on": map[string]any{"users_orders": "value"}}, "result_var": "joined_orders"},
		{"op": "filter", "input_var": "joined_orders", "args": map[string]any{"condition": map[string]any{"orders": "total_amount"}}, "result_var": "final_result"},
		{"op": "commit_tx", "args": map[string]any{"transaction": "$tx"}},
		{"op": "return", "input_var": "final_result"},
	}

	for _, step := range rawSteps {
		normalizeScriptStepForCompatibilityWithQuery(step, "Find orders for users with first_name 'John' with total amount > 500")
	}

	bytes, err := json.Marshal(rawSteps)
	assert.NoError(t, err)

	var script []ScriptInstruction
	err = json.Unmarshal(bytes, &script)
	assert.NoError(t, err)

	sanitized := sanitizeScript(script)
	assert.Equal(t, "tx", sanitized[1].Args["transaction"])
	assert.Equal(t, map[string]any{"first_name": map[string]any{"$eq": "John"}}, sanitized[3].Args["condition"])
	assert.Equal(t, map[string]any{"users_orders.value": "key"}, sanitized[5].Args["on"])
	assert.Equal(t, map[string]any{"orders.total_amount": map[string]any{"$gt": 500.0}}, sanitized[6].Args["condition"])
	assert.Equal(t, "defer", sanitized[7].Op)
	deferredCommand := sanitized[7].Args["command"].(map[string]any)
	assert.Equal(t, "commit_tx", deferredCommand["op"])
	assert.Equal(t, "tx", deferredCommand["transaction"])
}

func TestSanitizeScript_NormalizesLiveJohnQueryBooleanAliasPlaceholders(t *testing.T) {
	rawSteps := []map[string]any{
		{"op": "begin_tx", "args": map[string]any{"database": "dev_db", "mode": "read"}},
		{"op": "open_store", "args": map[string]any{"name": "users"}, "result_var": "s_users"},
		{"op": "scan", "input_var": "s_users", "result_var": "sc_users"},
		{"op": "filter", "input_var": "sc_users", "args": map[string]any{"condition": map[string]any{"first_name": true}}, "result_var": "f_users"},
		{"op": "join", "input_var": "f_users", "args": map[string]any{"store": "users_orders", "on": map[string]any{"key": "key"}}, "result_var": "j_users_orders"},
		{"op": "join", "input_var": "j_users_orders", "args": map[string]any{"store": "orders", "on": map[string]any{"users_orders": "value"}}, "result_var": "j_orders"},
		{"op": "filter", "input_var": "j_orders", "args": map[string]any{"condition": map[string]any{"orders": true}}, "result_var": "f_orders"},
		{"op": "commit_tx", "args": map[string]any{}},
		{"op": "return", "input_var": "f_orders"},
	}

	for _, step := range rawSteps {
		normalizeScriptStepForCompatibilityWithQuery(step, "Find orders for users with first_name 'John' with total amount > 500")
	}

	bytes, err := json.Marshal(rawSteps)
	assert.NoError(t, err)

	var script []ScriptInstruction
	err = json.Unmarshal(bytes, &script)
	assert.NoError(t, err)

	sanitized := sanitizeScript(script)
	assert.Equal(t, map[string]any{"first_name": map[string]any{"$eq": "John"}}, sanitized[3].Args["condition"])
	assert.Equal(t, map[string]any{"users_orders.value": "key"}, sanitized[5].Args["on"])
	assert.Equal(t, map[string]any{"orders.total_amount": map[string]any{"$gt": 500.0}}, sanitized[6].Args["condition"])
	assert.Equal(t, "defer", sanitized[7].Op)
}

func TestToolExecuteScript_RewritesRecordedArgsToNormalizedScript(t *testing.T) {
	agent := NewCopilotAgent(Config{StubMode: true}, nil, nil)
	ctx := context.WithValue(context.Background(), "session_payload", &ai.SessionPayload{CurrentUserQuery: "Find orders for users with first_name 'John' with total amount > 500"})
	args := map[string]any{
		"script": []any{
			map[string]any{"op": "begin_tx", "args": map[string]any{"mode": "read"}},
			map[string]any{"op": "open_store", "args": map[string]any{"name": "users"}},
			map[string]any{"op": "filter", "args": map[string]any{"condition": map[string]any{"first_name": true}}},
			map[string]any{"op": "join", "args": map[string]any{"store": "users_orders", "on": map[string]any{"key": "key"}}},
			map[string]any{"op": "join", "args": map[string]any{"store": "orders", "on": map[string]any{"value": "key"}}},
			map[string]any{"op": "filter", "args": map[string]any{"condition": map[string]any{"orders": true}}},
			map[string]any{"op": "commit_tx"},
		},
	}

	_, err := agent.toolExecuteScript(ctx, args)
	assert.NoError(t, err)

	script, ok := args["script"].([]any)
	assert.True(t, ok, "expected normalized script to be written back as []any")
	if !ok {
		return
	}

	filterUsers := script[2].(map[string]any)
	userCondition := filterUsers["args"].(map[string]any)["condition"].(map[string]any)
	assert.Equal(t, map[string]any{"first_name": map[string]any{"$eq": "John"}}, userCondition)

	joinOrders := script[4].(map[string]any)
	joinOn := joinOrders["args"].(map[string]any)["on"].(map[string]any)
	assert.Equal(t, map[string]any{"value": "key"}, joinOn)

	filterOrders := script[5].(map[string]any)
	orderCondition := filterOrders["args"].(map[string]any)["condition"].(map[string]any)
	assert.Equal(t, map[string]any{"orders.total_amount": map[string]any{"$gt": 500.0}}, orderCondition)

	commit := script[6].(map[string]any)
	assert.Equal(t, "defer", commit["op"])
	deferred := commit["args"].(map[string]any)["command"].(map[string]any)
	assert.Equal(t, "commit_tx", deferred["op"])
	assert.Equal(t, "tx", deferred["transaction"])
}
