package agent

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/sharedcode/sop/jsondb"
)

func TestOrderedKeySerialization(t *testing.T) {
	// Define an index specification with a specific order
	spec := &jsondb.IndexSpecification{
		IndexFields: []jsondb.IndexFieldSpecification{
			{FieldName: "c"},
			{FieldName: "a"},
			{FieldName: "b"},
		},
	}

	// Create a map with keys in random order (Go maps are unordered)
	m := map[string]any{
		"a": 1,
		"b": 2,
		"c": 3,
	}

	// Wrap it in OrderedKey
	ok := OrderedKey{m: m, spec: spec}

	// Marshal to JSON
	b, err := json.Marshal(ok)
	if err != nil {
		t.Fatalf("Failed to marshal OrderedKey: %v", err)
	}

	// Verify the order in the JSON string
	expected := `{"c":3,"a":1,"b":2}`
	if string(b) != expected {
		t.Errorf("Expected JSON %s, got %s", expected, string(b))
	}
}

func TestJoinProcessor_OrderedKeyOutput(t *testing.T) {
	// This test simulates the behavior of JoinProcessor.emitMatch with OrderedKey

	// 1. Setup the IndexSpecification
	spec := &jsondb.IndexSpecification{
		IndexFields: []jsondb.IndexFieldSpecification{
			{FieldName: "region"},
			{FieldName: "country"},
			{FieldName: "code"},
		},
	}

	// 2. Create a sample key map
	keyMap := map[string]any{
		"code":    "US",
		"country": "USA",
		"region":  "North America",
	}

	// 3. Simulate the logic in emitMatch
	var keyFormatted any = keyMap
	if spec != nil {
		if m, ok := keyFormatted.(map[string]any); ok {
			keyFormatted = OrderedKey{m: m, spec: spec}
		}
	}

	// 4. Create the final output map
	output := map[string]any{
		"key":   keyFormatted,
		"value": "some value",
	}

	// 5. Marshal to JSON
	b, err := json.Marshal(output)
	if err != nil {
		t.Fatalf("Failed to marshal output: %v", err)
	}

	// 6. Verify the order of keys in the "key" object
	// We expect "key":{"region":"North America","country":"USA","code":"US"}
	expectedKeyPart := `"key":{"region":"North America","country":"USA","code":"US"}`
	if !bytes.Contains(b, []byte(expectedKeyPart)) {
		t.Errorf("JSON output does not contain expected ordered key.\nExpected to contain: %s\nGot: %s", expectedKeyPart, string(b))
	}
}
