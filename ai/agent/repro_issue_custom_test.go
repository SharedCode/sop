package agent

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestReproProjectIssue(t *testing.T) {
	// Mock Data
	// Mimic JOIN output: Flat map with dot-prefixed keys
	userWrap := map[string]any{
		"users.first_name": "Williams",
		"users.last_name":  "O'Doherty",
		"users.age":        30,
		"users.country":    "USA",
		"users.email":      "williams@example.com",
		"users.gender":     "M",
		"users.key":        "user123",

		"orders.total_amount": 100.50,
		"orders.status":       "shipped",
		"orders.key":          "order999",
	}

	// Mock Projection Fields
	fields := []string{"users.first_name", "users.last_name", "orders.total_amount", "orders.status", "orders.key"}

	// Call renderItem (it's private, but we are in package agent)
	res := renderItem(nil, userWrap, fields)

	// Insepct Result
	om, ok := res.(*OrderedMap)
	if !ok {
		t.Fatalf("Expected *OrderedMap, got %T", res)
	}

	fmt.Printf("Keys: %v\n", om.keys)

	// Check content
	b, _ := json.MarshalIndent(om, "", "  ")
	fmt.Printf("JSON: %s\n", string(b))

	// Check for unexpected flattened artifacts
	if len(om.keys) != 5 {
		t.Errorf("Expected 5 keys, got %d: %v", len(om.keys), om.keys)
	}
}
