package parser

import (
	"testing"
)

func TestParseSlashCommand_Fetch(t *testing.T) {
	tests := []struct {
		input    string
		expected map[string]any
	}{
		{
			input: "/fetch orders limit 2",
			expected: map[string]any{
				"store": "orders",
				"limit": "2",
			},
		},
		{
			input: "/fetch orders limit 1",
			expected: map[string]any{
				"store": "orders",
				"limit": "1",
			},
		},
		{
			input: "/fetch orders limit 3",
			expected: map[string]any{
				"store": "orders",
				"limit": "3",
			},
		},
		{
			input: "fetch orders limit 5",
			expected: map[string]any{
				"store": "orders",
				"limit": "5",
			},
		},
		{
			input: "/fetch orders prefix user:",
			expected: map[string]any{
				"store":  "orders",
				"prefix": "user:",
			},
		},
		{
			input: "/fetch orders limit 2 prefix u:",
			expected: map[string]any{
				"store":  "orders",
				"limit":  "2",
				"prefix": "u:",
			},
		},
	}

	for _, tt := range tests {
		name, args, err := ParseSlashCommand(tt.input)
		if err != nil {
			t.Errorf("ParseSlashCommand(%q) returned error: %v", tt.input, err)
			continue
		}
		if name != "fetch" {
			t.Errorf("ParseSlashCommand(%q) returned name %q, expected 'fetch'", tt.input, name)
		}

		// Check for presence of expected keys and values
		for k, v := range tt.expected {
			if got, ok := args[k]; !ok || got != v {
				t.Errorf("ParseSlashCommand(%q) args[%q] = %v, expected %v", tt.input, k, got, v)
			}
		}
	}
}

func TestParseSlashCommand_Others(t *testing.T) {
	// Test select
	name, args, err := ParseSlashCommand("/select users")
	if err != nil {
		t.Fatal(err)
	}
	if name != "select" {
		t.Errorf("expected select, got %s", name)
	}
	if args["store"] != "users" {
		t.Errorf("expected store=users, got %v", args["store"])
	}

	// Test limit with =
	name, args, err = ParseSlashCommand("/fetch orders limit=5")
	if err != nil {
		t.Fatal(err)
	}
	if args["limit"] != "5" {
		t.Errorf("expected limit=5 (from limit=5), got %v", args["limit"])
	}
}
