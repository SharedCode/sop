package ai

import "testing"

func TestExtractListStoresFacts_FromStructuredPayload(t *testing.T) {
	raw := `{"database":"sales","stores":[{"name":"users","schema":{"key":"string","first_name":"string"},"relations":[{"source_fields":["key"],"target_store":"users_orders","target_fields":["user_id"]}]},{"name":"orders","schema":{"total_amount":"number"}}]}`
	facts := extractListStoresFacts(raw)
	if len(facts) != 3 {
		t.Fatalf("expected 3 facts, got %+v", facts)
	}
	if facts[0] != "list_stores confirmed users schema={first_name: string, key: string}" {
		t.Fatalf("unexpected first fact: %q", facts[0])
	}
	if facts[1] != "list_stores confirmed users relations=[users_orders(user_id->key)]" {
		t.Fatalf("unexpected relation fact: %q", facts[1])
	}
	if facts[2] != "list_stores confirmed orders schema={total_amount: number}" {
		t.Fatalf("unexpected orders fact: %q", facts[2])
	}
}

func TestExtractListStoresFacts_FromEnvelopePayload(t *testing.T) {
	raw := `{"tool_result":{"database":"sales","stores":[{"name":"users","schema":{"first_name":"string"}}]},"progress_hint":{"status":"progressing"}}`
	facts := extractListStoresFacts(raw)
	if len(facts) != 1 || facts[0] != "list_stores confirmed users schema={first_name: string}" {
		t.Fatalf("unexpected facts from envelope: %+v", facts)
	}
}
