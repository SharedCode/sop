package agent

import "testing"

func TestOpenDB_AcceptsDatabaseAlias(t *testing.T) {
	engine := NewScriptEngine(NewScriptContext(), func(name string) (Database, error) {
		if name != "tasks_db" {
			t.Fatalf("expected tasks_db, got %q", name)
		}
		return nil, nil
	})

	if _, err := engine.OpenDB(map[string]any{"database": "tasks_db"}); err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
}

func TestOpenDB_DefaultsToCurrent(t *testing.T) {
	engine := NewScriptEngine(NewScriptContext(), func(name string) (Database, error) {
		if name != "current" {
			t.Fatalf("expected current fallback, got %q", name)
		}
		return nil, nil
	})

	if _, err := engine.OpenDB(map[string]any{}); err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
}
