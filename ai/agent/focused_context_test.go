package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
)

func TestBuildSystemPrompt_IncludesFocusedStoreContext(t *testing.T) {
	ctx := context.Background()
	dbDir := t.TempDir()
	dbOpts := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{dbDir}}
	appDB := database.NewDatabase(dbOpts)
	sysDB := database.NewDatabase(sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}})

	tx, err := appDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}

	usersOpts := sop.StoreOptions{
		Name:           "users",
		SlotLength:     10,
		IsPrimitiveKey: true,
		Relations: []sop.Relation{{
			SourceFields: []string{"key"},
			TargetStore:  "orders",
			TargetFields: []string{"user_id"},
		}},
	}
	users, err := sopdb.NewBtree[string, any](ctx, dbOpts, "users", tx, nil, usersOpts)
	if err != nil {
		t.Fatalf("create users store: %v", err)
	}
	orders, err := sopdb.NewBtree[string, any](ctx, dbOpts, "orders", tx, nil, sop.StoreOptions{Name: "orders", SlotLength: 10, IsPrimitiveKey: true})
	if err != nil {
		t.Fatalf("create orders store: %v", err)
	}
	if ok, err := users.Add(ctx, "u1", map[string]any{"user_id": "u1", "first_name": "John"}); err != nil || !ok {
		t.Fatalf("add users record: %v", err)
	}
	if ok, err := orders.Add(ctx, "o1", map[string]any{"user_id": "u1", "total_amount": 600}); err != nil || !ok {
		t.Fatalf("add orders record: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit setup: %v", err)
	}

	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{"appdb": dbOpts}, sysDB)
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{}}}

	payload := &ai.SessionPayload{CurrentDB: "appdb", Variables: make(map[string]any)}
	ctx = context.WithValue(ctx, "session_payload", payload)

	prompt := ag.buildSystemPrompt(ctx, "Find users", TaskContextClassification{
		Domain:      StoresDomain,
		DBArtifacts: []string{"users"},
		Layers: []LayerInfo{{
			Name: "Single-Domain",
			CRUD: []string{"R"},
		}},
	})

	if !strings.Contains(prompt, "focused_execution_context") {
		t.Fatalf("expected focused execution context component in prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "Target Stores") || !strings.Contains(prompt, "- users") {
		t.Fatalf("expected focused users store details in prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "Relations:") || !strings.Contains(prompt, "orders([user_id])") {
		t.Fatalf("expected relations metadata for users store: %s", prompt)
	}
	if !strings.Contains(prompt, "R = Read") || !strings.Contains(prompt, "begin_tx(mode=read)") {
		t.Fatalf("expected read-only store operations in prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "use the active Current Database name from context") {
		t.Fatalf("expected focused prompt to teach open_db database selection from active context: %s", prompt)
	}
	if strings.Contains(prompt, "Available Stores:") {
		t.Fatalf("expected no generic all-store schema dump when a target artifact is classified: %s", prompt)
	}
}

func TestBuildSystemPrompt_IncludesCrossDomainFocusedContext(t *testing.T) {
	ctx := context.Background()
	sysDB := database.NewDatabase(sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}})
	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{}}}

	payload := &ai.SessionPayload{Variables: make(map[string]any)}
	ctx = context.WithValue(ctx, "session_payload", payload)

	prompt := ag.buildSystemPrompt(ctx, "Search release notes and inspect users", TaskContextClassification{
		Domain:          StoresDomain,
		StoresArtifacts: []string{"users"},
		SpacesArtifacts: []string{"release_notes"},
		Layers: []LayerInfo{{
			Name: "Cross-Domain",
			CRUD: []string{"R"},
		}},
	})

	if !strings.Contains(prompt, "Stores Execution Context") {
		t.Fatalf("expected stores cross-domain execution context in prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "Spaces Execution Context") {
		t.Fatalf("expected spaces cross-domain execution context in prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "Target Stores") || !strings.Contains(prompt, "users") {
		t.Fatalf("expected stores targets in cross-domain prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "Target Spaces: release_notes") {
		t.Fatalf("expected spaces targets in cross-domain prompt: %s", prompt)
	}
}

func TestBuildSystemPrompt_IncludesScriptAuthoringContextForStores(t *testing.T) {
	ctx := context.Background()
	sysDB := database.NewDatabase(sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}})
	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{}}}

	payload := &ai.SessionPayload{CurrentDB: SystemDBName, Variables: make(map[string]any)}
	ctx = context.WithValue(ctx, "session_payload", payload)

	prompt := ag.buildSystemPrompt(ctx, "Create a script named expensive_orders to find orders over 1000", TaskContextClassification{
		Domain:          StoresDomain,
		DBArtifacts:     []string{"orders"},
		Layers:          []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
		ScriptAuthoring: true,
	})

	if !strings.Contains(prompt, "Relevant Script Authoring Operations") {
		t.Fatalf("expected focused execution context to include script authoring guidance: %s", prompt)
	}
	if !strings.Contains(prompt, "Structured Context: Script Authoring Tools") {
		t.Fatalf("expected system tools to include script authoring manual: %s", prompt)
	}
	if !strings.Contains(prompt, "Use create_script for a new named reusable script") {
		t.Fatalf("expected create_script guidance in prompt: %s", prompt)
	}
	if !strings.Contains(prompt, "Provide reusable script steps under the `script` field") {
		t.Fatalf("expected script payload shape guidance in prompt: %s", prompt)
	}
}

func TestBuildSpacesCRUDOperationsContext_DoesNotRequireVectorizationByDefault(t *testing.T) {
	ctx := buildSpacesCRUDOperationsContext(map[string]bool{"C": true, "U": true})

	if !strings.Contains(ctx, "Use mint_to_space to add generated content") {
		t.Fatalf("expected create guidance to route generated space writes through mint_to_space: %s", ctx)
	}
	if !strings.Contains(ctx, "do not attempt an external import workflow") {
		t.Fatalf("expected create guidance to avoid import-style tool calls for generated content: %s", ctx)
	}
	if !strings.Contains(ctx, "pass that exact name in mint_to_space.kb_name") {
		t.Fatalf("expected create guidance to require explicit kb_name for user-named Spaces: %s", ctx)
	}
	if !strings.Contains(ctx, "Do NOT call vectorize_space_items") {
		t.Fatalf("expected create guidance to avoid default vectorization: %s", ctx)
	}
	if !strings.Contains(ctx, "mint_to_space manages its own transaction internally") {
		t.Fatalf("expected update guidance to describe mint_to_space transaction handling: %s", ctx)
	}
	if !strings.Contains(ctx, "Do NOT call vectorization APIs unless the user explicitly asks") {
		t.Fatalf("expected update guidance to avoid default vectorization: %s", ctx)
	}
}

func TestBuildSpacesCRUDOperationsContext_DeleteUsesDeleteSpace(t *testing.T) {
	ctx := buildSpacesCRUDOperationsContext(map[string]bool{"D": true})

	if !strings.Contains(ctx, "Use delete_space for full Space deletion") {
		t.Fatalf("expected delete guidance to expose delete_space: %s", ctx)
	}
	if !strings.Contains(ctx, "Prefer a confirmation step with the user") {
		t.Fatalf("expected delete guidance to require confirmation wording: %s", ctx)
	}
}
