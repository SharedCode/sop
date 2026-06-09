package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

type gate1MockGen struct {
	CapturedPrompt string
	Response       string
}

func (m *gate1MockGen) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.CapturedPrompt = opts.SystemPrompt
	return ai.GenOutput{Text: m.Response}, nil
}
func (m *gate1MockGen) Name() string                                 { return "gate1_mock" }
func (m *gate1MockGen) EstimateCost(inTokens, outTokens int) float64 { return 0.0 }

func (m *gate1MockGen) PrewarmCache(ctx context.Context, opts ai.GenOptions) error {
	return nil
}

func TestGate1_PartialPrefixHandling(t *testing.T) {
	ctx := context.Background()
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}}
	sysDB := database.NewDatabase(sysDBOptions)

	// Seed some sample data so the classifier can pull it
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	sysDB.NewBtree(ctx, "TestStoreAlpha", tx)
	sysDB.NewBtree(ctx, "TestStoreBeta", tx)
	tx.Commit(ctx)

	cfg := Config{}
	ag := NewCopilotAgent(cfg, map[string]sop.DatabaseOptions{}, sysDB)

	// Create mock string
	expectedResponse := `{"entity": "Omni", "domain": "Stores", "db_artifacts": ["TestStoreAlpha"], "layers": []}`

	tests := []struct {
		name           string
		query          string
		expectedPrompt []string // strings that must be present in the prompt
	}{
		{
			name:  "Missing Domain And Artifact",
			query: "Omni: ", // Empty right side
			expectedPrompt: []string{
				"Available Artifact Samples",
				"Stores:",
				"TestStoreAlpha",
			},
		},
		{
			name:  "Missing Artifact Only",
			query: "Omni:Stores:",
			expectedPrompt: []string{
				"Available Artifact Samples",
				"TestStoreAlpha",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mock := &gate1MockGen{Response: expectedResponse}

			parts := strings.Split(tc.query, ":")
			domain := ""
			if len(parts) > 1 {
				domain = strings.TrimSpace(parts[1])
			}

			_, err := ag.ClassifyFocusedTaskContext(ctx, tc.query, "Omni", domain, "", mock)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			for _, exp := range tc.expectedPrompt {
				if !strings.Contains(mock.CapturedPrompt, exp) {
					t.Errorf("Expected prompt to contain '%s', but got: %s", exp, mock.CapturedPrompt)
				}
			}
		})
	}
}

func TestClassifyFocusedTaskContext_EnforcesExplicitConstraints(t *testing.T) {
	ctx := context.Background()
	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, nil)
	mock := &gate1MockGen{Response: `{"entity":"Other","domain":"Spaces","db_artifacts":["kb_docs"],"stores_artifacts":[],"spaces_artifacts":["kb_docs"],"layers":[{"name":"Cross-Domain","crud":["C","R"]}]}`}

	taskCtx, err := ag.ClassifyFocusedTaskContext(ctx, "Omni:Stores:users", "Omni", "Stores", "users", mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if taskCtx.Entity != "Omni" {
		t.Fatalf("expected entity Omni, got %q", taskCtx.Entity)
	}
	if taskCtx.Domain != StoresDomain {
		t.Fatalf("expected forced domain %q, got %q", StoresDomain, taskCtx.Domain)
	}
	if len(taskCtx.DBArtifacts) != 1 || taskCtx.DBArtifacts[0] != "users" {
		t.Fatalf("expected forced db artifact users, got %#v", taskCtx.DBArtifacts)
	}
	if len(taskCtx.StoresArtifacts) != 1 || taskCtx.StoresArtifacts[0] != "users" {
		t.Fatalf("expected forced stores artifact users, got %#v", taskCtx.StoresArtifacts)
	}
	if len(taskCtx.SpacesArtifacts) != 0 {
		t.Fatalf("expected spaces artifacts to be cleared by forced Stores constraint, got %#v", taskCtx.SpacesArtifacts)
	}
	if hasLayer(taskCtx.Layers, "Cross-Domain") {
		t.Fatalf("expected Cross-Domain layer to be removed under explicit single-domain constraint, got %#v", taskCtx.Layers)
	}
}

func TestClassifyFocusedTaskContext_PromptEncouragesStoreNameParsing(t *testing.T) {
	ctx := context.Background()
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}}
	sysDB := database.NewDatabase(sysDBOptions)
	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	sysDB.NewBtree(ctx, "users", tx)
	sysDB.NewBtree(ctx, "orders", tx)
	tx.Commit(ctx)

	ag := NewCopilotAgent(Config{}, map[string]sop.DatabaseOptions{}, sysDB)
	mock := &gate1MockGen{Response: `{"entity":"Omni","domain":"Stores","db_artifacts":["orders"],"stores_artifacts":["orders"],"spaces_artifacts":[],"layers":[]}`}

	if _, err := ag.ClassifyFocusedTaskContext(ctx, "Omni:Stores:", "Omni", "Stores", "", mock); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(mock.CapturedPrompt, "parse likely store names from the user's query") {
		t.Fatalf("expected focused classifier prompt to encourage store-name parsing, got: %s", mock.CapturedPrompt)
	}
	if !strings.Contains(mock.CapturedPrompt, "singular/plural variants") {
		t.Fatalf("expected focused classifier prompt to mention singular/plural matching, got: %s", mock.CapturedPrompt)
	}
}
