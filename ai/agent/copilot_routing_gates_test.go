package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

type RouterTestGen struct {
	Response       string
	CapturedPrompt string
	Gate2IsSwitch  bool // if true, fake a switch boolean output
}

func (m *RouterTestGen) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.CapturedPrompt = opts.SystemPrompt

	// If it's Gate 2 Continuity
	if strings.Contains(opts.SystemPrompt, "The user is potentially continuing a previous topic") || strings.Contains(opts.SystemPrompt, "The user is continuing") {
		if m.Gate2IsSwitch {
			return ai.GenOutput{Text: `{"intent": "SWITCH", "layers": [{"name": "Single-Domain"}]}`}, nil
		}
		// otherwise return updated context via JSON
		return ai.GenOutput{Text: `{"intent": "CONTINUE", "domain": "TestInherited", "db_artifacts": ["Users"], "stores_artifacts": ["Users"], "layers": [{"name": "Single-Domain", "crud": ["R"]}]}`}, nil
	}

	return ai.GenOutput{Text: m.Response}, nil
}
func (m *RouterTestGen) Name() string                                 { return "RouterTestGen" }
func (m *RouterTestGen) EstimateCost(inTokens, outTokens int) float64 { return 0.0 }

func TestThreeGates_RoutingArchitecture(t *testing.T) {
	ctx := context.Background()
	sysDBOptions := sop.DatabaseOptions{Type: sop.Standalone, StoresFolders: []string{t.TempDir()}}
	sysDB := database.NewDatabase(sysDBOptions)

	tx, _ := sysDB.BeginTransaction(ctx, sop.ForWriting)
	sysDB.NewBtree(ctx, "TestStore", tx)
	tx.Commit(ctx)

	cfg := Config{}
	ag := NewCopilotAgent(cfg, map[string]sop.DatabaseOptions{}, sysDB)

	if ag.service == nil {
		ag.service = &Service{}
	}
	ag.service.session = &RunnerSession{
		MRU: []MRUItem{},
	}

	t.Run("Gate 1: Prefix Routing", func(t *testing.T) {
		gen := &RouterTestGen{
			Response: `{"entity": "Omni", "domain": "Stores", "db_artifacts": ["TestStore"], "layers": []}`,
		}
		payload := &ai.SessionPayload{Variables: make(map[string]any)}
		ctx = context.WithValue(ctx, "session_payload", payload)

		query := "Omni:Stores:TestStore"
		taskCtx := ag.evaluateRoutingGates(ctx, query, gen)

		if taskCtx.Domain != "Stores" {
			t.Errorf("Gate 1 failed to set Domain to Stores, got %s", taskCtx.Domain)
		}
		if len(taskCtx.DBArtifacts) == 0 || taskCtx.DBArtifacts[0] != "TestStore" {
			t.Errorf("Gate 1 failed to parse Artifact TestStore, got %v", taskCtx.DBArtifacts)
		}
		if len(taskCtx.StoresArtifacts) == 0 || taskCtx.StoresArtifacts[0] != "TestStore" {
			t.Errorf("Gate 1 fallback should seed StoresArtifacts, got %v", taskCtx.StoresArtifacts)
		}
		if len(taskCtx.Layers) != 1 || taskCtx.Layers[0].Name != "Single-Domain" {
			t.Errorf("Gate 1 fallback should synthesize Single-Domain layers, got %v", taskCtx.Layers)
		}
		if len(taskCtx.Layers) == 1 && (len(taskCtx.Layers[0].CRUD) != 1 || taskCtx.Layers[0].CRUD[0] != "R") {
			t.Errorf("Gate 1 fallback should default to read CRUD, got %v", taskCtx.Layers)
		}
		if taskCtx.RoutingGate != RoutingGateFocused {
			t.Errorf("Gate 1 should mark focused routing gate, got %q", taskCtx.RoutingGate)
		}

		// Verify MRU Session context got updated
		rs, ok := payload.Variables["RoutingState"].(*TaskContextClassification)
		if !ok || rs == nil || rs.Domain != "Stores" {
			t.Errorf("Gate 1 failed to persist RoutingState into payload")
		}
	})

	t.Run("Gate 2: MRU Context Inheritance", func(t *testing.T) {
		gen := &RouterTestGen{}
		ag.service.session.MRU = []MRUItem{
			{Category: askOutcomeMRUCategoryQuery, Context: "- Last user ask: Find John orders", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
			{Category: askOutcomeMRUCategoryToolPattern, Context: "- Tool pattern: list_stores -> execute_script", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		}
		// Seed payload with existing context
		payload := &ai.SessionPayload{Variables: make(map[string]any)}
		payload.Variables["RoutingState"] = &TaskContextClassification{
			Entity: "Omni",
			Domain: "OldDomain",
		}
		ctx = context.WithValue(ctx, "session_payload", payload)

		query := "Keep going but use TestInherited"
		taskCtx := ag.evaluateRoutingGates(ctx, query, gen)

		if taskCtx.Domain != "TestInherited" {
			t.Errorf("Gate 2 failed to inherit and update context, got %v", taskCtx)
		}
		if len(taskCtx.DBArtifacts) != 1 || taskCtx.DBArtifacts[0] != "Users" {
			t.Errorf("Gate 2 failed to update artifacts, got %v", taskCtx.DBArtifacts)
		}
		if len(taskCtx.StoresArtifacts) != 1 || taskCtx.StoresArtifacts[0] != "Users" {
			t.Errorf("Gate 2 failed to update stores artifacts, got %v", taskCtx.StoresArtifacts)
		}
		if taskCtx.RoutingGate != RoutingGateContinuity {
			t.Errorf("Gate 2 should mark continuity routing gate, got %q", taskCtx.RoutingGate)
		}
		if !strings.Contains(gen.CapturedPrompt, "CONTINUITY DIGEST:") {
			t.Fatalf("expected Gate 2 prompt to include continuity digest, got: %s", gen.CapturedPrompt)
		}
		if !strings.Contains(gen.CapturedPrompt, "Find John orders") || !strings.Contains(gen.CapturedPrompt, "list_stores -") || !strings.Contains(gen.CapturedPrompt, "execute_script") {
			t.Fatalf("expected Gate 2 prompt to include MRU-derived digest signals, got: %s", gen.CapturedPrompt)
		}
	})

	t.Run("Gate 2: Topic Switch Fallthrough to Gate 3", func(t *testing.T) {
		gen := &RouterTestGen{
			Gate2IsSwitch: true,                                                                       // Will output SWITCH
			Response:      `{"entity": "Omni", "domain": "NewApp", "db_artifacts": [], "layers": []}`, // Gate 3 response
		}

		payload := &ai.SessionPayload{Variables: make(map[string]any)}
		payload.Variables["RoutingState"] = &TaskContextClassification{
			Entity: "Omni",
			Domain: "OldDomain",
		}
		ctx = context.WithValue(ctx, "session_payload", payload)
		ag.markMRUCategoryWithSource(SYSTEM_TOOLS, "stale tools", MRUSourceSystemTools)
		ag.markMRUCategoryWithSource(playbookMRUCategory("Spaces"), "stale spaces playbook", MRUSourcePlaybook)
		ag.markMRUCategoryWithSource(playbookMRUCategory("sop"), "stale kb context", MRUSourcePlaybook)
		ag.markMRUCategoryWithSource("PERSONA_omni", "durable persona", MRUSourcePersona)

		query := "Nevermind, let's create a New App"
		taskCtx := ag.evaluateRoutingGates(ctx, query, gen)

		if taskCtx.Domain != "NewApp" {
			t.Errorf("Gate 2 should have fallen through to Gate 3 and returned NewApp, got %v", taskCtx.Domain)
		}
		if _, ok := payload.Variables["RoutingState"].(*TaskContextClassification); !ok || payload.Variables["RoutingState"].(*TaskContextClassification).Domain != "NewApp" {
			t.Errorf("Gate 2 switch should replace stale routing state, got %v", payload.Variables["RoutingState"])
		}
		if toolsCtx := ag.getSystemToolsContext(ctx); toolsCtx != "" {
			t.Errorf("Gate 2 switch should clear stale tool context when Gate 3 has no domain tools, got %q", toolsCtx)
		}

		for _, item := range ag.getMRUSnapshot() {
			if item.Category == playbookMRUCategory("Spaces") || item.Category == playbookMRUCategory("sop") || item.Category == SYSTEM_TOOLS {
				t.Errorf("Gate 2 switch should clear conversational MRU entries, found %+v", item)
			}
		}
		if persona, ok := ag.GetMRUCategory("PERSONA_omni"); !ok || persona != "durable persona" {
			t.Errorf("Gate 2 switch should preserve persona MRU cache, got %q present=%v", persona, ok)
		}
	})

	t.Run("Gate 3: Cold Start", func(t *testing.T) {
		gen := &RouterTestGen{
			Response: `{"entity": "Omni", "domain": "Discovery", "db_artifacts": [], "layers": []}`,
		}
		// Empty payload
		payload := &ai.SessionPayload{Variables: make(map[string]any)}
		ctx = context.WithValue(ctx, "session_payload", payload)

		query := "Cold start query"
		taskCtx := ag.evaluateRoutingGates(ctx, query, gen)

		if taskCtx.Domain != "Discovery" {
			t.Errorf("Gate 3 failed to cold start, got %v", taskCtx.Domain)
		}
		if taskCtx.RoutingGate != RoutingGateDiscovery {
			t.Errorf("Gate 3 should mark discovery routing gate, got %q", taskCtx.RoutingGate)
		}
	})

	t.Run("Gate 2: Rehydrates Routing State From STM", func(t *testing.T) {
		gen := &RouterTestGen{}
		payload := &ai.SessionPayload{Variables: make(map[string]any)}
		ctx = context.WithValue(ctx, "session_payload", payload)
		ag.service.session.Memory = NewShortTermMemory()
		ag.service.session.Memory.SetRoutingState(&TaskContextClassification{
			Entity:      "Omni",
			Domain:      StoresDomain,
			DBArtifacts: []string{"users"},
			Layers:      []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}},
		})

		taskCtx := ag.evaluateRoutingGates(ctx, "show users again", gen)
		if taskCtx == nil || taskCtx.RoutingGate != RoutingGateContinuity {
			t.Fatalf("expected continuity routing gate after STM rehydration, got %+v", taskCtx)
		}
		if rs, ok := payload.Variables["RoutingState"].(*TaskContextClassification); !ok || rs == nil || rs.Domain != "TestInherited" {
			t.Fatalf("expected routing state to be restored and updated through Gate 2, got %v", payload.Variables["RoutingState"])
		}
	})
}
