package agent

import (
	"context"
	"strings"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestParseClassificationResponse_NormalizesLegacyLayersAndCRUD(t *testing.T) {
	resp, err := parseClassificationResponse(`{
		"entity": " omni ",
		"domain": "store",
		"db_artifacts": [" users "],
		"layers": [
			{"name": "Layer 1", "crud": ["r", " X ", "u"]},
			{"name": "Layer 3", "crud": ["d"]}
		]
	}`)
	if err != nil {
		t.Fatalf("parseClassificationResponse returned error: %v", err)
	}

	if resp.Entity != "Omni" {
		t.Fatalf("expected entity Omni, got %q", resp.Entity)
	}
	if resp.Domain != StoresDomain {
		t.Fatalf("expected domain %q, got %q", StoresDomain, resp.Domain)
	}
	if len(resp.StoresArtifacts) != 1 || resp.StoresArtifacts[0] != "users" {
		t.Fatalf("expected normalized stores artifacts, got %#v", resp.StoresArtifacts)
	}
	if len(resp.Layers) != 2 {
		t.Fatalf("expected 2 normalized layers, got %#v", resp.Layers)
	}
	if resp.Layers[0].Name != "Single-Domain" || len(resp.Layers[0].CRUD) != 2 || resp.Layers[0].CRUD[0] != "R" || resp.Layers[0].CRUD[1] != "U" {
		t.Fatalf("expected normalized single-domain CRUD, got %#v", resp.Layers[0])
	}
	if resp.Layers[1].Name != "Cross-Domain" || len(resp.Layers[1].CRUD) != 1 || resp.Layers[1].CRUD[0] != "D" {
		t.Fatalf("expected normalized cross-domain layer, got %#v", resp.Layers[1])
	}
}

func TestParseClassificationResponse_ExtractsJSONObjectFromReasoningPreamble(t *testing.T) {
	resp, err := parseClassificationResponse(`**Clarifying JSON schema intent**

I need to classify the user's request before returning the JSON.
{
	"entity": "Omni",
	"domain": "Stores",
	"db_artifacts": ["users"],
	"stores_artifacts": ["users"],
	"spaces_artifacts": [],
	"layers": [
		{
			"name": "Single-Domain",
			"crud": ["R"]
		}
	]
}`)
	if err != nil {
		t.Fatalf("parseClassificationResponse returned error: %v", err)
	}
	if resp.Domain != StoresDomain {
		t.Fatalf("expected domain %q, got %q", StoresDomain, resp.Domain)
	}
	if len(resp.StoresArtifacts) != 1 || resp.StoresArtifacts[0] != "users" {
		t.Fatalf("expected normalized stores artifacts, got %#v", resp.StoresArtifacts)
	}
	if len(resp.Layers) != 1 || resp.Layers[0].Name != "Single-Domain" || len(resp.Layers[0].CRUD) != 1 || resp.Layers[0].CRUD[0] != "R" {
		t.Fatalf("expected normalized single-domain read layer, got %#v", resp.Layers)
	}
}

func TestNormalizeTaskContext_InfersCrossDomainFromPerDomainArtifacts(t *testing.T) {
	taskCtx := &TaskContextClassification{
		StoresArtifacts: []string{"users"},
		SpacesArtifacts: []string{"release_notes"},
	}

	normalizeTaskContext(taskCtx)

	if taskCtx.Entity != "Omni" {
		t.Fatalf("expected default entity Omni, got %q", taskCtx.Entity)
	}
	if !hasLayer(taskCtx.Layers, "Cross-Domain") {
		t.Fatalf("expected Cross-Domain layer, got %#v", taskCtx.Layers)
	}
	if len(taskCtx.Layers) != 1 || len(taskCtx.Layers[0].CRUD) != 1 || taskCtx.Layers[0].CRUD[0] != "R" {
		t.Fatalf("expected default read-only cross-domain layer, got %#v", taskCtx.Layers)
	}
}

type continuityIntentMockGen struct {
	response string
	prompt   string
}

func (m *continuityIntentMockGen) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	m.prompt = opts.SystemPrompt
	return ai.GenOutput{Text: m.response}, nil
}

func (m *continuityIntentMockGen) Name() string { return "continuity_intent_mock" }

func (m *continuityIntentMockGen) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func (m *continuityIntentMockGen) PrewarmCache(ctx context.Context, opts ai.GenOptions) error {
	return nil
}

func TestClassifyContinuityTaskContext_RejectsInvalidIntent(t *testing.T) {
	ag := &CopilotAgent{}
	_, _, err := ag.ClassifyContinuityTaskContext(context.Background(), "same task", &TaskContextClassification{Entity: "Omni", Domain: StoresDomain}, nil, &continuityIntentMockGen{response: `{"intent":"maybe","layers":[]}`})
	if err == nil {
		t.Fatal("expected invalid continuity intent to return an error")
	}
}

func TestBuildContinuityDigest_UsesAskOutcomeSignals(t *testing.T) {
	ag := &CopilotAgent{}
	ag.service = &Service{session: &RunnerSession{MRU: []MRUItem{
		{Category: askOutcomeMRUCategoryQuery, Context: "- Last user ask: Find users named John", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryResult, Context: "- Last outcome: Found matching orders", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryToolPattern, Context: "- Tool pattern: list_stores -> execute_script", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryFilterSelection + "_first_name", Context: "- Confirmed: execute_script confirmed filter field=first_name op=$eq", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
		{Category: askOutcomeMRUCategoryGuidance, Context: "- Reuse confirmed facts and successful patterns from this outcome before broadening scope.", Source: MRUSourceAskOutcome, Scope: MRUScopeSession},
	}}}

	digest := ag.buildContinuityDigest("show me more", &TaskContextClassification{Domain: StoresDomain, DBArtifacts: []string{"users"}}, nil)

	if digest.CurrentGoal != "Find users named John" {
		t.Fatalf("expected current goal from ask outcome query, got %q", digest.CurrentGoal)
	}
	if digest.Summary != "Found matching orders" {
		t.Fatalf("expected summary from ask outcome result, got %q", digest.Summary)
	}
	if len(digest.RecentPatterns) != 1 || digest.RecentPatterns[0] != "list_stores -> execute_script" {
		t.Fatalf("expected tool pattern in digest, got %#v", digest.RecentPatterns)
	}
	if len(digest.ConfirmedFacts) != 1 || !strings.Contains(digest.ConfirmedFacts[0], "first_name") {
		t.Fatalf("expected confirmed fact in digest, got %#v", digest.ConfirmedFacts)
	}
	if len(digest.SuggestedNextMoves) != 1 || !strings.Contains(digest.SuggestedNextMoves[0], "Reuse confirmed facts") {
		t.Fatalf("expected guidance in digest, got %#v", digest.SuggestedNextMoves)
	}
	if len(digest.ActiveDomains) != 1 || digest.ActiveDomains[0] != StoresDomain {
		t.Fatalf("expected active domain in digest, got %#v", digest.ActiveDomains)
	}
	if len(digest.ActiveArtifacts) != 1 || digest.ActiveArtifacts[0] != "users" {
		t.Fatalf("expected active artifact in digest, got %#v", digest.ActiveArtifacts)
	}
	if digest.CurrentQuerySignal != "show me more" {
		t.Fatalf("expected current query signal in digest, got %q", digest.CurrentQuerySignal)
	}
}

func TestClassifyContinuityTaskContext_IncludesExplicitAnchorInPrompt(t *testing.T) {
	ag := &CopilotAgent{}
	gen := &continuityIntentMockGen{response: `{"intent":"CONTINUE","domain":"Stores","db_artifacts":["users"],"stores_artifacts":["users"],"layers":[{"name":"Single-Domain","crud":["R"]}]}`}
	_, _, err := ag.ClassifyContinuityTaskContext(
		context.Background(),
		"Omni:Stores:users show me recent orders",
		&TaskContextClassification{Entity: "Omni", Domain: StoresDomain},
		&TaskContextClassification{Entity: "Omni", Domain: StoresDomain, DBArtifacts: []string{"users"}, StoresArtifacts: []string{"users"}, Layers: []LayerInfo{{Name: "Single-Domain", CRUD: []string{"R"}}}},
		gen,
	)
	if err != nil {
		t.Fatalf("expected continuity classification to succeed, got %v", err)
	}
	if !strings.Contains(gen.prompt, "CURRENT EXPLICIT ANCHOR:") || !strings.Contains(gen.prompt, "artifacts=users") {
		t.Fatalf("expected continuity prompt to include explicit anchor, got: %s", gen.prompt)
	}
}
