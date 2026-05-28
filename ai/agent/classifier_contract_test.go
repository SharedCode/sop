package agent

import (
	"context"
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
}

func (m *continuityIntentMockGen) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	return ai.GenOutput{Text: m.response}, nil
}

func (m *continuityIntentMockGen) Name() string { return "continuity_intent_mock" }

func (m *continuityIntentMockGen) EstimateCost(inTokens, outTokens int) float64 { return 0 }

func TestClassifyContinuityTaskContext_RejectsInvalidIntent(t *testing.T) {
	ag := &CopilotAgent{}
	_, _, err := ag.ClassifyContinuityTaskContext(context.Background(), "same task", &TaskContextClassification{Entity: "Omni", Domain: StoresDomain}, &continuityIntentMockGen{response: `{"intent":"maybe","layers":[]}`})
	if err == nil {
		t.Fatal("expected invalid continuity intent to return an error")
	}
}
