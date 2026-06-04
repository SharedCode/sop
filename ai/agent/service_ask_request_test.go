package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/ai"
)

// TestAskWithRequest_BasicFlow tests the new explicit parameter API
func TestAskWithRequest_BasicFlow(t *testing.T) {
	// This is a minimal smoke test to verify the new API compiles and runs
	// A real test would need proper setup with domain, database, generator, etc.

	// Skip for now as it requires full infrastructure setup
	t.Skip("Smoke test for new API - requires full infrastructure")

	// Example of how the new API would be used:
	req := AskRequest{
		Query: "test query",
		Session: &ai.SessionPayload{
			CurrentDB: "test_db",
		},
		Verbose: false,
	}

	// This demonstrates the explicit parameter pattern
	_ = req
}

// TestAskRequest_AllFieldsInitialized demonstrates all available fields
func TestAskRequest_AllFieldsInitialized(t *testing.T) {
	ctx := context.Background()

	req := AskRequest{
		Query:          "Find all users",
		Session:        &ai.SessionPayload{CurrentDB: "main"},
		Executor:       nil, // Will be auto-initialized
		Generator:      nil, // Will use service default
		Database:       nil, // Will be derived from session
		Writer:         nil,
		EventStreamer:  nil,
		ProgressSink:   nil,
		ScriptRecorder: nil, // Will use service default
		DefaultFormat:  "json",
		Options:        ai.NewConfigMap(),
		Verbose:        true,
	}

	// Verify struct is properly initialized
	if req.Query != "Find all users" {
		t.Errorf("Expected query 'Find all users', got %q", req.Query)
	}

	if req.Session == nil {
		t.Error("Session should not be nil")
	}

	if req.Session.CurrentDB != "main" {
		t.Errorf("Expected CurrentDB 'main', got %q", req.Session.CurrentDB)
	}

	// Use ctx to avoid unused variable warning
	_ = ctx
}

// TestAskResponse_Structure verifies the response structure
func TestAskResponse_Structure(t *testing.T) {
	resp := AskResponse{
		FinalText:      "User: John Doe",
		UpdatedSession: &ai.SessionPayload{CurrentDB: "main"},
		CarryoverState: &ai.CarryoverState{
			Mode:     ai.CarryoverModeCompact,
			Provider: "gemini",
		},
		ToolCalls: []ai.ToolCall{
			{Name: "execute_script", Args: map[string]any{"script": []any{}}},
		},
		OutcomeFacts:   []string{"Found 1 user"},
		OutcomeRecipes: []ai.LearnedRecipe{},
	}

	// Verify response structure
	if resp.FinalText != "User: John Doe" {
		t.Errorf("Expected FinalText 'User: John Doe', got %q", resp.FinalText)
	}

	if resp.UpdatedSession == nil {
		t.Error("UpdatedSession should not be nil")
	}

	if resp.CarryoverState == nil {
		t.Error("CarryoverState should not be nil")
	}

	if len(resp.ToolCalls) != 1 {
		t.Errorf("Expected 1 tool call, got %d", len(resp.ToolCalls))
	}

	if len(resp.OutcomeFacts) != 1 {
		t.Errorf("Expected 1 outcome fact, got %d", len(resp.OutcomeFacts))
	}
}
