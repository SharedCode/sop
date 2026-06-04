package generator

import (
	"testing"

	"github.com/sharedcode/sop/ai"
)

// TestGeminiOptimizations_ThinkingConfig verifies that thinking level is properly configured.
func TestGeminiOptimizations_ThinkingConfig(t *testing.T) {
	tests := []struct {
		name         string
		opts         ai.GenOptions
		wantThinking bool
		wantLevel    string
	}{
		{
			name: "explicit low thinking level",
			opts: ai.GenOptions{
				ThinkingLevel: "low",
			},
			wantThinking: true,
			wantLevel:    "low",
		},
		{
			name: "explicit medium thinking level",
			opts: ai.GenOptions{
				ThinkingLevel: "medium",
			},
			wantThinking: true,
			wantLevel:    "medium",
		},
		{
			name: "explicit high thinking level",
			opts: ai.GenOptions{
				ThinkingLevel: "high",
			},
			wantThinking: true,
			wantLevel:    "high",
		},
		{
			name: "auto-detect medium with tools",
			opts: ai.GenOptions{
				Tools: []ai.ToolDefinition{
					{Name: "query_store", Description: "Query data"},
				},
			},
			wantThinking: true,
			wantLevel:    "medium",
		},
		{
			name:         "no thinking level when no tools and no explicit setting",
			opts:         ai.GenOptions{},
			wantThinking: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := buildGeminiRequest("test query", tt.opts)

			if tt.wantThinking {
				if req.GenerationConfig == nil {
					t.Fatal("expected GenerationConfig to be set")
				}
				if req.GenerationConfig.ThinkingConfig == nil {
					t.Fatal("expected ThinkingConfig to be set")
				}
				if req.GenerationConfig.ThinkingConfig.ThinkingLevel != tt.wantLevel {
					t.Errorf("ThinkingLevel = %q, want %q",
						req.GenerationConfig.ThinkingConfig.ThinkingLevel, tt.wantLevel)
				}
			} else if req.GenerationConfig != nil && req.GenerationConfig.ThinkingConfig != nil {
				t.Errorf("expected no ThinkingConfig, got level %q",
					req.GenerationConfig.ThinkingConfig.ThinkingLevel)
			}
		})
	}
}

// TestGeminiOptimizations_ResponseSchema verifies that response schema is properly configured.
func TestGeminiOptimizations_ResponseSchema(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"answer": map[string]any{
				"type": "string",
			},
			"confidence": map[string]any{
				"type": "number",
			},
		},
		"required": []any{"answer"},
	}

	opts := ai.GenOptions{
		ResponseSchema: schema,
	}

	req := buildGeminiRequest("test query", opts)

	if req.GenerationConfig == nil {
		t.Fatal("expected GenerationConfig to be set")
	}
	if req.GenerationConfig.ResponseSchema == nil {
		t.Fatal("expected ResponseSchema to be set")
	}
	if req.GenerationConfig.ResponseSchema["type"] != "object" {
		t.Errorf("ResponseSchema type = %q, want %q",
			req.GenerationConfig.ResponseSchema["type"], "object")
	}
}

// TestGeminiOptimizations_Combined verifies both optimizations work together.
func TestGeminiOptimizations_Combined(t *testing.T) {
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"tool_call": map[string]any{
				"type": "string",
			},
		},
	}

	opts := ai.GenOptions{
		ThinkingLevel:  "low",
		ResponseSchema: schema,
		Temperature:    0.0,
		Tools: []ai.ToolDefinition{
			{Name: "execute_query", Description: "Execute a query"},
		},
	}

	req := buildGeminiRequest("test query", opts)

	if req.GenerationConfig == nil {
		t.Fatal("expected GenerationConfig to be set")
	}

	// Verify thinking config
	if req.GenerationConfig.ThinkingConfig == nil {
		t.Fatal("expected ThinkingConfig to be set")
	}
	if req.GenerationConfig.ThinkingConfig.ThinkingLevel != "low" {
		t.Errorf("ThinkingLevel = %q, want %q",
			req.GenerationConfig.ThinkingConfig.ThinkingLevel, "low")
	}

	// Verify response schema
	if req.GenerationConfig.ResponseSchema == nil {
		t.Fatal("expected ResponseSchema to be set")
	}

	// Verify tools are still set
	if len(req.Tools) == 0 {
		t.Fatal("expected Tools to be set")
	}
	if req.Tools[0].FunctionDeclarations[0].Name != "execute_query" {
		t.Errorf("Tool name = %q, want %q",
			req.Tools[0].FunctionDeclarations[0].Name, "execute_query")
	}
}
