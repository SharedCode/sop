package generator

import (
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestDescribeGeminiEmptyResponse_IncludesPromptFeedback(t *testing.T) {
	resp := geminiResponse{
		PromptFeedback: &struct {
			BlockReason string `json:"blockReason,omitempty"`
		}{BlockReason: "SAFETY"},
	}

	msg := describeGeminiEmptyResponse(resp)
	if msg != "no candidates returned from gemini; block_reason=SAFETY" {
		t.Fatalf("unexpected message: %s", msg)
	}
}

func TestDescribeGeminiEmptyResponse_IncludesFinishReason(t *testing.T) {
	resp := geminiResponse{
		Candidates: []struct {
			FinishReason string `json:"finishReason,omitempty"`
			Content      struct {
				Parts []geminiPart `json:"parts"`
			} `json:"content"`
		}{
			{FinishReason: "MAX_TOKENS"},
		},
	}

	msg := describeGeminiEmptyResponse(resp)
	if msg != "no candidates returned from gemini; finish_reason=MAX_TOKENS" {
		t.Fatalf("unexpected message: %s", msg)
	}
}

func TestBuildGeminiRequest_IncludesGenerationConfigAndTools(t *testing.T) {
	req := buildGeminiRequest("find users", ai.GenOptions{
		SystemPrompt: "system",
		MaxTokens:    321,
		Temperature:  0.15,
		TopP:         0.9,
		Tools: []ai.ToolDefinition{{
			Name:        "execute_script",
			Description: "Executes a script",
			Schema:      `{"type":"object","properties":{"script":{"type":"array"}}}`,
		}},
	})

	if req.GenerationConfig == nil {
		t.Fatal("expected generation config to be included")
	}
	if req.GenerationConfig.Temperature != 0.15 {
		t.Fatalf("expected temperature 0.15, got %v", req.GenerationConfig.Temperature)
	}
	if req.GenerationConfig.TopP != 0.9 {
		t.Fatalf("expected topP 0.9, got %v", req.GenerationConfig.TopP)
	}
	if req.GenerationConfig.MaxOutputTokens != 321 {
		t.Fatalf("expected max output tokens 321, got %d", req.GenerationConfig.MaxOutputTokens)
	}
	if req.SystemInstruction == nil || len(req.SystemInstruction.Parts) != 1 || req.SystemInstruction.Parts[0].Text != "system" {
		t.Fatalf("expected system instruction to be preserved, got %#v", req.SystemInstruction)
	}
	if len(req.Tools) != 1 || len(req.Tools[0].FunctionDeclarations) != 1 {
		t.Fatalf("expected one function declaration, got %#v", req.Tools)
	}
	if req.Tools[0].FunctionDeclarations[0].Name != "execute_script" {
		t.Fatalf("expected execute_script declaration, got %#v", req.Tools[0].FunctionDeclarations[0])
	}
}
