package generator

import (
	"strings"
	"testing"
)

func TestSummarizeChatGPTContinuationToolOutput_UsesPreviewForNonFinalResults(t *testing.T) {
	got := summarizeChatGPTContinuationToolOutput("execute_script", `[{"id":1},{"id":2},{"id":3}]`, false)
	if !strings.Contains(got, "Preview (first 3 rows)") {
		t.Fatalf("expected capped preview rows, got %q", got)
	}
	if !strings.Contains(got, "\"id\":1") {
		t.Fatalf("expected a sample row payload in the preview, got %q", got)
	}
}

func TestSummarizeChatGPTContinuationToolOutput_FinalResultUsesCompactSummary(t *testing.T) {
	got := summarizeChatGPTContinuationToolOutput("execute_script", `[{"id":1},{"id":2},{"id":3}]`, true)
	if !strings.Contains(got, "The full row payload was already streamed") {
		t.Fatalf("expected compact summary for final result, got %q", got)
	}
	if strings.Contains(got, "Preview (first") {
		t.Fatalf("did not expect preview rows for final result, got %q", got)
	}
}

func TestSummarizeGeminiContinuationToolOutput_UsesPreviewForNonFinalResults(t *testing.T) {
	got := summarizeGeminiContinuationToolOutput("execute_script", map[string]any{"result": `[{"id":1},{"id":2},{"id":3}]`}, false)
	payload, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", got)
	}
	result, _ := payload["result"].(string)
	if !strings.Contains(result, "Preview (first 3 rows)") {
		t.Fatalf("expected capped preview rows, got %q", result)
	}
}

func TestSummarizeGeminiContinuationToolOutput_FinalResultUsesCompactSummary(t *testing.T) {
	got := summarizeGeminiContinuationToolOutput("execute_script", map[string]any{"result": `[{"id":1},{"id":2},{"id":3}]`}, true)
	payload, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", got)
	}
	result, _ := payload["result"].(string)
	if !strings.Contains(result, "The full row payload was already streamed") {
		t.Fatalf("expected compact summary in default mode, got %q", result)
	}
}

func TestSummarizeAnthropicContinuationToolOutput_UsesPreviewForNonFinalResults(t *testing.T) {
	got := summarizeAnthropicContinuationToolOutput("execute_script", map[string]any{"result": `[{"id":1},{"id":2},{"id":3}]`}, false)
	payload, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", got)
	}
	result, _ := payload["result"].(string)
	if !strings.Contains(result, "Preview (first 3 rows)") {
		t.Fatalf("expected capped preview rows, got %q", result)
	}
}

func TestSummarizeAnthropicContinuationToolOutput_FinalResultUsesCompactSummary(t *testing.T) {
	got := summarizeAnthropicContinuationToolOutput("execute_script", map[string]any{"result": `[{"id":1},{"id":2},{"id":3}]`}, true)
	payload, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("expected map payload, got %T", got)
	}
	result, _ := payload["result"].(string)
	if !strings.Contains(result, "The full row payload was already streamed") {
		t.Fatalf("expected compact summary in default mode, got %q", result)
	}
}
