package generator

import "testing"

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
