package generator

import (
	"encoding/json"
	"testing"
)

func TestChatGPTPromptCaching_ChatCompletionsUsageTracking(t *testing.T) {
	// Test that the Chat Completions API response properly tracks cache usage
	responseJSON := `{
		"id": "chatcmpl-123",
		"object": "chat.completion",
		"created": 1677652288,
		"model": "gpt-4o",
		"choices": [{
			"index": 0,
			"message": {
				"role": "assistant",
				"content": "Hello! How can I help you today?"
			},
			"finish_reason": "stop"
		}],
		"usage": {
			"prompt_tokens": 5000,
			"completion_tokens": 10,
			"total_tokens": 5010,
			"prompt_tokens_details": {
				"cached_tokens": 4000
			}
		}
	}`

	var response openAIResponse
	if err := json.Unmarshal([]byte(responseJSON), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Verify basic usage
	if response.Usage.TotalTokens != 5010 {
		t.Fatalf("expected 5010 total tokens, got %d", response.Usage.TotalTokens)
	}
	if response.Usage.PromptTokens != 5000 {
		t.Fatalf("expected 5000 prompt tokens, got %d", response.Usage.PromptTokens)
	}
	if response.Usage.CompletionTokens != 10 {
		t.Fatalf("expected 10 completion tokens, got %d", response.Usage.CompletionTokens)
	}

	// Verify cache tracking
	if response.Usage.PromptTokensDetails == nil {
		t.Fatal("expected prompt_tokens_details to be present")
	}
	if response.Usage.PromptTokensDetails.CachedTokens != 4000 {
		t.Fatalf("expected 4000 cached tokens, got %d", response.Usage.PromptTokensDetails.CachedTokens)
	}

	t.Log("✅ Verified: Chat Completions API tracks cached tokens (4000/5000 = 80% cache hit)")
}

func TestChatGPTPromptCaching_ResponsesAPIUsageTracking(t *testing.T) {
	// Test that the Responses API properly tracks cache usage
	responseJSON := `{
		"id": "resp_123",
		"status": "completed",
		"output": [{
			"id": "msg_456",
			"type": "message",
			"role": "assistant",
			"content": [{
				"type": "text",
				"text": "Done"
			}]
		}],
		"usage": {
			"prompt_tokens": 8000,
			"completion_tokens": 50,
			"total_tokens": 8050,
			"prompt_tokens_details": {
				"cached_tokens": 7200
			}
		}
	}`

	var response openAIResponsesResponse
	if err := json.Unmarshal([]byte(responseJSON), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// Verify basic usage
	if response.Usage == nil {
		t.Fatal("expected usage to be present")
	}
	if response.Usage.TotalTokens != 8050 {
		t.Fatalf("expected 8050 total tokens, got %d", response.Usage.TotalTokens)
	}
	if response.Usage.PromptTokens != 8000 {
		t.Fatalf("expected 8000 prompt tokens, got %d", response.Usage.PromptTokens)
	}
	if response.Usage.CompletionTokens != 50 {
		t.Fatalf("expected 50 completion tokens, got %d", response.Usage.CompletionTokens)
	}

	// Verify cache tracking
	if response.Usage.PromptTokensDetails == nil {
		t.Fatal("expected prompt_tokens_details to be present")
	}
	if response.Usage.PromptTokensDetails.CachedTokens != 7200 {
		t.Fatalf("expected 7200 cached tokens, got %d", response.Usage.PromptTokensDetails.CachedTokens)
	}

	t.Log("✅ Verified: Responses API tracks cached tokens (7200/8000 = 90% cache hit)")
}

func TestChatGPTPromptCaching_AutomaticDetection(t *testing.T) {
	// This test documents that OpenAI prompt caching is automatic
	// No special request fields needed - OpenAI detects repeated message prefixes

	t.Log("OpenAI Prompt Caching Behavior:")
	t.Log("  1. Automatic detection - no special request markers needed")
	t.Log("  2. Caches repeated message prefixes (system prompts, tool definitions)")
	t.Log("  3. Minimum 1,024 tokens to activate caching")
	t.Log("  4. 5 minute TTL - same as Anthropic")
	t.Log("  5. 50% discount on cached tokens")
	t.Log("")
	t.Log("Example: 5K token system prompt + tools, 100 requests:")
	t.Log("  Request 1: 5,000 prompt tokens @ $2.50/1M = $0.0125")
	t.Log("  Request 2-100: 5,000 cached @ $1.25/1M = $0.00625 each")
	t.Log("  Total: $0.0125 + (99 × $0.00625) = $0.631")
	t.Log("  vs Without caching: 100 × $0.0125 = $1.25")
	t.Log("  Savings: $0.619 (49.5%)")
}

func TestChatGPTPromptCaching_WithoutCacheHit(t *testing.T) {
	// Test response without cache hits (first request or expired cache)
	responseJSON := `{
		"id": "chatcmpl-789",
		"choices": [{
			"message": {
				"role": "assistant",
				"content": "Hello"
			}
		}],
		"usage": {
			"prompt_tokens": 1500,
			"completion_tokens": 5,
			"total_tokens": 1505
		}
	}`

	var response openAIResponse
	if err := json.Unmarshal([]byte(responseJSON), &response); err != nil {
		t.Fatalf("failed to unmarshal response: %v", err)
	}

	// When no cache hit, prompt_tokens_details may be absent or have 0 cached_tokens
	if response.Usage.PromptTokensDetails != nil && response.Usage.PromptTokensDetails.CachedTokens != 0 {
		t.Fatalf("expected 0 cached tokens, got %d", response.Usage.PromptTokensDetails.CachedTokens)
	}

	t.Log("✅ Verified: Response without cache hit has no cached_tokens")
}

func TestChatGPTPromptCaching_CostCalculation(t *testing.T) {
	// Document cost calculation with caching

	// Scenario: 5K prompt (4K system+tools, 1K user query), 100 output, 80% cache hit
	promptTokens := 5000
	cachedTokens := 4000
	uncachedTokens := promptTokens - cachedTokens
	completionTokens := 100

	// GPT-4o pricing
	inputPricePerM := 2.50  // $2.50/1M tokens
	cachedPricePerM := 1.25 // $1.25/1M tokens (50% discount)
	outputPricePerM := 10.0 // $10.00/1M tokens

	uncachedCost := float64(uncachedTokens) / 1_000_000 * inputPricePerM
	cachedCost := float64(cachedTokens) / 1_000_000 * cachedPricePerM
	outputCost := float64(completionTokens) / 1_000_000 * outputPricePerM

	totalCost := uncachedCost + cachedCost + outputCost

	// Without caching
	noCacheCost := float64(promptTokens)/1_000_000*inputPricePerM + outputCost
	savings := noCacheCost - totalCost
	savingsPercent := (savings / noCacheCost) * 100

	t.Logf("Cost with 80%% cache hit: $%.6f", totalCost)
	t.Logf("  - Uncached input (1K): $%.6f", uncachedCost)
	t.Logf("  - Cached input (4K):   $%.6f (50%% discount)", cachedCost)
	t.Logf("  - Output (100):        $%.6f", outputCost)
	t.Logf("Cost without caching:   $%.6f", noCacheCost)
	t.Logf("Savings: $%.6f (%.1f%%)", savings, savingsPercent)

	// Verify math
	if totalCost >= noCacheCost {
		t.Fatal("caching should reduce cost")
	}
	if savingsPercent < 30 || savingsPercent > 50 {
		t.Fatalf("expected ~40%% savings with 80%% cache hit, got %.1f%%", savingsPercent)
	}
}
