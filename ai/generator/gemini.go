package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// gemini implements the Generator interface for Google's Gemini models.
type gemini struct {
	apiKey string
	model  string
}

func init() {
	Register("gemini", func(cfg map[string]any) (ai.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		if apiKey == "" {
			apiKey = os.Getenv("LLM_API_KEY")
			if apiKey == "" {
				apiKey = os.Getenv("GEMINI_API_KEY")
			}
		}
		if strings.HasPrefix(apiKey, "sk-") {
			return nil, fmt.Errorf("detected OpenAI API key (starts with 'sk-') but generator type is 'gemini'. Please check your configuration")
		}
		model, _ := cfg["model"].(string)
		if model == "" {
			model = os.Getenv("GEMINI_MODEL")
		}
		if model == "" {
			model = ai.DefaultModelGemini
		}
		return &gemini{apiKey: apiKey, model: model}, nil
	})
}

// Name returns the name of the generator.
func (g *gemini) Name() string { return "gemini" }

type geminiRequest struct {
	SystemInstruction *geminiContent          `json:"systemInstruction,omitempty"`
	Contents          []geminiContent         `json:"contents"`
	Tools             []geminiTool            `json:"tools,omitempty"`
	GenerationConfig  *geminiGenerationConfig `json:"generationConfig,omitempty"`
}

type geminiGenerationConfig struct {
	Temperature     float32 `json:"temperature,omitempty"`
	TopP            float32 `json:"topP,omitempty"`
	MaxOutputTokens int     `json:"maxOutputTokens,omitempty"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text         string              `json:"text,omitempty"`
	FunctionCall *geminiFunctionCall `json:"functionCall,omitempty"`
}

type geminiTool struct {
	FunctionDeclarations []geminiFunction `json:"functionDeclarations,omitempty"`
}

type geminiFunction struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters,omitempty"`
}

type geminiFunctionCall struct {
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

type geminiResponse struct {
	Candidates []struct {
		FinishReason string `json:"finishReason,omitempty"`
		Content      struct {
			Parts []geminiPart `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	PromptFeedback *struct {
		BlockReason string `json:"blockReason,omitempty"`
	} `json:"promptFeedback,omitempty"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func describeGeminiEmptyResponse(resp geminiResponse) string {
	parts := []string{"no candidates returned from gemini"}
	if resp.PromptFeedback != nil && strings.TrimSpace(resp.PromptFeedback.BlockReason) != "" {
		parts = append(parts, fmt.Sprintf("block_reason=%s", resp.PromptFeedback.BlockReason))
	}
	if len(resp.Candidates) > 0 && strings.TrimSpace(resp.Candidates[0].FinishReason) != "" {
		parts = append(parts, fmt.Sprintf("finish_reason=%s", resp.Candidates[0].FinishReason))
	}
	return strings.Join(parts, "; ")
}

func buildGeminiRequest(prompt string, opts ai.GenOptions) geminiRequest {
	reqBody := geminiRequest{
		Contents: []geminiContent{
			{Parts: []geminiPart{{Text: prompt}}},
		},
	}

	if opts.SystemPrompt != "" {
		reqBody.SystemInstruction = &geminiContent{
			Parts: []geminiPart{{Text: opts.SystemPrompt}},
		}
	}

	if len(opts.Tools) > 0 {
		var funcs []geminiFunction
		for _, t := range opts.Tools {
			var params map[string]any
			if t.Schema != "" && strings.HasPrefix(strings.TrimSpace(t.Schema), "{") {
				json.Unmarshal([]byte(t.Schema), &params)
			} else if t.Schema != "" {
				params = map[string]any{"type": "object"}
			}
			funcs = append(funcs, geminiFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  params,
			})
		}
		reqBody.Tools = []geminiTool{{FunctionDeclarations: funcs}}
	}

	if opts.Temperature > 0 || opts.TopP > 0 || opts.MaxTokens > 0 {
		reqBody.GenerationConfig = &geminiGenerationConfig{
			Temperature:     opts.Temperature,
			TopP:            opts.TopP,
			MaxOutputTokens: opts.MaxTokens,
		}
	}

	return reqBody
}

// Generate sends a prompt to the Gemini API and returns the generated text.
func (g *gemini) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	if g.apiKey == "" || g.apiKey == "YOUR_API_KEY" {
		return ai.GenOutput{
			Text: fmt.Sprintf("[Gemini Stub] Missing API Key. Please set LLM_API_KEY or GEMINI_API_KEY environment variable. Would send: %q", prompt),
		}, nil
	}

	apiURL := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent", g.model)

	reqBody := buildGeminiRequest(prompt, opts)

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-goog-api-key", g.apiKey)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ai.GenOutput{}, fmt.Errorf("gemini api request failed: %w", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		return ai.GenOutput{}, fmt.Errorf("gemini api error (status %d): %s", resp.StatusCode, string(body))
	}

	var geminiResp geminiResponse
	if err := json.Unmarshal(body, &geminiResp); err != nil {
		return ai.GenOutput{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if geminiResp.Error != nil {
		return ai.GenOutput{}, fmt.Errorf("gemini api returned error: %s", geminiResp.Error.Message)
	}

	if len(geminiResp.Candidates) == 0 || len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return ai.GenOutput{}, fmt.Errorf("%s", describeGeminiEmptyResponse(geminiResp))
	}

	var out ai.GenOutput
	for _, p := range geminiResp.Candidates[0].Content.Parts {
		if p.FunctionCall != nil {
			out.ToolCalls = append(out.ToolCalls, ai.ToolCall{
				Name: p.FunctionCall.Name,
				Args: p.FunctionCall.Args,
			})
		} else if p.Text != "" {
			out.Text += p.Text
		}
	}

	// Default rough estimate
	out.TokensUsed = len(prompt) / 4
	return out, nil
}

// EstimateCost estimates the cost of the generation based on token usage.
func (g *gemini) EstimateCost(inTokens, outTokens int) float64 {
	// Placeholder pricing
	return float64(inTokens)*0.0001 + float64(outTokens)*0.0002
}
