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
	ToolConfig        *geminiToolConfig       `json:"toolConfig,omitempty"`
	GenerationConfig  *geminiGenerationConfig `json:"generationConfig,omitempty"`
}

type geminiToolConfig struct {
	FunctionCallingConfig *geminiFunctionCallingConfig `json:"functionCallingConfig,omitempty"`
}

type geminiFunctionCallingConfig struct {
	Mode string `json:"mode,omitempty"`
}

type geminiGenerationConfig struct {
	Temperature     float32 `json:"temperature,omitempty"`
	TopP            float32 `json:"topP,omitempty"`
	MaxOutputTokens int     `json:"maxOutputTokens,omitempty"`
}

type geminiContent struct {
	Role  string       `json:"role,omitempty"`
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text             string                  `json:"text,omitempty"`
	FunctionCall     *geminiFunctionCall     `json:"functionCall,omitempty"`
	FunctionResponse *geminiFunctionResponse `json:"functionResponse,omitempty"`
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
	ID   string         `json:"id,omitempty"`
}

type geminiFunctionResponse struct {
	Name     string `json:"name"`
	Response any    `json:"response,omitempty"`
	ID       string `json:"id,omitempty"`
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

var geminiAllowedSchemaKeys = map[string]struct{}{
	"description": {},
	"enum":        {},
	"items":       {},
	"properties":  {},
	"required":    {},
	"type":        {},
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

func extractGeminiOutput(resp geminiResponse) (ai.GenOutput, error) {
	if resp.Error != nil {
		return ai.GenOutput{}, fmt.Errorf("gemini api returned error: %s", resp.Error.Message)
	}

	if len(resp.Candidates) == 0 || len(resp.Candidates[0].Content.Parts) == 0 {
		return ai.GenOutput{}, fmt.Errorf("%s", describeGeminiEmptyResponse(resp))
	}

	var out ai.GenOutput
	for _, p := range resp.Candidates[0].Content.Parts {
		if p.FunctionCall != nil {
			toolCall := ai.ToolCall{
				Name:     p.FunctionCall.Name,
				Args:     p.FunctionCall.Args,
				NativeID: strings.TrimSpace(p.FunctionCall.ID),
			}
			if toolCall.NativeID != "" {
				toolCall.TransportMeta = map[string]any{
					"provider":         "gemini",
					"function_call_id": toolCall.NativeID,
				}
			}
			out.ToolCalls = append(out.ToolCalls, toolCall)
		} else if p.Text != "" {
			out.Text += p.Text
		}
	}

	return out, nil
}

func buildGeminiRequest(prompt string, opts ai.GenOptions) geminiRequest {
	reqBody := geminiRequest{
		Contents: []geminiContent{
			{Role: "user", Parts: []geminiPart{{Text: prompt}}},
		},
	}

	for _, continuation := range opts.NativeToolContinuations {
		if strings.TrimSpace(continuation.ToolCall.Name) == "" {
			continue
		}

		reqBody.Contents = append(reqBody.Contents,
			geminiContent{
				Role: "model",
				Parts: []geminiPart{{FunctionCall: &geminiFunctionCall{
					Name: continuation.ToolCall.Name,
					Args: continuation.ToolCall.Args,
					ID:   strings.TrimSpace(continuation.ToolCall.NativeID),
				}}},
			},
			geminiContent{
				Role: "user",
				Parts: []geminiPart{{FunctionResponse: &geminiFunctionResponse{
					Name:     continuation.ToolCall.Name,
					Response: continuation.Response,
					ID:       strings.TrimSpace(continuation.ToolCall.NativeID),
				}}},
			},
		)
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
			params = sanitizeGeminiSchema(params)
			funcs = append(funcs, geminiFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  params,
			})
		}
		reqBody.Tools = []geminiTool{{FunctionDeclarations: funcs}}
		reqBody.ToolConfig = &geminiToolConfig{
			FunctionCallingConfig: &geminiFunctionCallingConfig{Mode: "VALIDATED"},
		}
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

func sanitizeGeminiSchema(schema map[string]any) map[string]any {
	sanitized, ok := sanitizeGeminiSchemaNode(schema)
	if !ok {
		return map[string]any{"type": "object"}
	}
	if _, ok := sanitized["type"].(string); !ok || strings.TrimSpace(sanitized["type"].(string)) == "" {
		sanitized["type"] = inferGeminiSchemaType(sanitized, "object")
	}
	return sanitized
}

func sanitizeGeminiSchemaNode(schema map[string]any) (map[string]any, bool) {
	if len(schema) == 0 {
		return nil, false
	}

	sanitized := make(map[string]any)
	for key := range geminiAllowedSchemaKeys {
		value, ok := schema[key]
		if !ok {
			continue
		}
		switch key {
		case "type":
			typeName, ok := value.(string)
			if ok && strings.TrimSpace(typeName) != "" {
				sanitized[key] = strings.ToLower(strings.TrimSpace(typeName))
			}
		case "description":
			description, ok := value.(string)
			if ok && strings.TrimSpace(description) != "" {
				sanitized[key] = description
			}
		case "enum":
			if enumValues := sanitizeGeminiEnum(value); len(enumValues) > 0 {
				sanitized[key] = enumValues
			}
		case "required":
			if required := sanitizeGeminiRequired(value); len(required) > 0 {
				sanitized[key] = required
			}
		case "properties":
			if props := sanitizeGeminiProperties(value); len(props) > 0 {
				sanitized[key] = props
			}
		case "items":
			if itemSchema, ok := value.(map[string]any); ok {
				child, childOK := sanitizeGeminiSchemaNode(itemSchema)
				if childOK {
					if _, hasType := child["type"]; !hasType {
						child["type"] = inferGeminiSchemaType(child, "string")
					}
					sanitized[key] = child
				}
			}
		}
	}

	if len(sanitized) == 0 {
		return nil, false
	}
	return sanitized, true
}

func sanitizeGeminiProperties(value any) map[string]any {
	propMap, ok := value.(map[string]any)
	if !ok || len(propMap) == 0 {
		return nil
	}
	props := make(map[string]any)
	for name, rawChild := range propMap {
		childMap, ok := rawChild.(map[string]any)
		if !ok {
			continue
		}
		child, childOK := sanitizeGeminiSchemaNode(childMap)
		if !childOK {
			continue
		}
		if _, hasType := child["type"]; !hasType {
			child["type"] = inferGeminiSchemaType(child, "string")
		}
		props[name] = child
	}
	if len(props) == 0 {
		return nil
	}
	return props
}

func sanitizeGeminiRequired(value any) []string {
	switch required := value.(type) {
	case []string:
		out := make([]string, 0, len(required))
		for _, name := range required {
			if strings.TrimSpace(name) != "" {
				out = append(out, name)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(required))
		for _, item := range required {
			name, ok := item.(string)
			if ok && strings.TrimSpace(name) != "" {
				out = append(out, name)
			}
		}
		return out
	default:
		return nil
	}
}

func sanitizeGeminiEnum(value any) []any {
	enumValues, ok := value.([]any)
	if ok && len(enumValues) > 0 {
		return enumValues
	}
	if stringValues, ok := value.([]string); ok && len(stringValues) > 0 {
		out := make([]any, 0, len(stringValues))
		for _, item := range stringValues {
			out = append(out, item)
		}
		return out
	}
	return nil
}

func inferGeminiSchemaType(schema map[string]any, fallback string) string {
	if _, ok := schema["properties"]; ok {
		return "object"
	}
	if _, ok := schema["items"]; ok {
		return "array"
	}
	return fallback
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

	out, err := extractGeminiOutput(geminiResp)
	if err != nil {
		return ai.GenOutput{}, err
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
