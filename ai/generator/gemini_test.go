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
	if req.ToolConfig == nil || req.ToolConfig.FunctionCallingConfig == nil {
		t.Fatalf("expected Gemini tool config to be included, got %#v", req.ToolConfig)
	}
	if req.ToolConfig.FunctionCallingConfig.Mode != "VALIDATED" {
		t.Fatalf("expected VALIDATED function-calling mode, got %#v", req.ToolConfig.FunctionCallingConfig)
	}
}

func TestBuildGeminiRequest_OmitsToolConfigWithoutTools(t *testing.T) {
	req := buildGeminiRequest("find users", ai.GenOptions{SystemPrompt: "system"})

	if req.ToolConfig != nil {
		t.Fatalf("expected no Gemini tool config when no tools are present, got %#v", req.ToolConfig)
	}
	if len(req.Tools) != 0 {
		t.Fatalf("expected no Gemini tools when none were provided, got %#v", req.Tools)
	}
}

func TestBuildGeminiRequest_IncludesNativeToolContinuation(t *testing.T) {
	req := buildGeminiRequest("continue after tool", ai.GenOptions{
		NativeToolContinuations: []ai.NativeToolContinuation{{
			ToolCall: ai.ToolCall{
				Name:     "list_stores",
				Args:     map[string]any{"store_names": []any{"users"}},
				NativeID: "call_abc123",
			},
			Response: map[string]any{"stores": []any{map[string]any{"name": "users"}}},
		}},
	})

	if len(req.Contents) != 3 {
		t.Fatalf("expected prompt plus function call/response continuation, got %#v", req.Contents)
	}
	if req.Contents[0].Role != "user" || req.Contents[0].Parts[0].Text != "continue after tool" {
		t.Fatalf("expected first content to carry the prompt, got %#v", req.Contents[0])
	}
	if req.Contents[1].Role != "model" || req.Contents[1].Parts[0].FunctionCall == nil {
		t.Fatalf("expected model functionCall continuation, got %#v", req.Contents[1])
	}
	if req.Contents[1].Parts[0].FunctionCall.ID != "call_abc123" {
		t.Fatalf("expected functionCall id to round-trip, got %#v", req.Contents[1].Parts[0].FunctionCall)
	}
	if req.Contents[2].Role != "user" || req.Contents[2].Parts[0].FunctionResponse == nil {
		t.Fatalf("expected user functionResponse continuation, got %#v", req.Contents[2])
	}
	if req.Contents[2].Parts[0].FunctionResponse.ID != "call_abc123" {
		t.Fatalf("expected functionResponse id to match functionCall id, got %#v", req.Contents[2].Parts[0].FunctionResponse)
	}
	if req.Contents[2].Parts[0].FunctionResponse.Name != "list_stores" {
		t.Fatalf("expected functionResponse name to match tool name, got %#v", req.Contents[2].Parts[0].FunctionResponse)
	}
}

func TestBuildGeminiRequest_SanitizesUnsupportedSchemaKeywords(t *testing.T) {
	req := buildGeminiRequest("sanitize", ai.GenOptions{
		Tools: []ai.ToolDefinition{{
			Name:        "execute_script",
			Description: "Executes a script",
			Schema: `{
				"properties": {
					"script": {
						"type": "array",
						"items": {
							"properties": {
								"op": {
									"type": "string",
									"enum": ["scan"],
									"default": "scan"
								}
							},
							"required": ["op"],
							"additionalProperties": false
						},
						"description": "script steps"
					},
					"mode": {
						"oneOf": [{"type": "string"}, {"type": "integer"}],
						"description": "mode selector"
					}
				},
				"required": ["script"],
				"additionalProperties": false,
				"default": {}
			}`,
		}},
	})

	params := req.Tools[0].FunctionDeclarations[0].Parameters
	if params["type"] != "object" {
		t.Fatalf("expected root type to default to object, got %#v", params)
	}
	if _, found := params["additionalProperties"]; found {
		t.Fatalf("expected unsupported root keyword to be removed, got %#v", params)
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected sanitized properties, got %#v", params)
	}
	mode, ok := props["mode"].(map[string]any)
	if !ok {
		t.Fatalf("expected mode property, got %#v", props)
	}
	if _, found := mode["oneOf"]; found {
		t.Fatalf("expected unsupported nested keyword to be removed, got %#v", mode)
	}
	if mode["description"] != "mode selector" {
		t.Fatalf("expected supported nested description to remain, got %#v", mode)
	}
	if mode["type"] != "string" {
		t.Fatalf("expected missing nested type to fall back to string, got %#v", mode)
	}
	script, ok := props["script"].(map[string]any)
	if !ok {
		t.Fatalf("expected script property, got %#v", props)
	}
	items, ok := script["items"].(map[string]any)
	if !ok {
		t.Fatalf("expected array items to be preserved, got %#v", script)
	}
	if _, found := items["additionalProperties"]; found {
		t.Fatalf("expected unsupported items keyword to be removed, got %#v", items)
	}
	itemProps, ok := items["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested item properties, got %#v", items)
	}
	op, ok := itemProps["op"].(map[string]any)
	if !ok {
		t.Fatalf("expected nested op property, got %#v", itemProps)
	}
	if _, found := op["default"]; found {
		t.Fatalf("expected unsupported nested default to be removed, got %#v", op)
	}
	if op["type"] != "string" {
		t.Fatalf("expected op type to remain, got %#v", op)
	}
	if required, ok := params["required"].([]string); !ok || len(required) != 1 || required[0] != "script" {
		t.Fatalf("expected required fields to be preserved as strings, got %#v", params["required"])
	}
}

func TestBuildGeminiRequest_PreservesStructuredListStoresSchema(t *testing.T) {
	req := buildGeminiRequest("research stores", ai.GenOptions{
		Tools: []ai.ToolDefinition{{
			Name:        "list_stores",
			Description: "Research store schema",
			Schema:      `{"type":"object","properties":{"database":{"type":"string"},"stores":{"type":"array","items":{"type":"string"}}}}`,
		}},
	})

	if len(req.Tools) != 1 || len(req.Tools[0].FunctionDeclarations) != 1 {
		t.Fatalf("expected one function declaration, got %#v", req.Tools)
	}
	params := req.Tools[0].FunctionDeclarations[0].Parameters
	if params["type"] != "object" {
		t.Fatalf("expected object root schema, got %#v", params)
	}
	props, ok := params["properties"].(map[string]any)
	if !ok {
		t.Fatalf("expected properties to survive sanitization, got %#v", params)
	}
	stores, ok := props["stores"].(map[string]any)
	if !ok {
		t.Fatalf("expected stores property to remain structured, got %#v", props)
	}
	if stores["type"] != "array" {
		t.Fatalf("expected stores to remain an array, got %#v", stores)
	}
	items, ok := stores["items"].(map[string]any)
	if !ok || items["type"] != "string" {
		t.Fatalf("expected stores.items to remain a string schema, got %#v", stores)
	}
}

func TestExtractGeminiOutput_PreservesFunctionCallIDForTransportContinuity(t *testing.T) {
	resp := geminiResponse{
		Candidates: []struct {
			FinishReason string `json:"finishReason,omitempty"`
			Content      struct {
				Parts []geminiPart `json:"parts"`
			} `json:"content"`
		}{
			{Content: struct {
				Parts []geminiPart `json:"parts"`
			}{Parts: []geminiPart{{FunctionCall: &geminiFunctionCall{
				Name: "execute_script",
				Args: map[string]any{"script": []any{"scan"}},
				ID:   "call_12345xyz",
			}}}}},
		},
	}

	out, err := extractGeminiOutput(resp)
	if err != nil {
		t.Fatalf("extractGeminiOutput failed: %v", err)
	}
	if len(out.ToolCalls) != 1 {
		t.Fatalf("expected one tool call, got %#v", out.ToolCalls)
	}
	if out.ToolCalls[0].NativeID != "call_12345xyz" {
		t.Fatalf("expected Gemini function call id to be preserved, got %#v", out.ToolCalls[0])
	}
	if out.ToolCalls[0].TransportMeta["provider"] != "gemini" {
		t.Fatalf("expected Gemini transport metadata, got %#v", out.ToolCalls[0].TransportMeta)
	}
	if out.ToolCalls[0].TransportMeta["function_call_id"] != "call_12345xyz" {
		t.Fatalf("expected Gemini function_call_id transport metadata, got %#v", out.ToolCalls[0].TransportMeta)
	}
}
