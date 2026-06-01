package generator

// ----------------------------------------------------------------------------
// OpenAI Responses API — Wire Types
// ----------------------------------------------------------------------------

type openAIResponsesRequest struct {
	Model              string                     `json:"model"`
	Instructions       string                     `json:"instructions,omitempty"`
	Input              []openAIResponsesInputItem `json:"input,omitempty"`
	Tools              []openAIResponsesTool      `json:"tools,omitempty"`
	ToolChoice         any                        `json:"tool_choice,omitempty"`
	PreviousResponseID string                     `json:"previous_response_id,omitempty"`
	Include            []string                   `json:"include,omitempty"`
	Reasoning          *openAIResponsesReasoning  `json:"reasoning,omitempty"`
	ParallelToolCalls  *bool                      `json:"parallel_tool_calls,omitempty"`
	Store              *bool                      `json:"store,omitempty"`
	Temperature        *float32                   `json:"temperature,omitempty"`
	MaxOutputTokens    int                        `json:"max_output_tokens,omitempty"`
	Stream             *bool                      `json:"stream,omitempty"`
}

type openAIResponsesInputItem struct {
	ID               string `json:"id,omitempty"`
	Role             string `json:"role,omitempty"`
	Content          string `json:"content,omitempty"`
	Type             string `json:"type,omitempty"`
	Name             string `json:"name,omitempty"`
	Arguments        string `json:"arguments,omitempty"`
	CallID           string `json:"call_id,omitempty"`
	Output           string `json:"output,omitempty"`
	Phase            string `json:"phase,omitempty"`
	EncryptedContent string `json:"encrypted_content,omitempty"`
}

type openAIResponsesTool struct {
	Type        string         `json:"type"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Parameters  map[string]any `json:"parameters,omitempty"`
	Strict      bool           `json:"strict,omitempty"`
}

type openAIResponsesReasoning struct {
	Effort string `json:"effort,omitempty"`
}

type openAIResponsesResponse struct {
	ID         string                      `json:"id"`
	Status     string                      `json:"status,omitempty"`
	Output     []openAIResponsesOutputItem `json:"output,omitempty"`
	OutputText string                      `json:"output_text,omitempty"`
	Usage      *openAIResponsesUsage       `json:"usage,omitempty"`
	Error      *openAIResponsesError       `json:"error,omitempty"`
}

type openAIResponsesOutputItem struct {
	ID               string                       `json:"id,omitempty"`
	Type             string                       `json:"type,omitempty"`
	Status           string                       `json:"status,omitempty"`
	Role             string                       `json:"role,omitempty"`
	Phase            string                       `json:"phase,omitempty"`
	CallID           string                       `json:"call_id,omitempty"`
	Name             string                       `json:"name,omitempty"`
	Arguments        string                       `json:"arguments,omitempty"`
	EncryptedContent string                       `json:"encrypted_content,omitempty"`
	Content          []openAIResponsesContentItem `json:"content,omitempty"`
	Summary          []openAIResponsesSummaryItem `json:"summary,omitempty"`
}

type openAIResponsesContentItem struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type openAIResponsesSummaryItem struct {
	Type string `json:"type,omitempty"`
	Text string `json:"text,omitempty"`
}

type openAIResponsesUsage struct {
	TotalTokens int `json:"total_tokens,omitempty"`
}

type openAIResponsesError struct {
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type openAIResponsesStreamEvent struct {
	Type         string                     `json:"type"`
	Response     *openAIResponsesResponse   `json:"response,omitempty"`
	Item         *openAIResponsesOutputItem `json:"item,omitempty"`
	OutputIndex  int                        `json:"output_index,omitempty"`
	ItemID       string                     `json:"item_id,omitempty"`
	ContentIndex int                        `json:"content_index,omitempty"`
	SummaryIndex int                        `json:"summary_index,omitempty"`
	Sequence     int                        `json:"sequence_number,omitempty"`
	Delta        string                     `json:"delta,omitempty"`
	Text         string                     `json:"text,omitempty"`
	Name         string                     `json:"name,omitempty"`
	Arguments    string                     `json:"arguments,omitempty"`
	Message      string                     `json:"message,omitempty"`
}

// openAIResponsesAssistantMessage is an extracted assistant message from a response.
type openAIResponsesAssistantMessage struct {
	Phase string
	Text  string
}

// ----------------------------------------------------------------------------
// OpenAI Chat Completions API — Wire Types
// ----------------------------------------------------------------------------

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float32         `json:"temperature,omitempty"`
}

type openAIResponse struct {
	Choices []struct {
		Message openAIMessage `json:"message"`
	} `json:"choices"`
	Usage struct {
		TotalTokens int `json:"total_tokens"`
	} `json:"usage"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}
