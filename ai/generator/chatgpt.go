package generator

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// chatgpt implements the Generator interface for OpenAI's ChatGPT models.
type chatgpt struct {
	apiKey string
	model  string
}

func init() {
	Register("chatgpt", func(cfg map[string]any) (ai.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		model, _ := cfg["model"].(string)
		if model == "" {
			model = "gpt-4"
		}
		return &chatgpt{apiKey: apiKey, model: model}, nil
	})
}

// Name returns the name of the generator.
func (g *chatgpt) Name() string { return "chatgpt" }

// Generate sends a prompt to the ChatGPT API and returns the generated text.
func (g *chatgpt) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	// Placeholder for actual OpenAI API call
	return ai.GenOutput{
		Text:       fmt.Sprintf("[ChatGPT %s] Response to: %s", g.model, prompt),
		TokensUsed: len(prompt) / 3,
		Raw:        nil,
	}, nil
}

// EstimateCost estimates the cost of the generation based on token usage.
func (g *chatgpt) EstimateCost(inTokens, outTokens int) float64 {
	return float64(inTokens)*0.0003 + float64(outTokens)*0.0006
}
