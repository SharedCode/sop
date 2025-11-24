package generator

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop/ai/internal/port"
)

type chatgpt struct {
	apiKey string
	model  string
}

func init() {
	Register("chatgpt", func(cfg map[string]any) (port.Generator, error) {
		apiKey, _ := cfg["api_key"].(string)
		model, _ := cfg["model"].(string)
		if model == "" {
			model = "gpt-4"
		}
		return &chatgpt{apiKey: apiKey, model: model}, nil
	})
}

func (g *chatgpt) Name() string { return "chatgpt" }

func (g *chatgpt) Generate(ctx context.Context, prompt string, opts port.GenOptions) (port.GenOutput, error) {
	// Placeholder for actual OpenAI API call
	return port.GenOutput{
		Text:       fmt.Sprintf("[ChatGPT %s] Response to: %s", g.model, prompt),
		TokensUsed: len(prompt) / 3,
		Raw:        nil,
	}, nil
}

func (g *chatgpt) EstimateCost(inTokens, outTokens int) float64 {
	return float64(inTokens)*0.0003 + float64(outTokens)*0.0006
}
