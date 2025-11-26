package generator

import (
	"context"
	"fmt"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// Perceptron is a toy local engine.
// It doesn't actually "think" like an LLM, but it demonstrates
// how a local model would look in this architecture.
// It uses a simple keyword-weighting mechanism to "classify" or "generate" responses.
type perceptron struct {
	name    string
	weights map[string]float64
}

func init() {
	Register("perceptron", func(cfg map[string]any) (ai.Generator, error) {
		// In a real scenario, we would load weights from SOP here.
		// For now, we initialize with random or hardcoded weights.
		return &perceptron{
			name: "local-perceptron-v1",
			weights: map[string]float64{
				"sop":      0.9,
				"database": 0.8,
				"fast":     0.7,
				"slow":     -0.5,
			},
		}, nil
	})
}

func (p *perceptron) Name() string { return p.name }

func (p *perceptron) Generate(ctx context.Context, prompt string, opts ai.GenOptions) (ai.GenOutput, error) {
	// 1. "Inference" (Simple Dot Product)
	score := 0.0
	words := strings.Fields(strings.ToLower(prompt))
	for _, w := range words {
		if w, ok := p.weights[w]; ok {
			score += w
		}
	}

	// 2. "Activation" & Output Generation
	var response string
	if score > 0.5 {
		response = fmt.Sprintf("[Perceptron] Positive sentiment detected (Score: %.2f). SOP is great!", score)
	} else if score < -0.5 {
		response = fmt.Sprintf("[Perceptron] Negative sentiment detected (Score: %.2f).", score)
	} else {
		response = fmt.Sprintf("[Perceptron] Neutral sentiment (Score: %.2f). I am a simple local bot.", score)
	}

	// Simulate "thinking" cost
	return ai.GenOutput{
		Text:       response,
		TokensUsed: len(words),
	}, nil
}

func (p *perceptron) EstimateCost(inTokens, outTokens int) float64 {
	return 0.0 // Local compute is "free" (monetarily)
}
