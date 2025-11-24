package generator

import (
	"context"

	"github.com/sharedcode/sop/ai/internal/port"
)

type Mock struct{ name string }

func NewMock(name string) *Mock { return &Mock{name: name} }
func (m *Mock) Name() string    { return m.name }

func (m *Mock) Generate(ctx context.Context, prompt string, opts port.GenOptions) (port.GenOutput, error) {
	// naive token count: split on spaces
	tokens := 0
	for _, r := range prompt {
		if r == ' ' || r == '\n' || r == '\t' {
			tokens++
		}
	}
	out := port.GenOutput{Text: "[mock] " + prompt, TokensUsed: tokens, Raw: map[string]any{"temperature": opts.Temperature}}
	return out, nil
}

func (m *Mock) EstimateCost(inTokens, outTokens int) float64 {
	return float64(inTokens+outTokens) * 0.000001 // placeholder pricing
}
