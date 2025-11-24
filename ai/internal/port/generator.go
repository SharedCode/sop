package port

import "context"

type GenOptions struct {
	MaxTokens   int
	Temperature float32
	TopP        float32
	Stop        []string
}

type GenOutput struct {
	Text       string
	TokensUsed int
	Raw        any
}

type Generator interface {
	Name() string
	Generate(ctx context.Context, prompt string, opts GenOptions) (GenOutput, error)
	EstimateCost(inTokens, outTokens int) float64
}
