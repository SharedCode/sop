package policy

import (
	"context"

	"github.com/sharedcode/sop/ai"
)

// Chain combines multiple policy engines into one.
// It evaluates them in order. If any policy returns "block", the chain returns "block" immediately.
type Chain struct {
	policies []ai.PolicyEngine
}

func NewChain(policies ...ai.PolicyEngine) *Chain {
	return &Chain{policies: policies}
}

func (c *Chain) Evaluate(ctx context.Context, stage string, sample ai.ContentSample, labels []ai.Label) (ai.PolicyDecision, error) {
	for _, p := range c.policies {
		decision, err := p.Evaluate(ctx, stage, sample, labels)
		if err != nil {
			return ai.PolicyDecision{}, err
		}
		if decision.Action == "block" {
			return decision, nil
		}
	}
	return ai.PolicyDecision{Action: "allow", PolicyID: "chain-allow"}, nil
}
