package policy

import "github.com/sharedcode/sop/ai/internal/port"

// Chain combines multiple policy engines into one.
// It evaluates them in order. If any policy returns "block", the chain returns "block" immediately.
type Chain struct {
	policies []port.PolicyEngine
}

func NewChain(policies ...port.PolicyEngine) *Chain {
	return &Chain{policies: policies}
}

func (c *Chain) Evaluate(stage string, sample port.ContentSample, labels []port.Label) (port.PolicyDecision, error) {
	for _, p := range c.policies {
		decision, err := p.Evaluate(stage, sample, labels)
		if err != nil {
			return port.PolicyDecision{}, err
		}
		if decision.Action == "block" {
			return decision, nil
		}
	}
	return port.PolicyDecision{Action: "allow", PolicyID: "chain-allow"}, nil
}
