package agent

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// PolicyAgent wraps a PolicyEngine and Classifier into an Agent interface.
// It evaluates the input against the policy. If allowed, it returns the input as is.
// If blocked, it returns an error.
type PolicyAgent struct {
	id         string
	policy     ai.PolicyEngine
	classifier ai.Classifier
}

// NewPolicyAgent creates a new PolicyAgent.
func NewPolicyAgent(id string, policy ai.PolicyEngine, classifier ai.Classifier) *PolicyAgent {
	return &PolicyAgent{
		id:         id,
		policy:     policy,
		classifier: classifier,
	}
}

// Search is not supported for PolicyAgent, but implemented to satisfy the interface.
func (p *PolicyAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit, error) {
	return nil, fmt.Errorf("PolicyAgent does not support Search")
}

// Ask evaluates the query against the policy.
func (p *PolicyAgent) Ask(ctx context.Context, query string) (string, error) {
	if p.policy == nil || p.classifier == nil {
		return query, nil
	}

	sample := ai.ContentSample{Text: query}
	labels, err := p.classifier.Classify(sample)
	if err != nil {
		return "", fmt.Errorf("policy agent '%s' classification failed: %w", p.id, err)
	}

	decision, err := p.policy.Evaluate("input", sample, labels)
	if err != nil {
		return "", fmt.Errorf("policy agent '%s' evaluation failed: %w", p.id, err)
	}

	if decision.Action == "block" {
		return "", fmt.Errorf("request blocked by policy '%s': %v", p.id, decision.Reasons)
	}

	// Pass-through if allowed
	return query, nil
}
