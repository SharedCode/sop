package agent

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// PolicyAgent is a specialized agent that enforces policies.
// It implements the Agent interface but focuses on validation rather than generation.
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

// Ask evaluates the input against the policy.
// If the policy passes, it returns the input (or a transformed version).
// If the policy fails, it returns an error.
func (p *PolicyAgent) Ask(ctx context.Context, query string) (string, error) {
	if p.classifier == nil || p.policy == nil {
		return query, nil
	}

	sample := ai.ContentSample{Text: query}
	labels, err := p.classifier.Classify(sample)
	if err != nil {
		return "", fmt.Errorf("policy classification failed: %w", err)
	}

	decision, err := p.policy.Evaluate("input", sample, labels)
	if err != nil {
		return "", fmt.Errorf("policy evaluation failed: %w", err)
	}

	if decision.Action == "block" {
		return "", fmt.Errorf("policy violation: %v", decision.Reasons)
	}

	return query, nil
}

// ID returns the agent ID.
func (p *PolicyAgent) ID() string {
	return p.id
}

// Search is not supported for PolicyAgent, but implemented to satisfy the interface.
func (p *PolicyAgent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[map[string]any], error) {
	return nil, fmt.Errorf("PolicyAgent does not support Search")
}
