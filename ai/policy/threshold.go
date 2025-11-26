package policy

import (
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// ThresholdPolicy blocks content if it violates specific labels with a score above a threshold.
type ThresholdPolicy struct {
	id        string
	threshold float32
	blocked   map[string]bool
}

// NewThresholdPolicy creates a new policy that blocks content based on label scores.
func NewThresholdPolicy(id string, threshold float32, blockedLabels []string) *ThresholdPolicy {
	b := make(map[string]bool)
	for _, l := range blockedLabels {
		b[l] = true
	}
	return &ThresholdPolicy{id: id, threshold: threshold, blocked: b}
}

// Evaluate checks if the content violates the policy.
func (p *ThresholdPolicy) Evaluate(stage string, sample ai.ContentSample, labels []ai.Label) (ai.PolicyDecision, error) {
	for _, l := range labels {
		if p.blocked[l.Name] && l.Score >= p.threshold {
			return ai.PolicyDecision{
				Action:   "block",
				Reasons:  []string{fmt.Sprintf("label %q score %.2f exceeds threshold", l.Name, l.Score)},
				PolicyID: p.id,
			}, nil
		}
	}
	return ai.PolicyDecision{Action: "allow", PolicyID: p.id}, nil
}
