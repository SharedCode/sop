package policy

import (
	"fmt"

	"github.com/sharedcode/sop/ai/internal/port"
)

type ThresholdPolicy struct {
	id        string
	threshold float32
	blocked   map[string]bool
}

func NewThresholdPolicy(id string, threshold float32, blockedLabels []string) *ThresholdPolicy {
	b := make(map[string]bool)
	for _, l := range blockedLabels {
		b[l] = true
	}
	return &ThresholdPolicy{id: id, threshold: threshold, blocked: b}
}

func (p *ThresholdPolicy) Evaluate(stage string, sample port.ContentSample, labels []port.Label) (port.PolicyDecision, error) {
	for _, l := range labels {
		if p.blocked[l.Name] && l.Score >= p.threshold {
			return port.PolicyDecision{
				Action:   "block",
				Reasons:  []string{fmt.Sprintf("label %q score %.2f exceeds threshold", l.Name, l.Score)},
				PolicyID: p.id,
			}, nil
		}
	}
	return port.PolicyDecision{Action: "allow", PolicyID: p.id}, nil
}
