package policy

import (
	"fmt"

	"github.com/sharedcode/sop/ai/internal/port"
)

type ConsecutiveViolationPolicy struct {
	id           string
	threshold    float32
	blocked      map[string]bool
	maxStrikes   int
	currentCount int
	locked       bool
}

func NewConsecutiveViolationPolicy(id string, threshold float32, blockedLabels []string, maxStrikes int) *ConsecutiveViolationPolicy {
	b := make(map[string]bool)
	for _, l := range blockedLabels {
		b[l] = true
	}
	return &ConsecutiveViolationPolicy{
		id:         id,
		threshold:  threshold,
		blocked:    b,
		maxStrikes: maxStrikes,
	}
}

func (p *ConsecutiveViolationPolicy) Evaluate(stage string, sample port.ContentSample, labels []port.Label) (port.PolicyDecision, error) {
	// Only enforce strikes on input to avoid resetting count on clean output
	if stage != "input" {
		return port.PolicyDecision{Action: "allow", PolicyID: p.id}, nil
	}

	if p.locked {
		return port.PolicyDecision{
			Action:   "block",
			Reasons:  []string{"Session terminated due to repeated safety violations."},
			PolicyID: p.id,
		}, nil
	}

	isViolation := false
	var violationLabel string
	var violationScore float32

	for _, l := range labels {
		if p.blocked[l.Name] && l.Score >= p.threshold {
			isViolation = true
			violationLabel = l.Name
			violationScore = l.Score
			break
		}
	}

	if isViolation {
		p.currentCount++
		if p.currentCount >= p.maxStrikes {
			p.locked = true
			return port.PolicyDecision{
				Action:   "block",
				Reasons:  []string{fmt.Sprintf("label %q score %.2f exceeds threshold. Session terminated (violation %d/%d)", violationLabel, violationScore, p.currentCount, p.maxStrikes)},
				PolicyID: p.id,
			}, nil
		}
		// Block but allow retry
		return port.PolicyDecision{
			Action:   "block",
			Reasons:  []string{fmt.Sprintf("label %q score %.2f exceeds threshold (violation %d/%d)", violationLabel, violationScore, p.currentCount, p.maxStrikes)},
			PolicyID: p.id,
		}, nil
	}

	// Reset on clean input
	p.currentCount = 0
	return port.PolicyDecision{Action: "allow", PolicyID: p.id}, nil
}
