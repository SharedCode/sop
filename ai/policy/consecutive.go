package policy

import (
	"fmt"

	"github.com/sharedcode/sop/ai"
)

// ConsecutiveViolationPolicy blocks content if it violates specific labels with a score above a threshold.
// It tracks the number of consecutive violations and locks the session if the limit is reached.
type ConsecutiveViolationPolicy struct {
	id           string
	threshold    float32
	blocked      map[string]bool
	maxStrikes   int
	currentCount int
	locked       bool
}

// NewConsecutiveViolationPolicy creates a new policy that tracks consecutive violations.
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

// Evaluate checks if the content violates the policy.
// If the violation count exceeds maxStrikes, the session is locked.
func (p *ConsecutiveViolationPolicy) Evaluate(stage string, sample ai.ContentSample, labels []ai.Label) (ai.PolicyDecision, error) {
	if p.locked {
		return ai.PolicyDecision{
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
		// Only increment strikes on input violations
		if stage == "input" {
			p.currentCount++
		}

		if p.currentCount >= p.maxStrikes {
			p.locked = true
			return ai.PolicyDecision{
				Action:   "block",
				Reasons:  []string{fmt.Sprintf("label %q score %.2f exceeds threshold. Session terminated (violation %d/%d)", violationLabel, violationScore, p.currentCount, p.maxStrikes)},
				PolicyID: p.id,
			}, nil
		}
		// Block but allow retry
		return ai.PolicyDecision{
			Action:   "block",
			Reasons:  []string{fmt.Sprintf("label %q score %.2f exceeds threshold (violation %d/%d)", violationLabel, violationScore, p.currentCount, p.maxStrikes)},
			PolicyID: p.id,
		}, nil
	}

	// Reset on clean input
	if stage == "input" {
		p.currentCount = 0
	}
	return ai.PolicyDecision{Action: "allow", PolicyID: p.id}, nil
}
