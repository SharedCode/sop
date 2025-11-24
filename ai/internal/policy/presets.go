package policy

import "github.com/sharedcode/sop/ai/internal/port"

// NewProfanityGuardrail returns a configured policy and classifier for profanity filtering.
// Note: The returned policy is stateful (tracks strikes). Use a new instance per user session.
func NewProfanityGuardrail(maxStrikes int) (port.PolicyEngine, port.Classifier) {
	classifier := NewRegexClassifier("profanity-filter", map[string]string{
		"profanity": `(?i)\b(slut|idiot|stupid)\b`,
	})

	policy := NewConsecutiveViolationPolicy("block-profanity", 0.9, []string{"profanity"}, maxStrikes)

	return policy, classifier
}
