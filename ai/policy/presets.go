package policy

import "github.com/sharedcode/sop/ai"

// NewProfanityGuardrail returns a configured policy and classifier for profanity filtering.
// Note: The returned policy is stateful (tracks strikes). Use a new instance per user session.
func NewProfanityGuardrail(maxStrikes int) (ai.PolicyEngine, ai.Classifier) {
	classifier := NewRegexClassifier("profanity-filter", map[string]string{
		"profanity": `(?i)\b(slut|idiot|stupid|damn|hell|ass|bitch|fuck)\b`,
	})

	policy := NewConsecutiveViolationPolicy("block-profanity", 0.9, []string{"profanity"}, maxStrikes)

	return policy, classifier
}
