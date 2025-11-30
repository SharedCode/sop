package policy

import (
	"context"

	"github.com/sharedcode/sop/ai"
)

// AllowAll is a policy that allows everything.
// It is useful as a default or fallback policy.
type AllowAll struct{ id string }

// NewAllow creates a new AllowAll policy with the given ID.
func NewAllow(id string) *AllowAll { return &AllowAll{id: id} }

// Evaluate always returns an "allow" decision.
func (a *AllowAll) Evaluate(ctx context.Context, stage string, sample ai.ContentSample, labels []ai.Label) (ai.PolicyDecision, error) {
	return ai.PolicyDecision{Action: "allow", PolicyID: a.id}, nil
}
