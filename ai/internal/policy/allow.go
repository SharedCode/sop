package policy

import "github.com/sharedcode/sop/ai/internal/port"

type AllowAll struct{ id string }

func NewAllow(id string) *AllowAll { return &AllowAll{id: id} }

func (a *AllowAll) Evaluate(stage string, sample port.ContentSample, labels []port.Label) (port.PolicyDecision, error) {
	return port.PolicyDecision{Action: "allow", PolicyID: a.id}, nil
}
