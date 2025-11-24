package port

type ContentSample struct {
	Text string
	Meta map[string]any
}

type Label struct {
	Name   string
	Score  float32
	Source string
}

type Classifier interface {
	Name() string
	Classify(sample ContentSample) ([]Label, error)
}

type PolicyDecision struct {
	Action   string
	Reasons  []string
	PolicyID string
}

type PolicyEngine interface {
	Evaluate(stage string, sample ContentSample, labels []Label) (PolicyDecision, error)
}
