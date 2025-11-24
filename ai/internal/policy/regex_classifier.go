package policy

import (
	"regexp"

	"github.com/sharedcode/sop/ai/internal/port"
)

type RegexClassifier struct {
	name     string
	patterns map[string]*regexp.Regexp
}

func NewRegexClassifier(name string, patterns map[string]string) *RegexClassifier {
	rc := &RegexClassifier{
		name:     name,
		patterns: make(map[string]*regexp.Regexp),
	}
	for label, pat := range patterns {
		rc.patterns[label] = regexp.MustCompile(pat)
	}
	return rc
}

func (rc *RegexClassifier) Name() string { return rc.name }

func (rc *RegexClassifier) Classify(sample port.ContentSample) ([]port.Label, error) {
	var labels []port.Label
	for label, re := range rc.patterns {
		if re.MatchString(sample.Text) {
			labels = append(labels, port.Label{
				Name:   label,
				Score:  1.0, // Binary match
				Source: rc.name,
			})
		}
	}
	return labels, nil
}
