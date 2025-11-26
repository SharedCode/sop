package policy

import (
	"regexp"

	"github.com/sharedcode/sop/ai"
)

// RegexClassifier classifies text based on regular expression patterns.
type RegexClassifier struct {
	name     string
	patterns map[string]*regexp.Regexp
}

// NewRegexClassifier creates a new classifier with the given name and patterns.
// patterns is a map where the key is the label name and the value is the regex pattern.
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

// Name returns the name of the classifier.
func (rc *RegexClassifier) Name() string { return rc.name }

// Classify checks the sample text against the regex patterns and returns matching labels.
func (rc *RegexClassifier) Classify(sample ai.ContentSample) ([]ai.Label, error) {
	var labels []ai.Label
	for label, re := range rc.patterns {
		if re.MatchString(sample.Text) {
			labels = append(labels, ai.Label{
				Name:   label,
				Score:  1.0, // Binary match
				Source: rc.name,
			})
		}
	}
	return labels, nil
}
