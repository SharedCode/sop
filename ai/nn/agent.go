package nn

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/sharedcode/sop/ai"
)

// Agent wraps a Perceptron to implement the ai.Agent interface.
// It interprets queries as input vectors or training commands.
type Agent struct {
	P *Perceptron
}

// NewAgent creates a new Perceptron Agent.
func NewAgent(inputSize int, lr float64) *Agent {
	return &Agent{
		P: NewPerceptron(inputSize, lr),
	}
}

// Search is not implemented for Perceptron, returns empty.
func (a *Agent) Search(ctx context.Context, query string, limit int) ([]ai.Hit[any], error) {
	return nil, nil
}

// Ask processes the query.
// Format: "predict: 1.0, 0.5" or "train: 1.0, 0.5 -> 1.0"
func (a *Agent) Ask(ctx context.Context, query string) (string, error) {
	if strings.HasPrefix(query, "predict:") {
		inputStr := strings.TrimPrefix(query, "predict:")
		inputs, err := parseFloats(inputStr)
		if err != nil {
			return "", err
		}
		result := a.P.Predict(inputs)
		return fmt.Sprintf("%f", result), nil
	}

	if strings.HasPrefix(query, "train:") {
		parts := strings.Split(query, "->")
		if len(parts) != 2 {
			return "", fmt.Errorf("invalid train format. Use: train: inputs -> target")
		}
		inputStr := strings.TrimPrefix(parts[0], "train:")
		inputs, err := parseFloats(inputStr)
		if err != nil {
			return "", err
		}

		target, err := strconv.ParseFloat(strings.TrimSpace(parts[1]), 64)
		if err != nil {
			return "", fmt.Errorf("invalid target: %v", err)
		}

		errVal := a.P.Train(inputs, target)
		return fmt.Sprintf("Trained. Error: %f", errVal), nil
	}

	return "", fmt.Errorf("unknown command. Use 'predict:' or 'train:'")
}

func parseFloats(s string) ([]float64, error) {
	parts := strings.Split(s, ",")
	floats := make([]float64, 0, len(parts))
	for _, p := range parts {
		f, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return nil, err
		}
		floats = append(floats, f)
	}
	return floats, nil
}
