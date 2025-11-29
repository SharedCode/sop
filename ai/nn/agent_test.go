package nn

import (
	"context"
	"testing"
)

func TestPerceptronAgent(t *testing.T) {
	// Create an agent for AND gate logic (2 inputs)
	agent := NewAgent(2, 0.1)
	ctx := context.Background()

	// Train AND gate
	// 0,0 -> 0
	// 0,1 -> 0
	// 1,0 -> 0
	// 1,1 -> 1
	trainingData := []struct {
		input  string
		target string
	}{
		{"0,0", "0"},
		{"0,1", "0"},
		{"1,0", "0"},
		{"1,1", "1"},
	}

	// Train for a few epochs
	for i := 0; i < 100; i++ {
		for _, td := range trainingData {
			query := "train: " + td.input + " -> " + td.target
			_, err := agent.Ask(ctx, query)
			if err != nil {
				t.Fatalf("Training failed: %v", err)
			}
		}
	}

	// Test Predictions
	tests := []struct {
		input    string
		expected string
	}{
		{"0,0", "0.000000"},
		{"0,1", "0.000000"},
		{"1,0", "0.000000"},
		{"1,1", "1.000000"},
	}

	for _, tt := range tests {
		query := "predict: " + tt.input
		result, err := agent.Ask(ctx, query)
		if err != nil {
			t.Fatalf("Prediction failed: %v", err)
		}
		if result != tt.expected {
			t.Errorf("Input %s: expected %s, got %s", tt.input, tt.expected, result)
		}
	}
}
