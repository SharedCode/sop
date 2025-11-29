package nn

import (
	"encoding/json"
	"math/rand"
	"os"
)

// Perceptron is a simple binary classifier.
type Perceptron struct {
	Weights []float64
	Bias    float64
	LR      float64 // Learning Rate
}

// NewPerceptron creates a new perceptron with random weights.
func NewPerceptron(inputSize int, lr float64) *Perceptron {
	weights := make([]float64, inputSize)
	for i := range weights {
		weights[i] = rand.Float64()*2 - 1 // -1 to 1
	}
	return &Perceptron{
		Weights: weights,
		Bias:    rand.Float64()*2 - 1,
		LR:      lr,
	}
}

// Predict returns 1.0 if activation >= 0, else 0.0.
func (p *Perceptron) Predict(inputs []float64) float64 {
	return p.Activate(p.WeightedSum(inputs))
}

// WeightedSum calculates the dot product of inputs and weights plus bias.
func (p *Perceptron) WeightedSum(inputs []float64) float64 {
	sum := p.Bias
	for i, w := range p.Weights {
		if i < len(inputs) {
			sum += w * inputs[i]
		}
	}
	return sum
}

// Activate applies the Heaviside Step function.
func (p *Perceptron) Activate(sum float64) float64 {
	if sum >= 0 {
		return 1.0
	}
	return 0.0
}

// Train updates weights based on the error.
// Returns the error (target - prediction).
func (p *Perceptron) Train(inputs []float64, target float64) float64 {
	prediction := p.Predict(inputs)
	error := target - prediction
	p.Update(inputs, error)
	return error
}

// Update adjusts weights and bias based on the error.
func (p *Perceptron) Update(inputs []float64, error float64) {
	// Update weights: w = w + lr * error * input
	for i := range p.Weights {
		if i < len(inputs) {
			p.Weights[i] += p.LR * error * inputs[i]
		}
	}
	// Update bias: b = b + lr * error
	p.Bias += p.LR * error
}

// Save persists the perceptron's weights and bias to a JSON file.
func (p *Perceptron) Save(path string) error {
	data, err := json.MarshalIndent(p, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// LoadPerceptron loads a perceptron from a JSON file.
func LoadPerceptron(path string) (*Perceptron, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var p Perceptron
	if err := json.Unmarshal(data, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
