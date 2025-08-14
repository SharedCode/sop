package main

import (
	"fmt"
	"math"
	"math/rand"
)

// PerceptronNode represents a single artificial neuron.
type PerceptronNode struct {
	Weights []float64
	Bias    float64
}

// NewPerceptronNode initializes a new perceptron node with random weights and bias.
func NewPerceptronNode(numInputs int) *PerceptronNode {

	weights := make([]float64, numInputs)
	for i := 0; i < numInputs; i++ {
		weights[i] = rand.Float64() // Random float between 0.0 and 1.0
	}

	return &PerceptronNode{
		Weights: weights,
		Bias:    rand.Float64(),
	}
}

// sigmoidActivation computes the sigmoid activation function.
func (p *PerceptronNode) sigmoidActivation(x float64) float64 {
	return 1.0 / (1.0 + math.Exp(-x))
}

// reluActivation computes the ReLU activation function.
func (p *PerceptronNode) reluActivation(x float64) float64 {
	return math.Max(0, x)
}

// Forward performs the forward pass of the perceptron.
func (p *PerceptronNode) Forward(inputs []float64, activationType string) (float64, error) {
	if len(inputs) != len(p.Weights) {
		return 0, fmt.Errorf("number of inputs (%d) must match the number of weights (%d)", len(inputs), len(p.Weights))
	}

	weightedSum := 0.0
	for i := 0; i < len(inputs); i++ {
		weightedSum += inputs[i] * p.Weights[i]
	}
	weightedSum += p.Bias

	var output float64
	switch activationType {
	case "sigmoid":
		output = p.sigmoidActivation(weightedSum)
	case "relu":
		output = p.reluActivation(weightedSum)
	default:
		return 0, fmt.Errorf("unsupported activation type: %s. Choose 'sigmoid' or 'relu'", activationType)
	}

	return output, nil
}

func main() {
	// Create a perceptron node with 3 inputs
	node := NewPerceptronNode(3)
	fmt.Printf("Initial weights: %v\n", node.Weights)
	fmt.Printf("Initial bias: %f\n", node.Bias)
	fmt.Println("------------------------------")

	// Define some sample inputs
	inputData1 := []float64{0.5, 0.2, 0.8}
	inputData2 := []float64{1.0, 0.1, 0.0}

	// Calculate output using sigmoid activation
	outputSigmoid1, err := node.Forward(inputData1, "sigmoid")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("Output for %v (Sigmoid): %.4f\n", inputData1, outputSigmoid1)
	}

	outputSigmoid2, err := node.Forward(inputData2, "sigmoid")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("Output for %v (Sigmoid): %.4f\n", inputData2, outputSigmoid2)
	}
	fmt.Println("------------------------------")

	// Calculate output using ReLU activation
	outputReLU1, err := node.Forward(inputData1, "relu")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("Output for %v (ReLU): %.4f\n", inputData1, outputReLU1)
	}

	outputReLU2, err := node.Forward(inputData2, "relu")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Printf("Output for %v (ReLU): %.4f\n", inputData2, outputReLU2)
	}

	// Example with more inputs
	nodeLarge := NewPerceptronNode(5)
	inputDataLarge := []float64{0.1, 0.3, 0.5, 0.7, 0.9}
	outputLarge, err := nodeLarge.Forward(inputDataLarge, "sigmoid")
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		fmt.Println("\n------------------------------")
		fmt.Printf("Output for %v (Sigmoid, 5 inputs): %.4f\n", inputDataLarge, outputLarge)
	}
}
