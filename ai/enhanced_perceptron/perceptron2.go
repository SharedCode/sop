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
	// Learning rate determines the step size during weight updates.
	LearningRate float64
}

// NewPerceptronNode initializes a new perceptron node with random weights and bias.
func NewPerceptronNode(numInputs int, learningRate float64) *PerceptronNode {
	weights := make([]float64, numInputs)
	for i := 0; i < numInputs; i++ {
		// Initialize weights to small random values, potentially negative too
		weights[i] = rand.Float64()*2 - 1 // Random float between -1.0 and 1.0
	}

	return &PerceptronNode{
		Weights:      weights,
		Bias:         rand.Float64()*2 - 1, // Random float between -1.0 and 1.0
		LearningRate: learningRate,
	}
}

// StepActivation is a simple step function, common for basic perceptrons.
// It outputs 1 if the input is positive, otherwise 0.
func (p *PerceptronNode) StepActivation(x float64) float64 {
	if x > 0 {
		return 1.0
	}
	return 0.0
}

// Forward performs the forward pass of the perceptron.
// It returns the raw weighted sum before activation, and the activated output.
func (p *PerceptronNode) Forward(inputs []float64) (float64, float64, error) {
	if len(inputs) != len(p.Weights) {
		return 0, 0, fmt.Errorf("number of inputs (%d) must match the number of weights (%d)", len(inputs), len(p.Weights))
	}

	weightedSum := 0.0
	for i := 0; i < len(inputs); i++ {
		weightedSum += inputs[i] * p.Weights[i]
	}
	weightedSum += p.Bias

	output := p.StepActivation(weightedSum)

	return weightedSum, output, nil
}

// Train adjusts the perceptron's weights and bias based on a single training example.
// This uses the perceptron learning rule.
func (p *PerceptronNode) Train(inputs []float64, targetOutput float64) error {
	_, predictedOutput, err := p.Forward(inputs)
	if err != nil {
		return err
	}

	// Calculate the error
	error := targetOutput - predictedOutput

	// Only update if there's an error
	if error != 0 {
		// Adjust weights
		for i := 0; i < len(p.Weights); i++ {
			p.Weights[i] += p.LearningRate * error * inputs[i]
		}
		// Adjust bias
		p.Bias += p.LearningRate * error
	}
	return nil
}

func main() {
	// Initialize the perceptron with 2 inputs (for x and y) and a learning rate
	learningRate := 0.1
	perceptron := NewPerceptronNode(2, learningRate)
	fmt.Printf("Initial Weights: %.4f, %.4f, Bias: %.4f\n", perceptron.Weights[0], perceptron.Weights[1], perceptron.Bias)
	fmt.Println("--------------------------------------------------")

	// Training data for our goal: (x, y) -> is y > x?
	// Target Output: 1 if y > x, 0 if y <= x
	trainingData := []struct {
		inputs []float64
		target float64
	}{
		{[]float64{1.0, 2.0}, 1.0},  // Above the line (y > x)
		{[]float64{2.0, 1.0}, 0.0},  // Below the line (y <= x)
		{[]float64{0.5, 0.3}, 0.0},  // Below
		{[]float64{0.3, 0.5}, 1.0},  // Above
		{[]float64{1.0, 1.0}, 0.0},  // On the line (treated as 0)
		{[]float64{-1.0, 0.0}, 1.0}, // Above
		{[]float64{0.0, -1.0}, 0.0}, // Below
		{[]float64{3.0, 4.0}, 1.0},  // Above
		{[]float64{4.0, 3.0}, 0.0},  // Below
	}

	numIterations := 20 // We'll train for 20 iterations

	fmt.Println("Starting training...")
	for i := 0; i < numIterations; i++ {
		totalError := 0.0
		fmt.Printf("\n--- Iteration %d ---\n", i+1)
		for _, data := range trainingData {
			// Train the perceptron with current data
			err := perceptron.Train(data.inputs, data.target)
			if err != nil {
				fmt.Printf("Error training: %v\n", err)
				continue
			}

			// Test after potential update to see current prediction and error
			_, predicted, _ := perceptron.Forward(data.inputs)
			currentError := data.target - predicted
			totalError += math.Abs(currentError) // Accumulate absolute error

			fmt.Printf("Input: %v, Target: %.0f, Predicted: %.0f, Error: %.0f. Weights: [%.4f %.4f], Bias: %.4f\n",
				data.inputs, data.target, predicted, currentError, perceptron.Weights[0], perceptron.Weights[1], perceptron.Bias)
		}
		fmt.Printf("Total error for iteration %d: %.0f\n", i+1, totalError)
		// If total error is 0, it means it classified all training data correctly
		if totalError == 0 {
			fmt.Println("\nPerceptron converged! All training data correctly classified.")
			break
		}
	}

	fmt.Println("\n--------------------------------------------------")
	fmt.Println("Training finished. Final Perceptron State:")
	fmt.Printf("Final Weights: %.4f, %.4f, Bias: %.4f\n", perceptron.Weights[0], perceptron.Weights[1], perceptron.Bias)

	// Test the trained perceptron with new data
	fmt.Println("\nTesting with new data:")
	testPoints := []struct {
		inputs []float64
		// expected string for user understanding, not used in calculation
		expectedClass string
	}{
		{[]float64{3.0, 4.0}, "Above"},   // 4 > 3
		{[]float64{5.0, 2.0}, "Below"},   // 2 <= 5
		{[]float64{2.0, 2.0}, "On Line"}, // 2 <= 2
		{[]float64{-2.0, -1.0}, "Above"}, // -1 > -2
		{[]float64{-1.0, -2.0}, "Below"}, // -2 <= -1
	}

	for _, point := range testPoints {
		_, prediction, err := perceptron.Forward(point.inputs)
		if err != nil {
			fmt.Printf("Error predicting for %v: %v\n", point.inputs, err)
			continue
		}
		fmt.Printf("Point %v (Expected: %s): Predicted -> %.0f (%s)\n", point.inputs, point.expectedClass, prediction, map[float64]string{0: "Below/On Line", 1: "Above Line"}[prediction])
	}
}
