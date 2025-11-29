package nn

import (
	"context"
	"os"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestPerceptronPersistenceWithModelStore(t *testing.T) {
	// Setup temporary directory for model store
	tmpDir := "./test_models_persistence"
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	store, err := ai.NewFileModelStore(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create model store: %v", err)
	}

	ctx := context.Background()

	// 1. Create and Train a Perceptron (OR gate)
	p := NewPerceptron(2, 0.1)
	inputs := [][]float64{
		{0, 0},
		{0, 1},
		{1, 0},
		{1, 1},
	}
	targets := []float64{0, 1, 1, 1}

	// Train for a bit
	for epoch := 0; epoch < 2000; epoch++ {
		for i, input := range inputs {
			p.Train(input, targets[i])
		}
	}

	// Verify it learned
	if p.Predict([]float64{0, 1}) != 1.0 {
		t.Fatal("Perceptron failed to learn OR gate before saving")
	}

	// 2. Save it using the ModelStore
	modelName := "or_gate_skill"
	if err := store.Save(ctx, modelName, p); err != nil {
		t.Fatalf("Failed to save model: %v", err)
	}

	// 3. Load it back into a new instance
	var loadedP Perceptron
	if err := store.Load(ctx, modelName, &loadedP); err != nil {
		t.Fatalf("Failed to load model: %v", err)
	}

	// 4. Verify loaded model works exactly as the original
	// Check internal state
	if loadedP.Bias != p.Bias {
		t.Errorf("Bias mismatch. Original: %v, Loaded: %v", p.Bias, loadedP.Bias)
	}
	if len(loadedP.Weights) != len(p.Weights) {
		t.Fatalf("Weights length mismatch")
	}
	for i, w := range p.Weights {
		if loadedP.Weights[i] != w {
			t.Errorf("Weight %d mismatch. Original: %v, Loaded: %v", i, w, loadedP.Weights[i])
		}
	}

	// Check behavior
	if loadedP.Predict([]float64{0, 1}) != 1.0 {
		t.Errorf("Loaded perceptron failed to predict correctly for [0, 1]")
	}
	if loadedP.Predict([]float64{0, 0}) != 0.0 {
		t.Errorf("Loaded perceptron failed to predict correctly for [0, 0]")
	}

	// 5. Test List
	names, err := store.List(ctx)
	if err != nil {
		t.Fatalf("Failed to list models: %v", err)
	}
	if len(names) != 1 || names[0] != modelName {
		t.Errorf("List failed. Expected [%s], got %v", modelName, names)
	}

	// 6. Test Delete
	if err := store.Delete(ctx, modelName); err != nil {
		t.Fatalf("Failed to delete model: %v", err)
	}
	names, _ = store.List(ctx)
	if len(names) != 0 {
		t.Errorf("Delete failed. Expected empty list, got %v", names)
	}
}
