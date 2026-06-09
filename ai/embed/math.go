package embed

import "math"

func NormalizeVector(v []float32) []float32 {
	var sum float32
	for _, val := range v {
		sum += val * val
	}

	norm := float32(math.Sqrt(float64(sum)))
	if norm == 0 {
		return v
	}

	normalized := make([]float32, len(v))
	for i, val := range v {
		normalized[i] = val / norm
	}
	return normalized
}
