package sop

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestInferType_Scalars(t *testing.T) {
	tests := []struct {
		name string
		in   any
		want string
	}{
		{"nil", nil, "string"},
		{"sop uuid", NewUUID(), "uuid"},
		{"google uuid", uuid.New(), "uuid"},
		{"time", time.Now(), "time"},
		{"plain string", "hello", "string"},
		{"uuid-shaped string", "123e4567-e89b-12d3-a456-426614174000", "uuid"},
		{"empty string", "", "string"},
		{"int", 1, "int"},
		{"int8", int8(1), "int"},
		{"uint64", uint64(1), "int"},
		{"whole float64 from json", float64(3), "int"},
		{"fractional float64", 3.5, "float64"},
		{"float32", float32(1.5), "float64"},
		{"float32 whole", float32(2), "float64"},
		{"bool", true, "bool"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, isArray := InferType(tt.in)
			if got != tt.want || isArray {
				t.Fatalf("InferType(%v) = (%q, %v), want (%q, false)", tt.in, got, isArray, tt.want)
			}
		})
	}
}
