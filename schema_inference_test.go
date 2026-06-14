package sop

import (
	"testing"
	"time"
)

func TestInferSchemaFromTypes_RecognizesTimeTimeAsDatetime(t *testing.T) {
	type orderValue struct {
		OrderDate time.Time `json:"order_date"`
	}

	result := InferSchemaFromTypes(struct{}{}, orderValue{})

	if got := result.Schema["order_date"]; got != "datetime" {
		t.Fatalf("expected order_date to infer as datetime, got %q", got)
	}
}

func TestInferSchemaFromTypes_UsesRuntimeMapKeyFields(t *testing.T) {
	result := InferSchemaFromTypes(map[string]any{
		"C1": "alpha",
		"C2": int64(7),
	}, map[string]any{
		"f1": "value",
	})

	if got := result.Schema["C1"]; got != "string" {
		t.Fatalf("expected key field C1 to infer as string, got %q", got)
	}
	if got := result.Schema["C2"]; got != "number" {
		t.Fatalf("expected key field C2 to infer as number, got %q", got)
	}
	if len(result.KeyFields) != 2 || result.KeyFields[0] != "C1" || result.KeyFields[1] != "C2" {
		t.Fatalf("expected key fields [C1 C2], got %+v", result.KeyFields)
	}
}
