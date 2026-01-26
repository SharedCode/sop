package agent

import (
	"testing"

	"github.com/google/uuid"
)

func TestInferType_UUID(t *testing.T) {
	// Real UUID (native type)
	id := uuid.New()
	typ := inferType(id)
	if typ != "uuid" {
		t.Errorf("Expected 'uuid' for native UUID value, got '%s'", typ)
	}

	// Valid UUID String
	u := uuid.NewString()
	typ = inferType(u)
	if typ != "uuid" {
		t.Errorf("Expected 'uuid' for value '%s', got '%s'", u, typ)
	}

	// Normal String
	s := "quadchopper"
	typ = inferType(s)
	if typ != "string" {
		t.Errorf("Expected 'string' for value '%s', got '%s'", s, typ)
	}

	// Empty String (parse fails)
	typ = inferType("")
	if typ != "string" {
		t.Errorf("Expected 'string' for empty value, got '%s'", typ)
	}
}

func TestInferSchema_UUID(t *testing.T) {
	u := uuid.NewString()
	item := map[string]any{
		"id":    u,
		"name":  "Widget",
		"price": 10.5,
	}

	schema := inferSchema(item)

	if schema["id"] != "uuid" {
		t.Errorf("Expected schema['id'] to be 'uuid', got '%s'", schema["id"])
	}
	if schema["name"] != "string" {
		t.Errorf("Expected schema['name'] to be 'string', got '%s'", schema["name"])
	}
	if schema["price"] != "number" {
		t.Errorf("Expected schema['price'] to be 'number', got '%s'", schema["price"])
	}
}
