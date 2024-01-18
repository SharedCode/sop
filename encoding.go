package sop

import (
	"encoding/json"
)

// Marshaler interface specifies encoding to byte array and back to the object.
type Marshaler interface {
	// Encodes any object to byte array.
	Marshal(v any) ([]byte, error)
	// Decodes byte array back to its Object type.
	Unmarshal(data []byte, v any) error
}

type defaultMardhaller struct {}

// Returns the default marshaller which uses the golang's json package.
func NewMarshaler() Marshaler {
	return &defaultMardhaller{}
}

// Encodes any object to a byte array.
func (m defaultMardhaller)Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decodes a byte array back to its Object type.
func (m defaultMardhaller)Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
