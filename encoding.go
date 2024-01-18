package sop

import (
	"encoding/json"
)

// Marshaler interface specifies encoding to byte array and back to the object.
type Marshaler interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type defaultMardhaller struct {}

// Returns the default marshaller which uses the golang's json package.
func NewMarshaler() Marshaler {
	return &defaultMardhaller{}
}

// Marshal encodes any object to byte array.
func (m defaultMardhaller)Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal encodes a byte array back to any object.
func (m defaultMardhaller)Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
