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

type defaultMarshaller struct{}

// Returns the default marshaller which uses the golang's json package.
// Json encoding was chosen as default because it supports "streaming" feature,
// which will be an enabler on future releases, for example when the B-Tree
// supports persistence of an item value's data to a separate segment(than the node's)
// and it is huge, B-Tree may support "streaming" access to this data and it may use
// Json's streaming feature.
//
// Streaming use-case: 2GB movie or a huge(2GB) data graph.
func NewMarshaler() Marshaler {
	return &defaultMarshaller{}
}

// Encodes any object to a byte array.
func (m defaultMarshaller) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decodes a byte array back to its Object type.
func (m defaultMarshaller) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
