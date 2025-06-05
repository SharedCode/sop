package encoding

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

// Global Default marshaller.
var DefaultMarshaler = NewMarshaler()

// Global BlobMarshaler takes care of packing and unpacking to/from blob object & byte array.
// You can replace with your desired Marshaler implementation if needed. Defaults to use JSON Marshal.
var BlobMarshaler = DefaultMarshaler

type defaultMarshaler struct{}

// Returns the default marshaller which uses the golang's json package.
// Json encoding was chosen as default because it supports "streaming" feature,
// which will be an enabler on future releases, for example when the B-Tree
// supports persistence of an item value's data to a separate segment(than the node's)
// and it is huge, B-Tree may support "streaming" access to this data and it may use
// Json's streaming feature.
//
// Streaming use-case: 2GB movie or a huge(2GB) data graph.
func NewMarshaler() Marshaler {
	return &defaultMarshaler{}
}

// Encodes any object to a byte array.
func (m defaultMarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decodes a byte array back to its Object type.
func (m defaultMarshaler) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Marshal that can do byte array pass-through.
func Marshal[T any](v T) ([]byte, error) {
	switch any(v).(type) {
	case *[]byte:
		var intf interface{}
		var v2 interface{} = v
		var ba *[]byte = v2.(*[]byte)
		intf = *ba
		return intf.([]byte), nil
	case []byte:
		var intf interface{}
		intf = v
		return intf.([]byte), nil
	default:
		return BlobMarshaler.Marshal(v)
	}
}

// Unmarshal that can do byte array pass-through.
func Unmarshal[T any](ba []byte, v *T) error {
	switch any(v).(type) {
	case *[]byte:
		var intf interface{}
		intf = ba
		*v = intf.(T)
		return nil
	case []byte:
		var intf interface{}
		intf = ba
		*v = intf.(T)
		return nil
	default:
		if err := BlobMarshaler.Unmarshal(ba, v); err != nil {
			return err
		}
		return nil
	}
}
