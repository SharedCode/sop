package encoding

import (
	"encoding/json"
)

// Marshaler defines methods to marshal/unmarshal values to/from byte slices.
type Marshaler interface {
	// Marshal encodes any object to a byte slice.
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes data back into the provided object pointer.
	Unmarshal(data []byte, v any) error
}

// DefaultMarshaler is the package-wide default marshaler using JSON encoding.
var DefaultMarshaler = NewMarshaler()

// BlobMarshaler handles encoding for blobs. It defaults to DefaultMarshaler but can be replaced.
var BlobMarshaler = DefaultMarshaler

type defaultMarshaler struct{}

// NewMarshaler returns a Marshaler implemented with the standard library JSON package.
// JSON is chosen as default for its streaming capabilities useful for large value payloads.
func NewMarshaler() Marshaler {
	return &defaultMarshaler{}
}

// Marshal encodes any object to a byte slice.
func (m defaultMarshaler) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Unmarshal decodes a byte slice back to its object type.
func (m defaultMarshaler) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

// Marshal is a generic helper that marshals values and passes through byte slices without copying.
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

// Unmarshal is a generic helper that unmarshals values and passes through byte slices without copying.
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
