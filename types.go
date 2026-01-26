package sop

import (
	"reflect"
	"time"

	"github.com/google/uuid"
)

// InferType returns the simplified type name (e.g. "string", "int", "uuid") and whether it's an array.
// This is used for UI display and loose type checking.
func InferType(v any) (string, bool) {
	if v == nil {
		return "string", false
	}

	// Handle sop.UUID and uuid.UUID explicitly
	switch v.(type) {
	case UUID, uuid.UUID:
		return "uuid", false
	case time.Time:
		return "time", false
	case string:
		s := v.(string)
		if _, err := ParseUUID(s); err == nil {
			return "uuid", false
		}
		return "string", false
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, uintptr:
		return "int", false
	case float32, float64:
		// Handle JSON number (float64) which might be int
		if f, ok := v.(float64); ok {
			if float64(int64(f)) == f {
				return "int", false
			}
			return "float64", false
		}
		return "float64", false
	case bool:
		return "bool", false
	}

	val := reflect.ValueOf(v)
	kind := val.Kind()

	if kind == reflect.Map {
		return "map", false
	}

	if kind == reflect.Slice || kind == reflect.Array {
		// Check for byte slice -> blob
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return "blob", false // Blob is treated as a type, not array of bytes
		}
		// Otherwise it's an array of something
		if val.Len() > 0 {
			elem := val.Index(0).Interface()
			t, _ := InferType(elem)
			return t, true
		}
		return "string", true
	}

	return "string", false
}
