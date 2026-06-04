package sop

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"

	"github.com/google/uuid"
)

// FlattenForSchema converts key and value of any type into a flat map[string]any
// suitable for schema inference. Uses JSON marshaling to handle structs.
func FlattenForSchema(key any, value any) map[string]any {
	result := make(map[string]any)

	// Helper to flatten a single value into result map
	flatten := func(v any, isKey bool) {
		if v == nil {
			return
		}

		// Try to convert to map via JSON for structs
		vType := reflect.TypeOf(v)
		vKind := vType.Kind()

		// Handle primitives directly
		switch vKind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
			reflect.Float32, reflect.Float64,
			reflect.Bool, reflect.String:
			if isKey {
				result["key"] = v
			} else {
				result["value"] = v
			}
			return
		}

		// Handle UUID type
		if _, ok := v.(UUID); ok {
			if isKey {
				result["key"] = v
			} else {
				result["value"] = v
			}
			return
		}

		// Try JSON marshaling for structs and maps
		b, err := json.Marshal(v)
		if err == nil {
			var m map[string]any
			if err := json.Unmarshal(b, &m); err == nil {
				for k, val := range m {
					result[k] = val
				}
				return
			}
		}

		// Fallback: store as-is
		if isKey {
			result["key"] = v
		} else {
			result["value"] = v
		}
	}

	flatten(key, true)
	flatten(value, false)

	return result
}

// InferSchema inspects a map and returns a simplified type definition (e.g. {"id": "uuid", "age": "number"}).
func InferSchema(item map[string]any) map[string]string {
	schema := make(map[string]string)
	for k, v := range item {
		schema[k] = InferSchemaType(v)
	}
	return schema
}

// InferSchemaType returns a string representation of the type of a value for schema inference.
func InferSchemaType(v any) string {
	if v == nil {
		return "object"
	}

	switch val := v.(type) {
	case string:
		if _, err := uuid.Parse(val); err == nil {
			return "uuid"
		}
		return "string"
	case UUID:
		return "uuid"
	case uuid.UUID:
		return "uuid"
	case int, int64, int32, int8, int16, uint, uint64, uint32, uint8, uint16, float64, float32:
		return "number"
	case bool:
		return "boolean"
	case map[string]any:
		return "object"
	case []any:
		return "list"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// FormatSchema formats a schema map as a sorted string like "{field: type, ...}".
func FormatSchema(schema map[string]string) string {
	var keys []string
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var pairs []string
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s: %s", k, schema[k]))
	}
	return fmt.Sprintf("{%s}", joinStrings(pairs, ", "))
}

// Helper to join strings
func joinStrings(items []string, sep string) string {
	if len(items) == 0 {
		return ""
	}
	if len(items) == 1 {
		return items[0]
	}
	res := items[0]
	for i := 1; i < len(items); i++ {
		res += sep + items[i]
	}
	return res
}
