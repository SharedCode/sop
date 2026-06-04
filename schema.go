package sop

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// InferSchemaFromTypes uses reflection to directly inspect the types of key and value,
// providing more accurate schema information than JSON marshaling.
// This is the preferred method when generic type information is available.
// Maintains Key/Value separation with "Key." and "Value." prefixes for SQL generation.
func InferSchemaFromTypes(key any, value any) map[string]string {
	schema := make(map[string]string)

	// Infer key schema with "Key." prefix
	if key != nil {
		keyType := reflect.TypeOf(key)
		keySchema := reflectTypeToSchemaWithPrefix(keyType, "Key")
		for k, v := range keySchema {
			schema[k] = v
		}
	}

	// Infer value schema with "Value." prefix
	if value != nil {
		// Handle pointer to value (common in B-tree Item)
		val := reflect.ValueOf(value)
		if val.Kind() == reflect.Ptr {
			if !val.IsNil() {
				value = val.Elem().Interface()
				val = reflect.ValueOf(value)
			} else {
				// Nil pointer - inspect the type it points to
				valueType := reflect.TypeOf(value).Elem()
				valueSchema := reflectTypeToSchemaWithPrefix(valueType, "Value")
				for k, v := range valueSchema {
					schema[k] = v
				}
				return schema
			}
		}

		// For map types with runtime values (like map[string]any), inspect the actual contents
		if val.Kind() == reflect.Map {
			valueSchema := reflectValueMapToSchemaWithPrefix(val, "Value")
			for k, v := range valueSchema {
				schema[k] = v
			}
		} else {
			// Use type-based inference for other types
			valueType := reflect.TypeOf(value)
			valueSchema := reflectTypeToSchemaWithPrefix(valueType, "Value")
			for k, v := range valueSchema {
				schema[k] = v
			}
		}
	}

	return schema
}

// reflectTypeToSchemaWithPrefix converts a reflect.Type to a schema map with proper Key/Value prefixing.
func reflectTypeToSchemaWithPrefix(t reflect.Type, prefix string) map[string]string {
	schema := make(map[string]string)

	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.Struct:
		// Special case for UUID
		if t.PkgPath() == "github.com/sharedcode/sop" && t.Name() == "UUID" {
			schema[prefix] = "uuid"
			return schema
		}
		if t.PkgPath() == "github.com/google/uuid" && t.Name() == "UUID" {
			schema[prefix] = "uuid"
			return schema
		}

		// Inspect struct fields with prefix
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)

			// Skip unexported fields
			if !field.IsExported() {
				continue
			}

			// Get field name from json tag if present, otherwise use field name
			fieldName := field.Name
			if jsonTag := field.Tag.Get("json"); jsonTag != "" {
				parts := strings.Split(jsonTag, ",")
				if parts[0] != "" && parts[0] != "-" {
					fieldName = parts[0]
				}
			}

			// Use "Key.field" or "Value.field" format
			prefixedName := fmt.Sprintf("%s.%s", prefix, strings.ToLower(fieldName))
			schema[prefixedName] = reflectTypeToString(field.Type)
		}

	case reflect.Map:
		// For maps without runtime data, just mark as object
		schema[prefix] = "object"

	case reflect.Slice, reflect.Array:
		schema[prefix] = "list"

	default:
		// Primitive types - use the prefix directly (e.g., "Key" or "Value")
		schema[prefix] = reflectTypeToString(t)
	}

	return schema
}

// reflectValueMapToSchemaWithPrefix inspects a map value at runtime with proper Key/Value prefixing.
func reflectValueMapToSchemaWithPrefix(mapVal reflect.Value, prefix string) map[string]string {
	schema := make(map[string]string)

	if !mapVal.IsValid() || mapVal.IsNil() {
		return schema
	}

	// Iterate over map keys
	iter := mapVal.MapRange()
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()

		// Only handle string keys
		if key.Kind() == reflect.String {
			fieldName := strings.ToLower(key.String())
			// Use "Key.field" or "Value.field" format
			prefixedName := fmt.Sprintf("%s.%s", prefix, fieldName)
			schema[prefixedName] = inferSchemaTypeFromValue(val)
		}
	}

	return schema
}

// inferSchemaTypeFromValue inspects a reflect.Value to determine its schema type.
func inferSchemaTypeFromValue(v reflect.Value) string {
	if !v.IsValid() {
		return "object"
	}

	// Unwrap interface
	if v.Kind() == reflect.Interface {
		if v.IsNil() {
			return "object"
		}
		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		// Check if it's a UUID string
		if v.String() != "" {
			if _, err := uuid.Parse(v.String()); err == nil {
				return "uuid"
			}
		}
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "number"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "number"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.Map:
		return "object"
	case reflect.Slice, reflect.Array:
		return "list"
	case reflect.Struct:
		// Check for UUID types
		if v.Type().PkgPath() == "github.com/sharedcode/sop" && v.Type().Name() == "UUID" {
			return "uuid"
		}
		if v.Type().PkgPath() == "github.com/google/uuid" && v.Type().Name() == "UUID" {
			return "uuid"
		}
		return "object"
	default:
		return "object"
	}
}

// reflectTypeToString converts a reflect.Type to a schema type string.
func reflectTypeToString(t reflect.Type) string {
	// Handle pointer types
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "number"
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "number"
	case reflect.Float32, reflect.Float64:
		return "number"
	case reflect.Bool:
		return "boolean"
	case reflect.Struct:
		// Check for UUID types
		if t.PkgPath() == "github.com/sharedcode/sop" && t.Name() == "UUID" {
			return "uuid"
		}
		if t.PkgPath() == "github.com/google/uuid" && t.Name() == "UUID" {
			return "uuid"
		}
		return "object"
	case reflect.Map:
		return "object"
	case reflect.Slice, reflect.Array:
		return "list"
	case reflect.Interface:
		return "object"
	default:
		return "object"
	}
}

// FlattenForSchema converts key and value of any type into a flat map[string]any
// suitable for schema inference. Uses JSON marshaling to handle structs.
// NOTE: InferSchemaFromTypes is preferred when type information is available.
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
