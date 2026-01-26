package agent

import (
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// inferSchema inspects a map and returns a simplified type definition (e.g. {"id": "string", "age": "number"}).
func inferSchema(item map[string]any) map[string]string {
	schema := make(map[string]string)
	for k, v := range item {
		schema[k] = inferType(v)
	}
	return schema
}

func inferType(v any) string {
	if v == nil {
		return "null"
	}
	switch val := v.(type) {
	case string:
		if _, err := uuid.Parse(val); err == nil {
			return "uuid"
		}
		return "string"
	case uuid.UUID:
		return "uuid"
	case int, int64, int32, float64, float32:
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

func formatSchema(schema map[string]string) string {
	var keys []string
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var pairs []string
	for _, k := range keys {
		pairs = append(pairs, fmt.Sprintf("%s: %s", k, schema[k]))
	}
	return fmt.Sprintf("{%s}", join(pairs, ", "))
}

// Helper strict join
func join(items []string, sep string) string {
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
