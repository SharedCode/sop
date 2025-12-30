package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/jsondb"
)

func matchesKey(itemKey, filterKey any) (bool, string) {
	if filterKey == nil {
		return true, ""
	}

	// If filter is a map, it might be an operator map OR a composite key match
	if mFilter, ok := filterKey.(map[string]any); ok {
		// Check if it is an operator map (keys start with $)
		isOp := false
		for k := range mFilter {
			if strings.HasPrefix(k, "$") {
				isOp = true
				break
			}
		}
		if isOp {
			return matchOperator(itemKey, mFilter), ""
		}

		// If itemKey is also a map, check fields
		if mItem, ok := itemKey.(map[string]any); ok {
			for k, v := range mFilter {
				itemVal, exists := mItem[k]

				// Check for operator map
				if opMap, ok := v.(map[string]any); ok {
					// Check if it is an operator map (keys start with $)
					isOp := false
					for opK := range opMap {
						if strings.HasPrefix(opK, "$") {
							isOp = true
							break
						}
					}

					if isOp {
						if !exists {
							// If field missing, fail unless checking for $ne: null?
							// For simplicity, fail if missing.
							return false, ""
						}
						if !matchOperator(itemVal, opMap) {
							return false, ""
						}
						continue
					}
				}

				// Simple equality check. For nested objects, this might need recursion.
				// But for now, we assume flat keys or strict equality on values.
				if !exists || btree.Compare(itemVal, v) != 0 {
					return false, ""
				}
			}
			return true, ""
		}

		// Handle JSON string keys (e.g. from jsondb stores with Map keys)
		if sKey, ok := itemKey.(string); ok && strings.HasPrefix(strings.TrimSpace(sKey), "{") {
			var mItem map[string]any
			if err := json.Unmarshal([]byte(sKey), &mItem); err == nil {
				// Recurse with the map
				return matchesKey(mItem, mFilter)
			}
		}

		// NEW: Handle Primitive Key vs Map Filter mismatch
		// If itemKey is NOT a map, but filterKey IS a map (and not Op map).
		// And filterKey has exactly 1 entry.
		if len(mFilter) == 1 {
			for k, v := range mFilter {
				// Align types if possible
				alignedV := alignType(v, itemKey)
				matched, _ := matchesKey(itemKey, alignedV)
				if matched {
					return true, k
				}
				return false, ""
			}
		}
	}
	// If primitives, strict equality
	return btree.Compare(itemKey, filterKey) == 0, ""
}

func matchOperator(val any, opMap map[string]any) bool {
	for op, target := range opMap {
		// Align target type to val type
		alignedTarget := alignType(target, val)
		cmp := btree.Compare(val, alignedTarget)
		switch op {
		case "$eq":
			if cmp != 0 {
				return false
			}
		case "$ne":
			if cmp == 0 {
				return false
			}
		case "$gt":
			if cmp <= 0 {
				return false
			}
		case "$gte":
			if cmp < 0 {
				return false
			}
		case "$lt":
			if cmp >= 0 {
				return false
			}
		case "$lte":
			if cmp > 0 {
				return false
			}
		}
	}
	return true
}

func isMap(v any) bool {
	_, ok := v.(map[string]any)
	return ok
}

func getOptimizationKey(filter any) any {
	if filter == nil {
		return nil
	}
	if !isMap(filter) {
		return filter
	}
	m := filter.(map[string]any)
	if v, ok := m["$eq"]; ok {
		return v
	}
	if v, ok := m["$gte"]; ok {
		return v
	}
	if v, ok := m["$gt"]; ok {
		return v
	}

	return nil
}

type OrderedKey struct {
	m    map[string]any
	spec *jsondb.IndexSpecification
}

// MarshalJSON implements json.Marshaler to enforce field order.
func (o OrderedKey) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')

	// 1. Write indexed fields in order
	written := make(map[string]bool)
	first := true

	for _, field := range o.spec.IndexFields {
		if val, ok := o.m[field.FieldName]; ok {
			if !first {
				buf.WriteByte(',')
			}
			first = false

			kb, _ := json.Marshal(field.FieldName)
			buf.Write(kb)
			buf.WriteByte(':')
			vb, _ := json.Marshal(val)
			buf.Write(vb)

			written[field.FieldName] = true
		}
	}

	// 2. Write remaining fields sorted alphabetically
	var remaining []string
	for k := range o.m {
		if !written[k] {
			remaining = append(remaining, k)
		}
	}
	sort.Strings(remaining)

	for _, k := range remaining {
		if !first {
			buf.WriteByte(',')
		}
		first = false

		kb, _ := json.Marshal(k)
		buf.Write(kb)
		buf.WriteByte(':')
		vb, _ := json.Marshal(o.m[k])
		buf.Write(vb)
	}

	buf.WriteByte('}')
	return buf.Bytes(), nil
}

type OrderedMap struct {
	m    map[string]any
	keys []string
}

func (o OrderedMap) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range o.keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		buf.Write(kb)
		buf.WriteByte(':')
		vb, err := json.Marshal(o.m[k])
		if err != nil {
			return nil, err
		}
		buf.Write(vb)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func filterFields(item map[string]any, fields []string) any {
	if len(fields) == 0 {
		return item
	}

	// Helper to parse "field AS alias"
	aliasRe := regexp.MustCompile(`(?i)\s+as\s+`)
	parseFieldAlias := func(field string) (string, string) {
		if loc := aliasRe.FindStringIndex(field); loc != nil {
			source := strings.TrimSpace(field[:loc[0]])
			alias := strings.TrimSpace(field[loc[1]:])
			return source, alias
		}
		return field, field
	}

	// Check if the item looks like a standard SOP Store Item (has "key" and "value")
	_, hasKey := item["key"]
	_, hasValue := item["value"]

	// If it's NOT a standard SOP wrapper (e.g. result of a Join or arbitrary JSON),
	// treat it as a flat map and filter top-level fields.
	if !hasKey || !hasValue {
		om := OrderedMap{
			m:    make(map[string]any),
			keys: make([]string, 0),
		}
		for _, f := range fields {
			source, alias := parseFieldAlias(f)
			if v, ok := item[source]; ok {
				om.keys = append(om.keys, alias)
				om.m[alias] = v
			}
		}
		return om
	}

	// We must preserve the Key/Value structure for API consistency.
	// However, we want to respect the order of fields requested within Key and Value.

	var newKey any = nil
	var newValue any = nil

	// Helper to check if a field is requested
	isRequested := func(target string) bool {
		for _, field := range fields {
			source, _ := parseFieldAlias(field)
			if source == target {
				return true
			}
		}
		return false
	}

	// 1. Handle Key
	originalKey := item["key"]
	if isRequested("key") || isRequested("Key") {
		newKey = originalKey
	} else {
		// Check if originalKey is a map/struct we can filter
		if keyMap, ok := originalKey.(map[string]any); ok {
			// Create OrderedMap for Key
			om := OrderedMap{
				m:    make(map[string]any),
				keys: make([]string, 0),
			}
			// Iterate requested fields to preserve order
			for _, f := range fields {
				source, alias := parseFieldAlias(f)
				if v, ok := keyMap[source]; ok {
					om.keys = append(om.keys, alias)
					om.m[alias] = v
				}
			}
			if len(om.keys) > 0 {
				newKey = om
			}
		} else if orderedKey, ok := originalKey.(OrderedKey); ok {
			keyMap := orderedKey.m
			// Create OrderedMap for Key
			om := OrderedMap{
				m:    make(map[string]any),
				keys: make([]string, 0),
			}
			for _, f := range fields {
				source, alias := parseFieldAlias(f)
				if v, ok := keyMap[source]; ok {
					om.keys = append(om.keys, alias)
					om.m[alias] = v
				}
			}
			if len(om.keys) > 0 {
				newKey = om
			}
		}
	}

	// 2. Handle Value
	originalValue := item["value"]
	if isRequested("value") || isRequested("Value") {
		newValue = originalValue
	} else {
		if valMap, ok := originalValue.(map[string]any); ok {
			// Create OrderedMap for Value
			om := OrderedMap{
				m:    make(map[string]any),
				keys: make([]string, 0),
			}
			for _, f := range fields {
				source, alias := parseFieldAlias(f)
				if v, ok := valMap[source]; ok {
					om.keys = append(om.keys, alias)
					om.m[alias] = v
				}
			}
			if len(om.keys) > 0 {
				newValue = om
			}
		}
	}

	return map[string]any{
		"key":   newKey,
		"value": newValue,
	}
}

func mergeMap(original, updates map[string]any) map[string]any {
	newMap := make(map[string]any)
	for k, v := range original {
		newMap[k] = v
	}
	for k, v := range updates {
		newMap[k] = v
	}
	return newMap
}

// alignType attempts to convert filterVal to match the type of targetVal.
// This is useful when comparing Int vs String keys.
func alignType(filterVal any, targetVal any) any {
	if _, ok := targetVal.(string); ok {
		return convertToString(filterVal)
	}
	if _, ok := targetVal.(float64); ok {
		return convertToFloat(filterVal)
	}
	if _, ok := targetVal.(int); ok {
		return convertToInt(filterVal)
	}
	return filterVal
}

func convertToFloat(v any) any {
	switch val := v.(type) {
	case int:
		return float64(val)
	case float64:
		return val
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return v
}

func convertToInt(v any) any {
	switch val := v.(type) {
	case int:
		return val
	case float64:
		return int(val)
	case string:
		if i, err := strconv.Atoi(val); err == nil {
			return i
		}
	}
	return v
}

func convertToString(v any) any {
	return fmt.Sprintf("%v", v)
}

// extractVal extracts a value from a key or value map/json.
func extractVal(key any, val any, field string) any {
	if field == "key" {
		return key
	}

	check := func(source any) any {
		if source == nil {
			return nil
		}
		// 1. Map
		if m, ok := source.(map[string]any); ok {
			if v, ok := m[field]; ok {
				return v
			}
			// Case-insensitive fallback
			for k, v := range m {
				if strings.EqualFold(k, field) {
					return v
				}
			}
		}
		// 2. JSON String
		if s, ok := source.(string); ok && strings.HasPrefix(strings.TrimSpace(s), "{") {
			var m map[string]any
			if err := json.Unmarshal([]byte(s), &m); err == nil {
				if v, ok := m[field]; ok {
					return v
				}
				// Case-insensitive fallback
				for k, v := range m {
					if strings.EqualFold(k, field) {
						return v
					}
				}
			}
		}
		return nil
	}

	if v := check(key); v != nil {
		return v
	}
	if v := check(val); v != nil {
		return v
	}
	return nil
}

// isKeyField checks if a field is part of the Key.
func isKeyField(key any, field string) (bool, string) {
	if field == "key" {
		return true, "key"
	}
	if key == nil {
		return false, ""
	}
	// 1. Map
	if km, ok := key.(map[string]any); ok {
		if _, ok := km[field]; ok {
			return true, field
		}
		for k := range km {
			if strings.EqualFold(k, field) {
				return true, k
			}
		}
	}
	// 2. JSON String
	if s, ok := key.(string); ok && strings.HasPrefix(strings.TrimSpace(s), "{") {
		var m map[string]any
		if err := json.Unmarshal([]byte(s), &m); err == nil {
			if _, ok := m[field]; ok {
				return true, field
			}
			for k := range m {
				if strings.EqualFold(k, field) {
					return true, k
				}
			}
		}
	}
	return false, ""
}

// coerce converts val to match the type of target.
func coerce(val any, target any) any {
	if val == nil || target == nil {
		return val
	}

	// If types match, return as is
	if fmt.Sprintf("%T", val) == fmt.Sprintf("%T", target) {
		return val
	}

	switch target.(type) {
	case int:
		switch v := val.(type) {
		case float64:
			return int(v)
		case float32:
			return int(v)
		case int:
			return v
		case int8:
			return int(v)
		case int16:
			return int(v)
		case int32:
			return int(v)
		case int64:
			return int(v)
		case uint:
			return int(v)
		case uint8:
			return int(v)
		case uint16:
			return int(v)
		case uint32:
			return int(v)
		case uint64:
			return int(v)
		case string:
			var i int
			if _, err := fmt.Sscanf(v, "%d", &i); err == nil {
				return i
			}
		}
	case int8:
		switch v := val.(type) {
		case float64:
			return int8(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 8); err == nil {
				return int8(i)
			}
		default:
			// Fallback to int conversion then cast
			if i, ok := val.(int); ok {
				return int8(i)
			}
		}
	case int16:
		switch v := val.(type) {
		case float64:
			return int16(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 16); err == nil {
				return int16(i)
			}
		}
	case int32:
		switch v := val.(type) {
		case float64:
			return int32(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 32); err == nil {
				return int32(i)
			}
		}
	case int64:
		switch v := val.(type) {
		case float64:
			return int64(v)
		case int:
			return int64(v)
		case string:
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i
			}
		}
	case uint:
		switch v := val.(type) {
		case float64:
			return uint(v)
		case string:
			if i, err := strconv.ParseUint(v, 10, 64); err == nil {
				return uint(i)
			}
		}
	case uint8:
		switch v := val.(type) {
		case float64:
			return uint8(v)
		case string:
			if i, err := strconv.ParseUint(v, 10, 8); err == nil {
				return uint8(i)
			}
		}
	case uint16:
		switch v := val.(type) {
		case float64:
			return uint16(v)
		case string:
			if i, err := strconv.ParseUint(v, 10, 16); err == nil {
				return uint16(i)
			}
		}
	case uint32:
		switch v := val.(type) {
		case float64:
			return uint32(v)
		case string:
			if i, err := strconv.ParseUint(v, 10, 32); err == nil {
				return uint32(i)
			}
		}
	case uint64:
		switch v := val.(type) {
		case float64:
			return uint64(v)
		case string:
			if i, err := strconv.ParseUint(v, 10, 64); err == nil {
				return i
			}
		}
	case float32:
		switch v := val.(type) {
		case float64:
			return float32(v)
		case string:
			if f, err := strconv.ParseFloat(v, 32); err == nil {
				return float32(f)
			}
		}
	case float64:
		switch v := val.(type) {
		case int:
			return float64(v)
		case string:
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f
			}
		}
	case string:
		return fmt.Sprintf("%v", val)
	case bool:
		switch v := val.(type) {
		case bool:
			return v
		case string:
			return strings.ToLower(v) == "true"
		}
	case sop.UUID:
		if s, ok := val.(string); ok {
			if id, err := sop.ParseUUID(s); err == nil {
				return id
			}
		}
	}
	return val
}

// valuesMatch checks if two values match (strict string comparison).
func valuesMatch(v1, v2 any) bool {
	return fmt.Sprintf("%v", v1) == fmt.Sprintf("%v", v2)
}

// generateJoinKey generates a cache key for Hash Join.
func generateJoinKey(key any, val any, fields []string) string {
	var parts []string
	for _, f := range fields {
		v := extractVal(key, val, f)
		parts = append(parts, fmt.Sprintf("%v", v))
	}
	return strings.Join(parts, "|")
}
