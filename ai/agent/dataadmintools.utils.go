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
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/jsondb"
)

// mapToScriptSteps converts a generic slice of maps into []ai.ScriptStep.
func mapToScriptSteps(list []any) ([]ai.ScriptStep, error) {
	var steps []ai.ScriptStep
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			// Round trip via JSON to robustly handle type conversion
			b, err := json.Marshal(m)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal step: %v", err)
			}
			var step ai.ScriptStep
			if err := json.Unmarshal(b, &step); err != nil {
				return nil, fmt.Errorf("failed to unmarshal step: %v", err)
			}

			// Validation: Ensure valid type and command
			if step.Type == "" {
				step.Type = "command" // Default to command if unspecified
			}
			if step.Type == "command" && step.Command == "" {
				return nil, fmt.Errorf("invalid step: 'command' is required for steps of type 'command'")
			}

			steps = append(steps, step)
		} else {
			return nil, fmt.Errorf("invalid step format (expected object)")
		}
	}
	return steps, nil
}

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

				// Fallback: Case-insensitive key lookup (AI UX improvement)
				// If the exact key isn't found, try finding a key differing only in case.
				if !exists {
					for mk, mv := range mItem {
						if strings.EqualFold(mk, k) {
							itemVal = mv
							exists = true
							break
						}
					}
				}

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
				alignedV := alignType(v, itemVal)
				if !exists || btree.Compare(itemVal, alignedV) != 0 {
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
	// Debug logging
	// fmt.Printf("DEBUG: OrderedMap.MarshalJSON Keys: %v\n", o.keys)

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

// reorderItem ensures the output map follows the IndexSpecification order if available,
// or the requested fields order.
func reorderItem(item any, fields []string, indexSpec *jsondb.IndexSpecification) any {
	mItem, ok := item.(map[string]any)
	if !ok {
		return item
	}

	// 1. If explicit fields are requested, filterFields handles ordering.
	if len(fields) > 0 {
		return filterFields(mItem, fields)
	}

	// 2. Use IndexSpecification for default ordering (Key vs Value vs Index Fields)
	if indexSpec != nil {
		// Case A: Item has "key" field which is a Map -> Apply OrderedKey to it
		if kVal, hasKey := mItem["key"]; hasKey {
			if kMap, kOk := kVal.(map[string]any); kOk {
				// Replace "key" with OrderedKey wrapper
				mItem["key"] = OrderedKey{m: kMap, spec: indexSpec}
			}
			// Return OrderedMap ensuring "key" comes before "value"
			// Check if "value" exists
			keys := []string{"key"}
			if _, hasVal := mItem["value"]; hasVal {
				keys = append(keys, "value")
			}
			// Append any other fields (e.g. valid, deleted, etc)
			for k := range mItem {
				if k != "key" && k != "value" {
					keys = append(keys, k)
				}
			}
			return &OrderedMap{m: mItem, keys: keys}
		}

		// Case B: Flat Map - Order by Index Fields
		orderedKeys := make([]string, 0, len(mItem))
		used := make(map[string]bool)

		// Add index fields first
		for _, f := range indexSpec.IndexFields {
			if _, ok := mItem[f.FieldName]; ok {
				orderedKeys = append(orderedKeys, f.FieldName)
				used[f.FieldName] = true
			}
		}

		// Add remaining fields (sorted alphabetically)
		var remaining []string
		for k := range mItem {
			if !used[k] {
				remaining = append(remaining, k)
			}
		}
		sort.Strings(remaining)
		orderedKeys = append(orderedKeys, remaining...)

		return &OrderedMap{
			m:    mItem,
			keys: orderedKeys,
		}
	}

	return item
}

func filterFields(item map[string]any, fields []string) any {
	// log.Debug("filterFields called", "fields", fields)
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

	// 0. Always execute Flat Mode (Tabular Output)
	// We no longer enforce Key/Value structure for UI compatibility.
	// The result is a flat record based on the projection fields.

	// Flat Mode: Return primitives directly in OrderedMap
	out := &OrderedMap{
		m:    make(map[string]any),
		keys: make([]string, 0, len(fields)),
	}

	for _, f := range fields {
		source, alias := parseFieldAlias(f)

		// 1. Try direct lookup first (most common for flat join results)
		val, ok := item[source]

		// 2. If not found, check if it's a "key" or "value" access on a Store Item wrapper
		if !ok {
			if source == "key" {
				val, ok = item["key"]
			} else if source == "value" {
				val, ok = item["value"]
			}
		}

		// 3. If still not found, try to dig into Key/Value wrappers if they exist
		// This supports "name" mapping to "value.name" implicitly
		if !ok {
			// Check Value map
			if vMap, isMap := item["value"].(map[string]any); isMap {
				// Try exact match
				if v, found := vMap[source]; found {
					val = v
					ok = true
				} else {
					// Try case-insensitive? (Maybe later)
				}
			}
			// Check Key map (Composite Key)
			if !ok {
				if kMap, isMap := item["key"].(map[string]any); isMap {
					if v, found := kMap[source]; found {
						val = v
						ok = true
					}
				}
			}
		}

		// 4. Nested Dot Notation (e.g. "address.city", "left.name")
		if !ok && strings.Contains(source, ".") {
			parts := strings.SplitN(source, ".", 2)
			root := parts[0]
			path := parts[1]

			// Resolve root
			var rootObj any
			if rVal, rOk := item[root]; rOk {
				rootObj = rVal
			} else if root == "key" {
				rootObj, _ = item["key"]
			} else if root == "value" {
				rootObj, _ = item["value"]
			}

			// Dig
			if rootObj != nil {
				if rootMap, isMap := rootObj.(map[string]any); isMap {
					val, ok = rootMap[path]
				} else if rootOm, isOm := rootObj.(*OrderedMap); isOm {
					val, ok = rootOm.m[path]
				}
			}
		}

		if ok {
			out.keys = append(out.keys, alias)
			out.m[alias] = val
		}
	}
	return out
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

func coerceToFloat(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case float32:
		return float64(val)
	case string:
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return f
		}
	}
	return 0
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
		// Handle OrderedMap
		if om, ok := source.(*OrderedMap); ok && om != nil {
			source = om.m
		} else if om, ok := source.(OrderedMap); ok {
			source = om.m
		}
		// 1. Map
		if m, ok := source.(map[string]any); ok {
			if field == "*" {
				return m
			}
			if v, ok := m[field]; ok {
				return v
			}
			// Case-insensitive fallback
			for k, v := range m {
				if strings.EqualFold(k, field) {
					return v
				}
			}

			// Dot-Notation Fallback (e.g. "a.name" -> "name" if "a.name" is missing)
			// This allows referring to "a.name" even if the map only has "name" (unaliased Left side)
			if idx := strings.Index(field, "."); idx > 0 {
				stripped := field[idx+1:]
				if v, ok := m[stripped]; ok {
					return v
				}
				// Case-insensitive fallback for stripped
				for k, v := range m {
					if strings.EqualFold(k, stripped) {
						return v
					}
				}
			}

			// Reverse Fallback / Suffix Match
			// If we asked for "name" but map only has "b.name" (aliased), and "name" is unique (suffix match).
			// This enables selecting un-aliased names when storage is strictly aliased.
			dotSuffix := "." + field
			for k, v := range m {
				if strings.HasSuffix(k, dotSuffix) {
					// We found a candidate e.g. "b.name" for "name"
					// TODO: Should we check for ambiguity? (e.g. "a.name" and "b.name" both exist)
					// SQL standard says "Ambiguous". For now, we pick the first one we find to allow progress.
					// Or we could check if there are multiple.
					return v
				}
			}
		}
		// 2. JSON String
		if s, ok := source.(string); ok && strings.HasPrefix(strings.TrimSpace(s), "{") {
			var m map[string]any
			if err := json.Unmarshal([]byte(s), &m); err == nil {
				if field == "*" {
					return m
				}
				if v, ok := m[field]; ok {
					return v
				}
				// Case-insensitive fallback
				for k, v := range m {
					if strings.EqualFold(k, field) {
						return v
					}
				}

				// Dot-Notation Fallback
				if idx := strings.Index(field, "."); idx > 0 {
					stripped := field[idx+1:]
					if v, ok := m[stripped]; ok {
						return v
					}
					// Case-insensitive fallback for stripped
					for k, v := range m {
						if strings.EqualFold(k, stripped) {
							return v
						}
					}
				}

				// Reverse Fallback / Suffix Match for JSON too
				dotSuffix := "." + field
				for k, v := range m {
					if strings.HasSuffix(k, dotSuffix) {
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

// flattenItem merges key and value into a single map.
// If key/value are maps, their fields are merged at the top level.
// If they are primitives, they are stored as "key" and "value" (or specific field names if needed).
func flattenItem(key any, value any) map[string]any {
	// fmt.Printf("flattenItem Input: keyType=%T valType=%T val=%+v\n", key, value, value)
	result := make(map[string]any)

	// Helper to unwrap pointers
	var unwrap func(v any) any
	unwrap = func(v any) any {
		if v == nil {
			return nil
		}
		// 1. Check for *any (pointer to interface)
		if ptr, ok := v.(*any); ok {
			if ptr == nil {
				return nil
			}
			return unwrap(*ptr) // Recursively unwrap
		}
		return v
	}

	key = unwrap(key)
	value = unwrap(value)

	// 1. Flatten Key
	if kMap, ok := key.(map[string]any); ok {
		for k, v := range kMap {
			result[k] = v
		}
	} else if kOm, ok := key.(OrderedMap); ok {
		for k, v := range kOm.m {
			result[k] = v
		}
	} else if kOm, ok := key.(*OrderedMap); ok && kOm != nil {
		for k, v := range kOm.m {
			result[k] = v
		}
	} else {
		if key != nil {
			result["key"] = key
		}
	}

	// 2. Flatten Value
	if vMap, ok := value.(map[string]any); ok {
		for k, v := range vMap {
			result[k] = v
		}
	} else if vOm, ok := value.(OrderedMap); ok {
		for k, v := range vOm.m {
			result[k] = v
		}
	} else if vOm, ok := value.(*OrderedMap); ok && vOm != nil {
		for k, v := range vOm.m {
			result[k] = v
		}
	} else {
		// Attempt to flatten structs via JSON to respect struct tags
		if value != nil {
			isStruct := false
			// Simple heuristics (or just try json)
			// We only want to do this for things that look like structs, not primitives
			switch value.(type) {
			case int, int64, float64, string, bool:
				// Primitives: Just set as "value"
			default:
				isStruct = true
			}

			if isStruct {
				b, err := json.Marshal(value)
				if err == nil {
					var m map[string]any
					if err := json.Unmarshal(b, &m); err == nil {
						for k, v := range m {
							result[k] = v
						}
						// If successfully flattened, we don't necessarily need "value" key,
						// but 'renderItem' logic might rely on 'value' field availability.
						// However, getField looks at root too.
						// Let's store strict value as well for reference?
						// "value" key collision risk if struct has "value" field.
						// The original code did result["value"] = value.
						// If we flattened, we did valid expansion.
						// We should NOT set result["value"] if we flattened, to avoid shadowing if possible,
						// OR we set it so tools inspecting the object as a whole can see strict types?
						// Let's err on side of just flattening.
						return result
					}
				}
			}
			result["value"] = value
		}
	}

	return result
}

type ProjectionField struct {
	Src string
	Dst string
}

func parseProjectionFields(input any) []ProjectionField {
	var result []ProjectionField
	aliasRe := regexp.MustCompile(`(?i)\s+as\s+`)

	cleanName := func(f string) string {
		prefixes := []string{"left.", "right.", "a.", "b.", "left_", "right_"}
		lowerF := strings.ToLower(f)
		for _, p := range prefixes {
			if strings.HasPrefix(lowerF, p) {
				// If it's a wildcard projection like "a.*" or "left.*", preserve the prefix!
				// We need the prefix in renderItem to know WHICH side to broaden.
				if len(f) > len(p) && f[len(p):] == "*" {
					return f
				}
				// Handle case where LLM sends "a." or "left." meaning "a.*"
				if len(f) == len(p) {
					return f + "*"
				}
				return f[len(p):]
			}
		}
		// Also stripping "Department." if table alias expansion happened?
		// User reported "Department.region".
		// Let's rely on standard SQL behavior: usually "TableName.Column"
		// If we encounter "TableName.Column", should we strip TableName?
		// Only if user didn't ask for it specifically?
		// If input is "a.region", loop below handles "a." stripping.
		// If input was "region" but system expanded it to "Department.region".
		// We should probably strip ANY prefix if it looks like Table.Col
		if idx := strings.Index(f, "."); idx > 0 {
			return f[idx+1:]
		}
		return f
	}

	processString := func(f string) {
		src := f
		dst := f
		if loc := aliasRe.FindStringIndex(f); loc != nil {
			src = strings.TrimSpace(f[:loc[0]])
			dst = strings.TrimSpace(f[loc[1]:])
		} else {
			dst = cleanName(f)
		}

		// Handle "a." -> "a.*" expansion manually, but DO NOT strip prefixes for fields
		// We want to pass "b.name" as "b.name" so extractVal can find the aliased key.
		prefixes := []string{"left.", "right.", "a.", "b.", "left_", "right_"}
		lowerSrc := strings.ToLower(src)
		for _, p := range prefixes {
			if strings.HasPrefix(lowerSrc, p) {
				if len(src) == len(p) {
					src = src + "*"
				}
				break
			}
		}

		result = append(result, ProjectionField{Src: src, Dst: dst})
	}

	// Definition of a Map Parsing Rule:
	// Tries to interpret a map object as a projection definition.
	// Returns true if handled, and appends fields to the result.
	type MapParsingRule func(map[string]any) ([]ProjectionField, bool)

	rules := []MapParsingRule{
		// Rule 1: Specific {"field": "src", "alias": "dst"} format
		// Used by some SQL Agents for explicit aliasing
		func(m map[string]any) ([]ProjectionField, bool) {
			// Check for "alias" or "as"
			var alias string
			if a, ok := m["alias"].(string); ok {
				alias = a
			} else if a, ok := m["as"].(string); ok {
				alias = a
			}

			if alias != "" {
				if field, okF := m["field"].(string); okF {
					return []ProjectionField{{Src: field, Dst: alias}}, true
				}
			}
			return nil, false
		},
		// Rule 2: Generic Mapping {"alias": "src"} (Target : Source)
		// Used by Agent for JSON construction: {"employee": "right.name"} -> employee = right.name
		func(m map[string]any) ([]ProjectionField, bool) {
			var res []ProjectionField
			// Sort keys to ensure deterministic output order (map iteration is random)
			keys := make([]string, 0, len(m))
			for k := range m {
				keys = append(keys, k)
			}
			sort.Strings(keys)

			for _, k := range keys {
				v := m[k]
				src := fmt.Sprintf("%v", v)

				// Apply cleanName logic to source to handle wildcards properly
				// e.g. "a." -> "a.*"
				// cleanSrc := cleanName(src) // Unused now, we do manual logic below

				prefixes := []string{"left.", "right.", "a.", "b.", "left_", "right_"}
				// isSpecial := false // Unused
				lowerSrc := strings.ToLower(src)

				// If source is just "left" or "right" (common mistake), treat as wildcard
				if lowerSrc == "left" || lowerSrc == "right" {
					src = src + ".*"
					// isSpecial = true
				} else {
					for _, p := range prefixes {
						if strings.HasPrefix(lowerSrc, p) {
							if len(src) == len(p) {
								src = src + "*" // "left." -> "left.*"
							}
							// isSpecial = true
							break
						}
					}
				}

				res = append(res, ProjectionField{Src: src, Dst: k})
			}
			// This rule is greedy and always matches if the map is not empty
			return res, len(res) > 0
		},
	}

	if list, ok := input.([]any); ok {
		for _, item := range list {
			if s, ok := item.(string); ok {
				processString(s)
			} else if m, ok := item.(map[string]any); ok {
				// Try apply rules in order
				for _, rule := range rules {
					if res, handled := rule(m); handled {
						result = append(result, res...)
						break
					}
				}
			}
		}
	} else if list, ok := input.([]string); ok {
		for _, s := range list {
			processString(s)
		}
	} else if list, ok := input.([]ProjectionField); ok {
		return list
	} else if m, ok := input.(map[string]any); ok {
		// Handle single map as projection definition (Target: Source)
		for _, rule := range rules {
			if res, handled := rule(m); handled {
				return res
			}
		}
	}

	return result
}

// renderItem creates a result map from the key and value, applying standard flattening or field selection.
// It is used by Scan, Select, and Join cursors to ensure consistent output format.
func renderItem(key any, val any, fields any) any {
	// 1. Wildcard / Flatten Mode
	shouldFlatten := false
	if fields == nil {
		shouldFlatten = true
	} else if l, ok := fields.([]string); ok {
		if len(l) == 0 || (len(l) == 1 && l[0] == "") {
			shouldFlatten = true
		}
	} else if l, ok := fields.([]any); ok {
		if len(l) == 0 {
			shouldFlatten = true
		} else if len(l) == 1 {
			if s, ok := l[0].(string); ok && s == "" {
				shouldFlatten = true
			}
		}
	}

	if shouldFlatten {
		return flattenItem(key, val)
	}

	// Parse fields using the common helper if not already parsed
	var pFields []ProjectionField
	if pf, ok := fields.([]ProjectionField); ok {
		pFields = pf
	} else {
		pFields = parseProjectionFields(fields)
	}

	if len(pFields) == 0 {
		return flattenItem(key, val)
	}

	// 2. Projection Mode
	resultMap := OrderedMap{m: make(map[string]any), keys: make([]string, 0)}

	for _, f := range pFields {
		// Handle Wildcard Projection (e.g. "a.*" or "*")
		if strings.HasSuffix(f.Src, "*") {
			// Determine filter scope
			isAny := f.Src == "*"
			isLeft := strings.HasPrefix(strings.ToLower(f.Src), "a.") || strings.HasPrefix(strings.ToLower(f.Src), "left.")
			isRight := strings.HasPrefix(strings.ToLower(f.Src), "b.") || strings.HasPrefix(strings.ToLower(f.Src), "right.")
			// Also support custom aliases ending in ".*"
			// If not matching specific keywords but has prefix
			customPrefix := ""
			if !isLeft && !isRight && !isAny {
				if idx := strings.Index(f.Src, ".*"); idx > 0 {
					customPrefix = f.Src[:idx+1] // e.g. "my."
				}
			}

			// isAny declaration moved up

			// Flatten the source object and merge all keys
			flat := flattenItem(key, val)

			// Collect keys in deterministic order
			var flatKeys []string
			for k := range flat {
				flatKeys = append(flatKeys, k)
			}
			sort.Strings(flatKeys)

			for _, k := range flatKeys {
				v := flat[k]

				// Apply Scope Filtering
				include := false
				if isAny {
					include = true
				} else if isLeft {
					// Left side "a.*" or "left.*"
					// Include keys relative to Left.
					// Strictly, this means keys starting with "a." or keys WITHOUT any dot (standard SQL legacy where Left is naked).
					// But we must EXCLUDE keys that definitely belong to another alias (e.g. "b.something", "right.something").

					hasDot := strings.Contains(k, ".")
					if !hasDot {
						include = true
					} else {
						// It has a dot. Only include if it matches "left." or "a." (our prefixes)
						// OR if it doesn't match known "alien" prefixes.
						// Since we don't know all aliases, we assume if it starts with "Right." => Alien.
						// If it starts with "b." => Alien (heuristic common pattern).
						// Ideally we should prefer including ONLY if it starts with "a." or "left."?
						// But what if Left data has "user.name"?
						// SQL behavior: a.* returns columns of table A.

						if strings.HasPrefix(strings.ToLower(k), "a.") || strings.HasPrefix(strings.ToLower(k), "left.") {
							include = true
						} else if !strings.HasPrefix(k, "Right.") && !strings.HasPrefix(strings.ToLower(k), "b.") {
							// HEURISTIC: Exclude "b." and "Right." to fix duplication issue in "select a.*, b.name".
							// This is valid because we now enforce prefixing for "b" in JoinRight.
							include = true
						}
					}
				} else if isRight {
					// Right side = include keys starting with "Right."
					if strings.HasPrefix(k, "Right.") {
						include = true
					}
					// Also include keys matching the specific requested prefix (e.g. "b." from "b.*")
					prefix := f.Src[:len(f.Src)-1]
					if len(prefix) > 0 && strings.HasPrefix(k, prefix) {
						include = true
					}
				} else if customPrefix != "" {
					if strings.HasPrefix(k, customPrefix) {
						include = true
						// UX Improvement: Strip the prefix from the output key
						// If we are projecting "users.*" (customPrefix="users."), and we find "users.age",
						// we want the output key to be "age".
						k = k[len(customPrefix):]
					}
				}

				if include {
					// Don't overwrite existing explicit explicit aliased fields (precedence)
					// Actually, in projection lists, order matters.
					resultMap.m[k] = v

					// Add to key list if not present
					found := false
					for _, existingKey := range resultMap.keys {
						if existingKey == k {
							found = true
							break
						}
					}
					if !found {
						resultMap.keys = append(resultMap.keys, k)
					}
				}
			}
			continue
		}

		// Extract Value
		v := extractVal(key, val, f.Src)

		// Include even if nil?
		if v != nil {
			resultMap.m[f.Dst] = v
			resultMap.keys = append(resultMap.keys, f.Dst)
		}
	}
	return &resultMap
}

// CleanArgs extracts non-reserved arguments from a map.
// It skips keys starting with "_" and any key in the reserved list.
func CleanArgs(args map[string]any, reserved ...string) map[string]any {
	out := make(map[string]any)
	reservedMap := make(map[string]struct{})
	for _, r := range reserved {
		reservedMap[r] = struct{}{}
	}

	for k, v := range args {
		if strings.HasPrefix(k, "_") {
			continue
		}
		if _, ok := reservedMap[k]; ok {
			continue
		}
		out[k] = v
	}
	return out
}
