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

	// 0. Detect Flat Input Case (e.g. from Script variables or Join output that was already flattened)
	// If the item doesn't structurally look like a Key/Value store wrapper (which must have "key" and/or "value"),
	// and we are filtering it, we treat it as a flat map and return an OrderedMap.
	_, hasKey := item["key"]
	_, hasValue := item["value"]

	// Assuming standard wrapper always produces "key" and "value" fields, even if nil.
	// But sometimes they might be missing?
	// If strict Key/Value context, at least one should exist.
	if !hasKey && !hasValue {
		// Flat Mode: Return primitives directly in OrderedMap
		out := &OrderedMap{
			m:    make(map[string]any),
			keys: make([]string, 0, len(fields)),
		}

		for _, f := range fields {
			source, alias := parseFieldAlias(f)

			// Simple dotted path navigation could be supported here too?
			// For now, simple key lookup.
			val, ok := item[source]

			// If not found, check if source has dot?
			if !ok && strings.Contains(source, ".") {
				// Very basic nested lookup (one level)
				parts := strings.SplitN(source, ".", 2)
				if sub, subOk := item[parts[0]].(map[string]any); subOk {
					val, ok = sub[parts[1]]
				}
			}

			if ok {
				out.keys = append(out.keys, alias)
				out.m[alias] = val
			}
		}
		// log.Debug("payload contents:", "Function", "filterFields", "keys", out.keys, "fields", fields)
		return out
	}

	// 1. Prepare Key and Value Containers
	// We always respect the UI contract: Output must be {"key": ..., "value": ...}

	originalKey := item["key"]
	originalValue := item["value"]

	// These hold the projected sub-fields
	keyMap := &OrderedMap{m: make(map[string]any), keys: make([]string, 0)}
	valMap := &OrderedMap{m: make(map[string]any), keys: make([]string, 0)}

	// Flags to track if we should just return the whole original key/value (e.g. "select key")
	var finalKey any = nil
	var finalValue any = nil
	keySelected := false
	valueSelected := false

	// Helper to extract a field from a map with case-insensitivity
	getFromMap := func(source any, fieldName string) (any, bool) {
		if source == nil {
			return nil, false
		}

		// If source is OrderedKey, unwrap
		if ok, isOk := source.(OrderedKey); isOk {
			source = ok.m
		}

		if m, ok := source.(map[string]any); ok {
			if v, ok := m[fieldName]; ok {
				return v, true
			}
			// Case-insensitive fallback
			lowerField := strings.ToLower(fieldName)
			for k, v := range m {
				if strings.ToLower(k) == lowerField {
					return v, true
				}
			}
		}
		return nil, false
	}

	for _, f := range fields {
		source, alias := parseFieldAlias(f)

		// 1. Direct Selection of "key" or "value"
		if strings.EqualFold(source, "key") {
			finalKey = originalKey
			keySelected = true
			continue
		}
		if strings.EqualFold(source, "value") {
			finalValue = originalValue
			valueSelected = true
			continue
		}

		// 2. Probing

		// Try Key first (strict precedence?)
		found := false
		if v, ok := getFromMap(originalKey, source); ok {
			keyMap.keys = append(keyMap.keys, alias)
			keyMap.m[alias] = v
			found = true
		}

		// Then Try Value
		if v, ok := getFromMap(originalValue, source); ok {
			// If alias collision with key, value takes precedence? Or duplicate?
			// SQL typically allows duplicates.
			// But here we are splitting into Key and Value structs.
			// If it's in both, we probably want it in both to represent the record accurately.
			// BUT, usually an ID is in Key and Name is in Value.

			// Note: if I "select id", and id is in Key. I don't want it in Value if it's NOT in Value.
			valMap.keys = append(valMap.keys, alias)
			valMap.m[alias] = v
			found = true
		}

		// Robustness: If not found in either map, maybe it's a "value.something" path?
		// or "key.something"?
		if !found {
			// Check prefixes
			lowerSource := strings.ToLower(source)
			if strings.HasPrefix(lowerSource, "key.") {
				fieldName := source[4:]
				if v, ok := getFromMap(originalKey, fieldName); ok {
					keyMap.keys = append(keyMap.keys, alias)
					keyMap.m[alias] = v
				}
			} else if strings.HasPrefix(lowerSource, "value.") {
				fieldName := source[6:]
				if v, ok := getFromMap(originalValue, fieldName); ok {
					valMap.keys = append(valMap.keys, alias)
					valMap.m[alias] = v
				}
			}
		}
	}

	// Construct Final Output

	// If "key" was not explicitly selected as a whole, use the projected map
	if !keySelected {
		// Always return the projected map (even if empty) to ensure consumers receive a map structure
		finalKey = keyMap
	}

	// If "value" was not explicitly selected as a whole, use the projected map
	if !valueSelected {
		// Always return the projected map (even if empty) to ensure consumers receive a map structure
		finalValue = valMap
	}

	return map[string]any{
		"key":   finalKey,
		"value": finalValue,
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
	result := make(map[string]any)

	// 1. Flatten Key
	if kMap, ok := key.(map[string]any); ok {
		for k, v := range kMap {
			result[k] = v
		}
	} else {
		// If primitive key, and not nil (though nil key is rare in B-Tree)
		if key != nil {
			result["key"] = key
		}
	}

	// 2. Flatten Value
	if vMap, ok := value.(map[string]any); ok {
		for k, v := range vMap {
			// Value fields overwrite key fields in case of name collision?
			// Usually Value fields are distinct or "more details"
			result[k] = v
		}
	} else {
		if value != nil {
			result["value"] = value
		}
	}

	return result
}

// renderItem creates a result map from the key and value, applying standard flattening or field selection.
// It is used by Scan, Select, and Join cursors to ensure consistent output format.
func renderItem(key any, val any, fields []string) any {
	// 1. Wildcard / Flatten Mode
	if len(fields) == 0 {
		return flattenItem(key, val)
	}

	// 2. Projection Mode
	resultMap := OrderedMap{m: make(map[string]any), keys: make([]string, 0)}

	for _, f := range fields {
		// Handle Alias: "field AS alias"
		srcField := f
		dstField := f

		// Case-insensitive " AS " check
		lowerF := strings.ToLower(f)
		if strings.Contains(lowerF, " as ") {
			parts := strings.Split(f, " AS ") // Try uppercase
			if len(parts) != 2 {
				parts = strings.Split(f, " as ") // Try lowercase
			}
			if len(parts) == 2 {
				srcField = strings.TrimSpace(parts[0])
				dstField = strings.TrimSpace(parts[1])
			}
		}

		// Extract Value
		v := extractVal(key, val, srcField)

		// Include even if nil?
		if v != nil {
			resultMap.m[dstField] = v
			resultMap.keys = append(resultMap.keys, dstField)
		}
	}
	return &resultMap
}
