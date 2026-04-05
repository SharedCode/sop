package agent

import (
	"fmt"
	"sort"
	"strings"
)

func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func findSimilarKey(target string, m map[string]any) string {

	targetLower := strings.ToLower(target)
	for k := range m {
		if strings.ToLower(k) == targetLower {
			return k
		}
		if strings.Contains(strings.ToLower(k), targetLower) || strings.Contains(targetLower, strings.ToLower(k)) {
			return k
		}
	}
	return ""
}

func matchesMap(item any, condition map[string]any) bool {
	for k, v := range condition {
		itemVal := getField(item, k)

		if ops, ok := v.(map[string]any); ok {
			isOperator := false
			for op := range ops {
				if strings.HasPrefix(op, "$") {
					isOperator = true
					break
				}
			}

			if isOperator {
				for op, opVal := range ops {
					switch op {
					case "$eq":
						if compare(itemVal, opVal) != 0 {
							return false
						}
					case "$gt":
						if compare(itemVal, opVal) <= 0 {
							return false
						}
					case "$gte":
						if compare(itemVal, opVal) < 0 {
							return false
						}
					case "$lt":
						if compare(itemVal, opVal) >= 0 {
							return false
						}
					case "$lte":
						if compare(itemVal, opVal) > 0 {
							return false
						}
					case "$ne":
						if compare(itemVal, opVal) == 0 {
							return false
						}
					case "$in":
						found := false
						if list, ok := opVal.([]any); ok {
							for _, val := range list {
								if compare(itemVal, val) == 0 {
									found = true
									break
								}
							}
						}
						if !found {
							return false
						}
					case "$nin":
						if list, ok := opVal.([]any); ok {
							for _, val := range list {
								if compare(itemVal, val) == 0 {
									return false
								}
							}
						}
					}
				}
				continue
			}
		}

		if compare(itemVal, v) != 0 {
			return false
		}
	}
	return true
}

func resolveKey(m map[string]any, target string) (string, bool) {

	if _, ok := m[target]; ok {
		return target, true
	}

	for k := range m {
		if strings.EqualFold(k, target) {
			return k, true
		}
	}

	{
		var candidate string
		foundCount := 0
		suffix := "." + strings.ToLower(target)
		for k := range m {
			kLower := strings.ToLower(k)
			if strings.HasSuffix(kLower, suffix) {
				candidate = k
				foundCount++
			}
		}
		if foundCount == 1 {
			return candidate, true
		}
	}

	if idx := strings.Index(target, "."); idx != -1 {
		suffix := target[idx+1:]
		if _, ok := m[suffix]; ok {
			return suffix, true
		}
	}

	return "", false
}

func getField(itemObj any, field string) any {
	var item map[string]any
	if m, ok := itemObj.(map[string]any); ok {
		item = m
	} else if om, ok := itemObj.(OrderedMap); ok {
		item = om.m
	} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
		item = om.m
	} else {
		return nil
	}

	if key, found := resolveKey(item, field); found {
		return item[key]
	}

	if field == "key" {
		if v, ok := item["key"]; ok {
			return v
		}
	}

	if keyMap, ok := item["key"].(map[string]any); ok {
		if v, ok := keyMap[field]; ok {
			return v
		}
	}

	if valMap, ok := item["value"].(map[string]any); ok {
		if v, ok := valMap[field]; ok {
			return v
		}
	}

	if keyMap, ok := item["key"].(map[string]any); ok {
		if v, ok := keyMap[field]; ok {
			return v
		}
	}

	reconstructed := make(map[string]any)
	prefix := field + "."
	hasReconstructed := false

	for k, v := range item {
		if strings.HasPrefix(k, prefix) {
			suffix := k[len(prefix):]
			if suffix != "" {
				reconstructed[suffix] = v
				hasReconstructed = true
			}
		}
	}
	if hasReconstructed {
		return reconstructed
	}

	return nil
}

// Helper: Compare two values
func compare(a, b any) int {

	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	sa, okA := a.(string)
	sb, okB := b.(string)
	if okA && okB {
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}

	fa, okA := toFloat(a)
	fb, okB := toFloat(b)
	if okA && okB {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	sa = fmt.Sprintf("%v", a)
	sb = fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch i := v.(type) {
	case int:
		return float64(i), true
	case int64:
		return float64(i), true
	case float64:
		return i, true
	case float32:
		return float64(i), true
	case int32:
		return float64(i), true
	case int16:
		return float64(i), true
	case int8:
		return float64(i), true
	case uint:
		return float64(i), true
	case uint64:
		return float64(i), true
	case uint32:
		return float64(i), true
	case uint16:
		return float64(i), true
	case uint8:
		return float64(i), true
	}
	return 0, false
}

// sanitizeScript performs a pass over the script instructions to clean up common LLM errors.
// This is the "Compiler/Linter" phase before execution.
func sanitizeScript(script []ScriptInstruction) []ScriptInstruction {

	varOrigins := make(map[string]string)

	for i := range script {
		instr := &script[i]

		if instr.Op == "scan" || instr.Op == "open_store" {
			if store, ok := instr.Args["store"].(string); ok {
				if rVar := instr.ResultVar; rVar != "" {
					varOrigins[rVar] = store
				}
			} else if store, ok := instr.Args["name"].(string); ok {

				if rVar := instr.ResultVar; rVar != "" {
					varOrigins[rVar] = store
				}
			}
		} else if instr.Op == "filter" || instr.Op == "project" || instr.Op == "limit" || instr.Op == "sort" {

			if rVar := instr.ResultVar; rVar != "" {
				if inputVar := instr.InputVar; inputVar != "" {
					if origin, ok := varOrigins[inputVar]; ok {
						varOrigins[rVar] = origin
					}
				}
			}
		}

		if instr.Op == "project" {
			if fieldsRaw, ok := instr.Args["fields"]; ok {

				parsed := parseProjectionFields(fieldsRaw)

				instr.Args["fields"] = parsed
			}
		}

		if instr.Op == "join" || instr.Op == "join_right" {

			isExplicitRight := instr.Op == "join_right"
			instr.Op = "join"

			if t, ok := instr.Args["type"].(string); ok {
				instr.Args["type"] = strings.ToLower(strings.TrimSpace(t))
			} else if isExplicitRight {
				instr.Args["type"] = "right_outer"
			}

			if _, ok := instr.Args["type"]; !ok {
				instr.Args["type"] = "inner"
			}

			if _, hasAlias := instr.Args["left_alias"]; !hasAlias {
				if origin, ok := varOrigins[instr.InputVar]; ok {
					instr.Args["left_alias"] = origin
				}
			}

			if _, hasAlias := instr.Args["alias"]; !hasAlias {
				if _, hasRightAlias := instr.Args["right_alias"]; !hasRightAlias {

					resultVar := instr.ResultVar
					if resultVar != "" {

						for j := i + 1; j < len(script); j++ {
							future := &script[j]
							if future.Op == "project" && future.InputVar == resultVar {

								var candidates []string
								if fRaw, ok := future.Args["fields"]; ok {
									p := parseProjectionFields(fRaw)
									for _, field := range p {
										candidates = append(candidates, field.Src)
									}
								}

								prefix := resultVar + "."
								found := false
								for _, c := range candidates {
									if strings.HasPrefix(c, prefix) {
										found = true
										break
									}
								}

								if found {

									instr.Args["alias"] = resultVar
									break
								}
							}
						}
					}
				}
			}

			if onMap, ok := instr.Args["on"].(map[string]any); ok {
				newOn := make(map[string]any)
				for k, v := range onMap {

					newK := k

					if strings.EqualFold(k, "Value") {
						newK = "value"
					} else if strings.EqualFold(k, "Key") {
						newK = "key"
					}

					newV := v
					if vStr, ok := v.(string); ok {
						if strings.EqualFold(vStr, "Value") {
							newV = "value"
						} else if strings.EqualFold(vStr, "Key") {
							newV = "key"
						}
					}
					newOn[newK] = newV
				}
				instr.Args["on"] = newOn
			}
		}

		if instr.Op == "commit_tx" {

			hasCursorProducer := false
			for j := 0; j < i; j++ {
				prev := script[j]
				if prev.Op == "scan" || prev.Op == "join" || prev.Op == "filter" {
					hasCursorProducer = true
					break
				}
			}

			if hasCursorProducer {

				cmdToDefer := make(map[string]any)
				if instr.Args != nil {
					for k, v := range instr.Args {
						cmdToDefer[k] = v
					}
				}
				cmdToDefer["op"] = "commit_tx"

				instr.Op = "defer"
				instr.Args = map[string]any{
					"command": cmdToDefer,
				}
			}
		}
	}
	return script
}
