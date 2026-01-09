package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// ensurePlan selects the execution strategy using simple schema analysis.
// This is the "Planning Phase".
func (jc *JoinRightCursor) ensurePlan() error {
	if jc.planReady {
		return nil
	}
	jc.planReady = true
	jc.plan = JoinPlan{Strategy: StrategyInMemory}

	info := jc.right.GetStoreInfo()

	// 1. Primitive vs Composite Check
	isPrimitive := true
	if info.MapKeyIndexSpecification != "" {
		isPrimitive = false
		var spec struct {
			IndexFields []struct {
				FieldName          string `json:"field_name"`
				AscendingSortOrder *bool  `json:"ascending_sort_order"`
			} `json:"index_fields"`
		}
		if err := json.Unmarshal([]byte(info.MapKeyIndexSpecification), &spec); err == nil {
			for i, f := range spec.IndexFields {
				jc.plan.IndexFields = append(jc.plan.IndexFields, f.FieldName)
				if i == 0 {
					if f.AscendingSortOrder != nil {
						jc.plan.Ascending = *f.AscendingSortOrder
					} else {
						jc.plan.Ascending = true
					}
				}
			}
			jc.plan.IsComposite = true
		}
	}

	// 2. Strategy Selection
	if isPrimitive {
		for _, rFieldRaw := range jc.on {
			if fmt.Sprintf("%v", rFieldRaw) == "key" {
				jc.plan.Strategy = StrategyIndexSeek
				jc.plan.IsComposite = false
				return nil
			}
		}
	} else if len(jc.plan.IndexFields) > 0 {
		// Find Longest Common matching Prefix
		for _, idxField := range jc.plan.IndexFields {
			found := false
			for _, rFieldRaw := range jc.on {
				if fmt.Sprintf("%v", rFieldRaw) == idxField {
					found = true
					break
				}
			}
			if found {
				jc.plan.PrefixFields = append(jc.plan.PrefixFields, idxField)
			} else {
				break
			}
		}

		if len(jc.plan.PrefixFields) > 0 {
			jc.plan.Strategy = StrategyIndexSeek
			return nil
		}
	}

	return nil
}

// NextOptimized is the "Execution Phase".
func (jc *JoinRightCursor) NextOptimized(ctx context.Context) (any, bool, error) {
	if err := jc.ensurePlan(); err != nil {
		return nil, false, err
	}

	// STRATEGY: Fallback (Memory Hash Join)
	if jc.plan.Strategy == StrategyInMemory {
		if !jc.useFallback {
			jc.useFallback = true
			// Materialize
			// ERROR HANDLING: If First() or Next() fails, return error immediately.
			scanIter, err := jc.right.First(ctx)
			if err != nil {
				return nil, false, fmt.Errorf("join materialization error (First): %w", err)
			}

			// Hard Logic Limit to prevent OOM on massive unrelated joins
			// TODO: Make configurable
			limit := 100000
			count := 0

			for scanIter && count < limit {
				k, err := jc.right.GetCurrentKey()
				if err != nil {
					return nil, false, fmt.Errorf("join materialization error (Key): %w", err)
				}
				v, err := jc.right.GetCurrentValue(ctx)
				if err != nil {
					return nil, false, fmt.Errorf("join materialization error (Value): %w", err)
				}

				item := renderItem(k, v, nil)
				jc.fallbackList = append(jc.fallbackList, item)

				scanIter, err = jc.right.Next(ctx)
				if err != nil {
					return nil, false, fmt.Errorf("join materialization error (Next): %w", err)
				}
				count++
			}

			if count >= limit {
				// Determine if we stopped due to limit (and there is more data)
				// If scanIter is TRUE, it means there is more data.
				if scanIter {
					return nil, false, fmt.Errorf("join error: right store too large for in-memory fallback (> %d records). Create an index matching ON fields", limit)
				}
			}
		}

		for {
			if jc.currentL == nil {
				var ok bool
				var err error
				jc.currentL, ok, err = jc.left.Next(ctx)
				if err != nil {
					return nil, false, err
				}
				if !ok {
					return nil, false, nil
				}
				jc.fallbackIdx = 0
				jc.matched = false
			}

			for jc.fallbackIdx < len(jc.fallbackList) {
				rItem := jc.fallbackList[jc.fallbackIdx]
				jc.fallbackIdx++

				match := true
				for lField, rFieldRaw := range jc.on {
					rField := fmt.Sprintf("%v", rFieldRaw)
					lVal := getField(jc.currentL, lField)
					rVal := getField(rItem, rField)

					if fmt.Sprintf("%v", lVal) != fmt.Sprintf("%v", rVal) {
						match = false
						break
					}
				}

				if match {
					jc.matched = true
					merged := jc.mergeResult(jc.currentL, rItem, getField(rItem, "key"))
					return merged, true, nil
				}
			}

			if !jc.matched && jc.joinType == "left" {
				res := jc.currentL
				jc.currentL = nil
				return res, true, nil
			}
			jc.currentL = nil
		}
	}

	// STRATEGY: Index Seek
	for {
		if jc.currentL == nil {
			var ok bool
			var err error
			jc.currentL, ok, err = jc.left.Next(ctx)
			if err != nil {
				return nil, false, err
			}
			if !ok {
				return nil, false, nil
			}
			jc.matched = false

			// Seek
			var seekKey any
			if jc.plan.IsComposite {
				compKey := make(map[string]any)
				for _, pField := range jc.plan.PrefixFields {
					for lField, rFieldRaw := range jc.on {
						if fmt.Sprintf("%v", rFieldRaw) == pField {
							compKey[pField] = getField(jc.currentL, lField)
							break
						}
					}
				}
				seekKey = compKey
			} else {
				for lField, rFieldRaw := range jc.on {
					if fmt.Sprintf("%v", rFieldRaw) == "key" {
						seekKey = getField(jc.currentL, lField)
						break
					}
				}
			}

			var found bool
			found, err = jc.right.FindOne(ctx, seekKey, true)
			if err != nil {
				return nil, false, fmt.Errorf("join seek error: %w", err)
			}

			if !found && !jc.plan.IsComposite {
				// No match for this key
				if jc.joinType == "left" {
					// Emit (LHS, nil)
					res := jc.currentL
					jc.currentL = nil // Consumed
					return res, true, nil
				}

				jc.currentL = nil // Consumed (Inner Join: discard LHS)
				continue
			}
		}

		// Scan
		for {
			k, err := jc.right.GetCurrentKey()
			if err != nil {
				return nil, false, fmt.Errorf("join scan error (Key): %w", err)
			}
			if k == nil {
				break
			}
			v, err := jc.right.GetCurrentValue(ctx)
			if err != nil {
				return nil, false, fmt.Errorf("join scan error (Value): %w", err)
			}

			// Stop Check (B-Tree Order Awareness)
			stop := false
			if jc.plan.IsComposite {
				if kMap, ok := k.(map[string]any); ok {
					for _, pField := range jc.plan.PrefixFields {
						var targetVal any
						for lField, rFieldRaw := range jc.on {
							if fmt.Sprintf("%v", rFieldRaw) == pField {
								targetVal = getField(jc.currentL, lField)
								break
							}
						}
						// Use Compare helper if available, or string fallback logic
						// We use the same comparison logic as B-Tree for consistency
						sCurr := fmt.Sprintf("%v", kMap[pField])
						sTarget := fmt.Sprintf("%v", targetVal)

						if jc.plan.Ascending {
							if sCurr > sTarget {
								stop = true
								break
							} else if sCurr < sTarget {
								stop = false
							}
						} else {
							// Descending: Stop if Current < Target (Passed it)
							// e.g. Target=EU. Scan=US, EU, CN.
							// Start US. US > EU. Continue.
							// Next EU. Match.
							// Next CN. CN < EU. Stop.
							// Wait. "US" > "EU".
							// If order is US, EU, CN.
							// Scan US. "US" > "EU". Continue.
							// Scan EU. "EU" == "EU". Match.
							// Scan CN. "CN" < "EU". Stop.
							if sCurr < sTarget {
								stop = true
								break
							} else if sCurr > sTarget {
								stop = false
							}
						}
						// If equal, check next field
					}
				}
			} else {
				// Primitive Key assumes Ascending? Or check store info?
				// Primitive stores usually default to Ascending.
				var targetVal any
				for lField, rFieldRaw := range jc.on {
					if fmt.Sprintf("%v", rFieldRaw) == "key" {
						targetVal = getField(jc.currentL, lField)
						break
					}
				}
				if fmt.Sprintf("%v", k) > fmt.Sprintf("%v", targetVal) {
					stop = true
				}
			}

			if stop {
				break
			}

			// Match Check (Filter)
			match := true
			for lField, rFieldRaw := range jc.on {
				rField := fmt.Sprintf("%v", rFieldRaw)
				lVal := getField(jc.currentL, lField)
				var rVal any
				if rField == "key" {
					rVal = k
				} else {
					if vMap, ok := v.(map[string]any); ok {
						if val, found := vMap[rField]; found {
							rVal = val
						}
					}
					// If not found in Value, check the Key (Composite Key support)
					if rVal == nil {
						if kMap, ok := k.(map[string]any); ok {
							if val, found := kMap[rField]; found {
								rVal = val
							}
						}
					}
				}
				if fmt.Sprintf("%v", lVal) != fmt.Sprintf("%v", rVal) {
					match = false
					break
				}
			}

			if match {
				jc.matched = true
				merged := jc.mergeResult(jc.currentL, v, k)
				jc.right.Next(ctx)
				return merged, true, nil
			}

			ok, err := jc.right.Next(ctx)
			if err != nil {
				return nil, false, fmt.Errorf("join scan error (Next): %w", err)
			}
			if !ok {
				break
			}
		}

		if !jc.matched && jc.joinType == "left" {
			res := jc.currentL
			jc.currentL = nil
			return res, true, nil
		}
		jc.currentL = nil
	}
}

func (jc *JoinRightCursor) mergeResult(l any, rAny any, rKey any) any {
	// Determine keys for L
	var lKeys []string
	var lMap map[string]any

	if om, ok := l.(*OrderedMap); ok && om != nil {
		lKeys = om.keys
		lMap = om.m
	} else if om, ok := l.(OrderedMap); ok {
		lKeys = om.keys
		lMap = om.m
	} else if m, ok := l.(map[string]any); ok && m != nil {
		lMap = m
		for k := range m {
			lKeys = append(lKeys, k)
		}
		sort.Strings(lKeys)
	} else {
		lMap = make(map[string]any)
	}

	// Flatten Right Item
	rObj := renderItem(rKey, rAny, nil)

	var rMap map[string]any
	if m, ok := rObj.(map[string]any); ok {
		rMap = m
	} else if om, ok := rObj.(*OrderedMap); ok && om != nil {
		rMap = om.m
	} else if om, ok := rObj.(OrderedMap); ok {
		rMap = om.m
	}

	// Merge
	newKeys := make([]string, len(lKeys))
	copy(newKeys, lKeys)
	newMap := make(map[string]any)

	for k, v := range lMap {
		newMap[k] = v
	}

	if rMap != nil {
		var rKeys []string
		for k := range rMap {
			rKeys = append(rKeys, k)
		}
		sort.Strings(rKeys)

		for _, k := range rKeys {
			newMap[k] = rMap[k]
			found := false
			for _, existing := range newKeys {
				if existing == k {
					found = true
					break
				}
			}
			if !found {
				newKeys = append(newKeys, k)
			}
		}
	}

	return &OrderedMap{m: newMap, keys: newKeys}
}
