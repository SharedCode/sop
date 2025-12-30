package agent

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/jsondb"
)

func (a *DataAdminAgent) toolJoin(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Left Database
	var leftDb *database.Database
	leftDbName, _ := args["database"].(string)
	if leftDbName == "" {
		leftDbName = p.CurrentDB
	}
	if leftDbName != "" {
		if leftDbName == "system" && a.systemDB != nil {
			leftDb = a.systemDB
		} else if opts, ok := a.databases[leftDbName]; ok {
			leftDb = database.NewDatabase(opts)
		}
	}
	if leftDb == nil {
		return "", fmt.Errorf("left database not found or not selected")
	}

	// Resolve Right Database
	var rightDb *database.Database
	rightDbName, _ := args["right_database"].(string)
	if rightDbName == "" {
		rightDbName = leftDbName
	}
	if rightDbName != "" {
		if rightDbName == "system" && a.systemDB != nil {
			rightDb = a.systemDB
		} else if opts, ok := a.databases[rightDbName]; ok {
			rightDb = database.NewDatabase(opts)
		}
	}
	if rightDb == nil {
		return "", fmt.Errorf("right database not found")
	}

	leftStoreName, _ := args["left_store"].(string)
	rightStoreName, _ := args["right_store"].(string)

	// Parse Join Fields (support both single string and array)
	var leftFields []string
	if lf, ok := args["left_join_fields"].([]any); ok {
		for _, v := range lf {
			if s, ok := v.(string); ok {
				leftFields = append(leftFields, s)
			}
		}
	} else if lf, ok := args["left_join_fields"].([]string); ok {
		leftFields = lf
	}

	var rightFields []string
	if rf, ok := args["right_join_fields"].([]any); ok {
		for _, v := range rf {
			if s, ok := v.(string); ok {
				rightFields = append(rightFields, s)
			}
		}
	} else if rf, ok := args["right_join_fields"].([]string); ok {
		rightFields = rf
	}

	// Parse Projection Fields
	var fields []string
	if f, ok := args["fields"]; ok {
		if fSlice, ok := f.([]any); ok {
			for _, v := range fSlice {
				if s, ok := v.(string); ok {
					fields = append(fields, s)
				}
			}
		} else if fSlice, ok := f.([]string); ok {
			fields = fSlice
		}
	}

	joinType, _ := args["join_type"].(string)
	if joinType == "" {
		joinType = "inner"
	}
	limit, _ := args["limit"].(float64)
	if limit <= 0 {
		limit = 10
	}

	action, _ := args["action"].(string)
	isDeleteLeft := action == "delete_left"
	isUpdateLeft := action == "update_left"
	updateValues, _ := args["update_values"].(map[string]any)

	if leftStoreName == "" || rightStoreName == "" {
		return "", fmt.Errorf("both left_store and right_store are required")
	}
	if len(leftFields) == 0 {
		return "", fmt.Errorf("left_join_fields (or left_join_field) is required")
	}
	if len(rightFields) == 0 {
		rightFields = []string{"key"}
	}
	if len(leftFields) != len(rightFields) {
		return "", fmt.Errorf("number of left join fields (%d) must match number of right join fields (%d)", len(leftFields), len(rightFields))
	}

	leftMode := sop.ForReading
	if isDeleteLeft || isUpdateLeft {
		leftMode = sop.ForWriting
	}

	leftTx, leftAutoCommit, err := a.resolveTransaction(ctx, leftDb, leftDbName, leftMode)
	if err != nil {
		return "", err
	}
	if leftAutoCommit {
		defer leftTx.Rollback(ctx)
	}

	var rightTx sop.Transaction
	var rightAutoCommit bool

	if rightDbName == leftDbName {
		rightTx = leftTx
	} else {
		rightTx, rightAutoCommit, err = a.resolveTransaction(ctx, rightDb, rightDbName, sop.ForReading)
		if err != nil {
			return "", err
		}
		if rightAutoCommit {
			defer rightTx.Rollback(ctx)
		}
	}

	// Open Stores
	leftStore, _, leftIndexSpec, err := a.openGenericStore(ctx, leftDb.Options(), leftStoreName, leftTx)
	if err != nil {
		return "", fmt.Errorf("failed to open left store: %w", err)
	}
	rightStore, rightComparer, _, err := a.openGenericStore(ctx, rightDb.Options(), rightStoreName, rightTx)
	if err != nil {
		return "", fmt.Errorf("failed to open right store: %w", err)
	}

	emitter := NewResultEmitter(ctx)

	jp := &JoinProcessor{
		ctx:            ctx,
		leftStore:      leftStore,
		rightStore:     rightStore,
		leftFields:     leftFields,
		rightFields:    rightFields,
		fields:         fields,
		joinType:       joinType,
		limit:          limit,
		isDeleteLeft:   isDeleteLeft,
		isUpdateLeft:   isUpdateLeft,
		updateValues:   updateValues,
		emitter:        emitter,
		rightComparer:  rightComparer,
		leftTx:         leftTx,
		leftAutoCommit: leftAutoCommit,
		leftIndexSpec:  leftIndexSpec,
		leftStoreName:  leftStoreName,
		rightStoreName: rightStoreName,
	}

	return jp.Execute()
}

// JoinProcessor handles the execution of a join operation between two stores.
type JoinProcessor struct {
	ctx            context.Context
	leftStore      btree.BtreeInterface[any, any]
	rightStore     btree.BtreeInterface[any, any]
	rightComparer  btree.ComparerFunc[any]
	leftFields     []string
	rightFields    []string
	fields         []string
	joinType       string
	limit          float64
	isDeleteLeft   bool
	isUpdateLeft   bool
	updateValues   map[string]any
	emitter        *ResultEmitter
	leftTx         sop.Transaction
	leftAutoCommit bool
	count          int
	displayKeys    []string
	leftIndexSpec  *jsondb.IndexSpecification
	leftStoreName  string
	rightStoreName string

	// Internal State
	rightKeyFields   []string
	rightValueFields []string
	rightKeyFieldMap map[string]string
	rightKeyIsJSON   bool
	rightSampleKey   any
	canUseLookup     bool
	rightCache       map[string][]cachedItem
	lastLookupKey    any
}

type cachedItem struct {
	Key   any
	Value any
}

func (jp *JoinProcessor) analyzeRightStore() error {
	rOk, _ := jp.rightStore.First(jp.ctx)
	jp.rightKeyFieldMap = make(map[string]string)
	rightKeySampleVals := make(map[string]any)

	if rOk {
		rKey := jp.rightStore.GetCurrentKey()
		jp.rightSampleKey = rKey.Key
		// Check if key is JSON string
		if s, ok := rKey.Key.(string); ok && strings.HasPrefix(strings.TrimSpace(s), "{") {
			jp.rightKeyIsJSON = true
		}

		for _, field := range jp.rightFields {
			isKey, actualName := isKeyField(rKey.Key, field)
			if isKey {
				jp.rightKeyFields = append(jp.rightKeyFields, field)
				jp.rightKeyFieldMap[field] = actualName
				rightKeySampleVals[field] = extractVal(rKey.Key, nil, actualName)
			} else {
				jp.rightValueFields = append(jp.rightValueFields, field)
			}
		}
	} else {
		// Empty right store, treat all as value fields (fallback to scan)
		jp.rightValueFields = jp.rightFields
	}

	// We can use Lookup if we have at least one Key field to search on
	// AND we are not trying to lookup sub-fields of a JSON string key (unless we have the full key string)
	// We use FindOne(..., false) to seek to the first item >= key, which supports partial keys (prefix search).
	jp.canUseLookup = len(jp.rightKeyFields) > 0
	if jp.rightKeyIsJSON && len(jp.rightKeyFields) > 0 {
		// If we are joining on "key" explicitly, we can lookup (assuming left side provides the full JSON string)
		// If we are joining on sub-fields (e.g. "region"), we cannot construct the JSON string key reliably -> Disable Lookup
		jp.canUseLookup = false
		for _, f := range jp.rightKeyFields {
			if f == "key" {
				jp.canUseLookup = true
				break
			}
		}
	}
	return nil
}

func (jp *JoinProcessor) buildHashCache() error {
	if !jp.canUseLookup {
		log.Info("Join: Building Hash Cache for Right Store (Hash Join Optimization)...")
		jp.rightCache = make(map[string][]cachedItem)

		// Scan Right Store Once
		rFound, _ := jp.rightStore.First(jp.ctx)
		for rFound {
			rKey := jp.rightStore.GetCurrentKey()
			rVal, _ := jp.rightStore.GetCurrentValue(jp.ctx)

			cacheKey := generateJoinKey(rKey.Key, rVal, jp.rightFields)
			jp.rightCache[cacheKey] = append(jp.rightCache[cacheKey], cachedItem{Key: rKey.Key, Value: rVal})

			rFound, _ = jp.rightStore.Next(jp.ctx)
		}
		log.Info(fmt.Sprintf("Join: Hash Cache built with %d unique keys.", len(jp.rightCache)))
	}
	return nil
}

func (jp *JoinProcessor) processLeftItem(k, v any) (bool, error) {
	// Extract Join Values from Left
	leftJoinVals := make(map[string]any)
	for i, field := range jp.leftFields {
		rightFieldName := jp.rightFields[i]
		leftJoinVals[rightFieldName] = extractVal(k, v, field)
	}

	if !jp.canUseLookup {
		// Hash Join Probe
		probeKey := generateJoinKey(k, v, jp.leftFields)
		matches, ok := jp.rightCache[probeKey]

		if ok {
			for _, matchItem := range matches {
				rKey := matchItem.Key
				rVal := matchItem.Value

				stop, err := jp.emitMatch(k, v, rKey, rVal)
				if err != nil {
					return false, err
				}
				if stop {
					return true, nil
				}
			}
		}
		return false, nil
	}

	var rightFound bool

	if jp.canUseLookup {
		var lookupKey any
		if len(jp.rightKeyFields) == 1 && jp.rightKeyFields[0] == "key" {
			lookupKey = leftJoinVals["key"]
			if jp.rightSampleKey != nil {
				lookupKey = coerce(lookupKey, jp.rightSampleKey)
			}
		} else {
			lkMap := make(map[string]any)
			for _, field := range jp.rightKeyFields {
				actualName := jp.rightKeyFieldMap[field]
				v := leftJoinVals[field]
				lkMap[actualName] = v
			}
			lookupKey = lkMap
		}

		if lookupKey != nil {
			var err error
			needFind := true

			// Check if input is sorted (optimization safety)
			isSortedInput := true
			if jp.lastLookupKey != nil {
				if jp.rightComparer(lookupKey, jp.lastLookupKey) < 0 {
					isSortedInput = false
				}
			}
			jp.lastLookupKey = lookupKey

			// Optimization: Check current position
			currKey := jp.rightStore.GetCurrentKey()
			if currKey.Key != nil {
				cmp := jp.rightComparer(currKey.Key, lookupKey)
				if cmp == 0 {
					needFind = false
					rightFound = true
				} else if cmp < 0 {
					if ok, _ := jp.rightStore.Next(jp.ctx); ok {
						nextKey := jp.rightStore.GetCurrentKey()
						cmpNext := jp.rightComparer(nextKey.Key, lookupKey)
						if cmpNext == 0 {
							needFind = false
							rightFound = true
						} else if cmpNext > 0 {
							needFind = false
							rightFound = true
						}
					} else {
						needFind = false
						rightFound = false
					}
				} else {
					if isSortedInput {
						needFind = false
						rightFound = true
					} else {
						needFind = true
					}
				}
			}

			if needFind {
				_, err = jp.rightStore.Find(jp.ctx, lookupKey, false)
				if err != nil {
					rightFound = false
				} else {
					rightFound = (jp.rightStore.GetCurrentKey().Key != nil)
				}
			}
		}
	} else {
		rightFound, _ = jp.rightStore.First(jp.ctx)
	}

	for rightFound {
		rKey := jp.rightStore.GetCurrentKey()
		rVal, _ := jp.rightStore.GetCurrentValue(jp.ctx)

		match := true
		for _, field := range jp.rightKeyFields {
			rFieldVal := extractVal(rKey.Key, rVal, field)
			lVal := leftJoinVals[field]
			if !valuesMatch(lVal, rFieldVal) {
				match = false
				break
			}
		}

		if !match && jp.canUseLookup {
			break
		}

		if match {
			for _, field := range jp.rightValueFields {
				rFieldVal := extractVal(rKey.Key, rVal, field)
				lVal := leftJoinVals[field]
				if !valuesMatch(lVal, rFieldVal) {
					match = false
					break
				}
			}
		}

		if match {
			stop, err := jp.emitMatch(k, v, rKey.Key, rVal)
			if err != nil {
				return false, err
			}
			if stop {
				return true, nil
			}
		}

		rightFound, _ = jp.rightStore.Next(jp.ctx)
	}
	return false, nil
}

func (jp *JoinProcessor) computeDisplayKeys() {
	jp.displayKeys = make([]string, len(jp.fields))
	candidates := make([]string, len(jp.fields))
	counts := make(map[string]int)

	// Regex for " AS " (case insensitive, handles multiple spaces/tabs)
	aliasRe := regexp.MustCompile(`(?i)\s+as\s+`)

	for i, f := range jp.fields {
		// Handle "AS" alias
		if loc := aliasRe.FindStringIndex(f); loc != nil {
			// loc[0] is start of match, loc[1] is end of match
			alias := strings.TrimSpace(f[loc[1]:])
			jp.fields[i] = strings.TrimSpace(f[:loc[0]]) // Update field to source name
			candidates[i] = alias
			counts[alias]++
			continue
		}

		clean := f
		lowerF := strings.ToLower(f)
		// Strip prefixes
		prefixes := []string{"left.", "right.", "a.", "b.", "left_", "right_"}
		if jp.leftStoreName != "" {
			prefixes = append(prefixes, strings.ToLower(jp.leftStoreName)+".")
		}
		if jp.rightStoreName != "" {
			prefixes = append(prefixes, strings.ToLower(jp.rightStoreName)+".")
		}

		for _, prefix := range prefixes {
			if strings.HasPrefix(lowerF, prefix) {
				clean = f[len(prefix):]
				break
			}
		}
		// Strip numeric suffix if it looks like _1
		lastUnderscore := strings.LastIndex(clean, "_")
		if lastUnderscore > 0 && lastUnderscore < len(clean)-1 {
			suffix := clean[lastUnderscore+1:]
			if _, err := strconv.Atoi(suffix); err == nil {
				clean = clean[:lastUnderscore]
			}
		}

		// Title Case (Simple)
		if len(clean) > 0 {
			clean = strings.ToUpper(clean[:1]) + clean[1:]
		}
		candidates[i] = clean
		counts[clean]++
	}

	for i := range jp.fields {
		clean := candidates[i]
		if counts[clean] > 1 {
			// Collision: Fallback to formatted original
			// But if it was an explicit alias, we should probably respect it?
			// If user aliased two fields to the same name, that's their problem/intent.
			// But let's keep collision handling for auto-generated names.

			// Check if this candidate came from an explicit alias
			// We can check if the original field string had " as ".
			// But we lost the original string in the first loop if we overwrote jp.fields[i].
			// Let's assume explicit aliases are intentional and don't dedupe them aggressively,
			// or we just let them collide.

			jp.displayKeys[i] = clean
		} else {
			jp.displayKeys[i] = clean
		}
	}
}

func (jp *JoinProcessor) emitMatch(k, v, rKey, rVal any) (bool, error) {
	include := false
	if jp.joinType == "inner" || jp.joinType == "left" {
		include = true
	}

	if include {
		if jp.isDeleteLeft {
			jp.leftStore.Remove(jp.ctx, k)
			jp.count++
			return false, nil
		}

		if jp.isUpdateLeft {
			var newVal any
			if vMap, ok := v.(map[string]any); ok {
				newVal = mergeMap(vMap, jp.updateValues)
			} else {
				newVal = jp.updateValues
			}
			if ok, err := jp.leftStore.Update(jp.ctx, k, newVal); err != nil || !ok {
				return false, fmt.Errorf("failed to update left item: %v", err)
			}
			jp.count++
			return false, nil
		} else {
			if len(jp.fields) > 0 {
				keyMap := OrderedMap{m: make(map[string]any), keys: make([]string, 0)}
				valMap := OrderedMap{m: make(map[string]any), keys: make([]string, 0)}

				for i, f := range jp.fields {
					var val any
					var isKey bool

					// Note: f has been stripped of " AS alias" in computeDisplayKeys
					// But it might still have prefixes like "a.", "b." etc.

					lowerF := strings.ToLower(f)

					if strings.HasPrefix(lowerF, "left.") {
						fieldName := f[5:]
						val = extractVal(k, v, fieldName)
						isKey, _ = isKeyField(k, fieldName)
					} else if strings.HasPrefix(lowerF, "right.") {
						fieldName := f[6:]
						val = extractVal(rKey, rVal, fieldName)
						isKey, _ = isKeyField(rKey, fieldName)
					} else {
						val = extractVal(k, v, f)
						if val != nil {
							isKey, _ = isKeyField(k, f)
						} else {
							val = extractVal(rKey, rVal, f)
							if val != nil {
								isKey, _ = isKeyField(rKey, f)
							}
						}

						// Heuristic for aliases (a., b., left_, right_)
						if val == nil {
							var fieldName string
							var tryLeft, tryRight bool

							if strings.HasPrefix(lowerF, "a.") {
								fieldName = f[2:]
								tryLeft = true
							} else if strings.HasPrefix(lowerF, "b.") {
								fieldName = f[2:]
								tryRight = true
							} else if strings.HasPrefix(lowerF, "left_") {
								fieldName = f[5:]
								tryLeft = true
							} else if strings.HasPrefix(lowerF, "right_") {
								fieldName = f[6:]
								tryRight = true
							} else if jp.leftStoreName != "" && strings.HasPrefix(lowerF, strings.ToLower(jp.leftStoreName)+".") {
								fieldName = f[len(jp.leftStoreName)+1:]
								tryLeft = true
							} else if jp.rightStoreName != "" && strings.HasPrefix(lowerF, strings.ToLower(jp.rightStoreName)+".") {
								fieldName = f[len(jp.rightStoreName)+1:]
								tryRight = true
							}

							if tryLeft {
								val = extractVal(k, v, fieldName)
								isKey, _ = isKeyField(k, fieldName)
							} else if tryRight {
								val = extractVal(rKey, rVal, fieldName)
								isKey, _ = isKeyField(rKey, fieldName)
							}
						}

						// Heuristic for numeric suffixes (e.g. department_1 -> department)
						if val == nil {
							lastUnderscore := strings.LastIndex(f, "_")
							if lastUnderscore > 0 && lastUnderscore < len(f)-1 {
								suffix := f[lastUnderscore+1:]
								if _, err := strconv.Atoi(suffix); err == nil {
									baseName := f[:lastUnderscore]
									val = extractVal(k, v, baseName)
									if val != nil {
										isKey, _ = isKeyField(k, baseName)
									} else {
										val = extractVal(rKey, rVal, baseName)
										if val != nil {
											isKey, _ = isKeyField(rKey, baseName)
										}
									}
								}
							}
						}
					}

					// Normalize key for display (replace . with _)
					displayKey := jp.displayKeys[i]

					if isKey {
						keyMap.m[displayKey] = val
						keyMap.keys = append(keyMap.keys, displayKey)
					} else {
						valMap.m[displayKey] = val
						valMap.keys = append(valMap.keys, displayKey)
					}
				}
				jp.emitter.Emit(map[string]any{
					"key":   keyMap,
					"value": valMap,
				})
			} else {
				// Merge left and right values
				merged := make(map[string]any)
				if vm, ok := v.(map[string]any); ok {
					for k, val := range vm {
						merged[k] = val
					}
				} else {
					merged["left"] = v
				}

				if rm, ok := rVal.(map[string]any); ok {
					for k, val := range rm {
						merged[k] = val
					}
				} else {
					merged["right"] = rVal
				}

				var keyFormatted any = k
				if jp.leftIndexSpec != nil {
					if m, ok := k.(map[string]any); ok {
						keyFormatted = OrderedKey{m: m, spec: jp.leftIndexSpec}
					}
				}

				jp.emitter.Emit(map[string]any{
					"key":   keyFormatted,
					"value": merged,
				})
			}
			jp.count++
			if jp.count >= int(jp.limit) {
				return true, nil
			}
		}
	}
	return false, nil
}

func (jp *JoinProcessor) Execute() (string, error) {
	jp.computeDisplayKeys()

	lOk, _ := jp.leftStore.First(jp.ctx)
	if !lOk {
		return jp.emitter.Finalize(), nil
	}

	if err := jp.analyzeRightStore(); err != nil {
		return "", err
	}
	if err := jp.buildHashCache(); err != nil {
		return "", err
	}

	type leftItem struct {
		k any
		v any
	}
	batchSize := 100
	for lOk {
		if jp.count >= int(jp.limit) {
			break
		}

		var batch []leftItem
		for i := 0; i < batchSize && lOk && i+jp.count < int(jp.limit); i++ {
			k := jp.leftStore.GetCurrentKey()
			v, _ := jp.leftStore.GetCurrentValue(jp.ctx)
			batch = append(batch, leftItem{k.Key, v})
			lOk, _ = jp.leftStore.Next(jp.ctx)
		}

		for _, lItem := range batch {
			if jp.count >= int(jp.limit) {
				break
			}
			// Debug Log
			// fmt.Printf("Processing Left Item: Key=%v, Val=%v\n", lItem.k, lItem.v)
			stop, err := jp.processLeftItem(lItem.k, lItem.v)
			if err != nil {
				return "", err
			}
			if stop {
				break
			}
		}
	}

	if jp.isDeleteLeft {
		jp.emitter.Finalize()
		if jp.leftAutoCommit {
			if err := jp.leftTx.Commit(jp.ctx); err != nil {
				return "", fmt.Errorf("failed to commit delete transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"deleted_count": %d}`, jp.count), nil
	}

	if jp.isUpdateLeft {
		jp.emitter.Finalize()
		if jp.leftAutoCommit {
			if err := jp.leftTx.Commit(jp.ctx); err != nil {
				return "", fmt.Errorf("failed to commit update transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"updated_count": %d}`, jp.count), nil
	}

	return jp.emitter.Finalize(), nil
}
