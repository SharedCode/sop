package agent

import (
	"context"
	"fmt"
	log "log/slog"
	"strings"
	"time"

	"github.com/sharedcode/sop/jsondb"
)

func (e *ScriptEngine) Filter(ctx context.Context, input any, args map[string]any) (any, error) {
	conditionRaw := args["condition"]
	if conditionRaw == nil {
		return input, nil
	}

	if cursor, ok := input.(ScriptCursor); ok {
		// Early validation: Check if filter condition references valid fields
		log.Info("Filter: Validating filter condition", "condition", conditionRaw)
		if err := e.validateFilterCondition(ctx, cursor, conditionRaw); err != nil {
			log.Error("Filter: Validation failed", "error", err, "condition", conditionRaw)
			return nil, fmt.Errorf("filter validation failed: %w", err)
		}
		log.Info("Filter: Validation passed", "condition", conditionRaw)

		return &FilterCursor{
			source: cursor,
			filter: conditionRaw,
			engine: e,
		}, nil
	}

	var list []any
	if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}
	return e.stageFilter(list, args)
}

// validateFilterCondition checks if filter condition is valid before execution
func (e *ScriptEngine) validateFilterCondition(ctx context.Context, cursor ScriptCursor, condition any) error {
	// Check for empty string condition
	if strCond, ok := condition.(string); ok {
		if strings.TrimSpace(strCond) == "" {
			return fmt.Errorf("filter condition cannot be empty string")
		}
		// Valid CEL string expression
		return nil
	}

	// Check for map-based conditions
	condMap, ok := condition.(map[string]any)
	if !ok {
		return fmt.Errorf("filter condition must be a map or CEL string, got %T", condition)
	}

	// Check for empty map condition
	if len(condMap) == 0 {
		return fmt.Errorf("filter condition cannot be empty map")
	}

	// Check each filter field for common mistakes
	for field, value := range condMap {
		if strings.TrimSpace(field) == "" {
			return fmt.Errorf("filter field name cannot be empty")
		}

		// Check if value is a scalar (string, number, bool) instead of operator map
		// This catches mistakes like {"total_amount": "500"} instead of {"total_amount": {"$gt": 500}}
		switch v := value.(type) {
		case string:
			// Check if it's an operator that was accidentally used as a scalar
			if strings.HasPrefix(v, "$") {
				return fmt.Errorf("filter field '%s' has operator '%s' as scalar value. Use {\"field\": {\"%s\": value}} format", field, v, v)
			}
			// Check if value looks like a field name (likely a mistake)
			if isLikelyFieldName(v) {
				return fmt.Errorf("filter condition suspicious: '%s' = '%s'. Did you mean to use an operator like {\"$gt\": ...} or {\"$eq\": ...}?", field, v)
			}
			// Scalar string values are treated as equality checks, but warn if suspicious

		case float64, int, int64, bool:
			// Scalar values without operators - could be equality check or mistake
			if boolVal, isBool := value.(bool); isBool {
				// Boolean true/false as filter value is almost always a mistake
				// The LLM probably meant to check if field exists or has a specific value
				return fmt.Errorf("filter field '%s' has boolean value %t. Did you mean to use an operator like {'$gt': value}, {'$eq': value}, or {'$exists': true}?", field, boolVal)
			}
			// Numeric scalar values could be intentional equality checks, allow them

		case map[string]any:
			// Good: operator map like {"$gt": 500}
			// Validate it has at least one operator
			hasOperator := false
			validOperators := map[string]bool{
				"$eq": true, "$ne": true, "$gt": true, "$gte": true,
				"$lt": true, "$lte": true, "$in": true, "$nin": true,
				"$exists": true, "$regex": true, "$contains": true,
			}
			for k := range v {
				if validOperators[k] || strings.HasPrefix(k, "$") {
					hasOperator = true
					break
				}
			}
			if !hasOperator {
				mapKeys := make([]string, 0, len(v))
				for k := range v {
					mapKeys = append(mapKeys, k)
				}
				// Special case: detect nested field references like {"orders": {"total_amount": true}}
				// This is a common LLM mistake - trying to reference nested fields
				if len(v) == 1 {
					for k, nestedVal := range v {
						if _, isBool := nestedVal.(bool); isBool {
							return fmt.Errorf("filter field '%s' has nested structure {'%s': %v} which looks like incorrect syntax. Use dot notation: '%s.%s' or use proper operator: {'%s': {'$gt': value}}", field, k, nestedVal, field, k, k)
						}
					}
				}
				return fmt.Errorf("filter field '%s' has map value but no operators. Got keys: %v. Expected operators like $gt, $lt, $eq, etc.", field, mapKeys)
			}

			// Validate operator values - boolean true/false as comparison values are almost always mistakes
			for opKey, opValue := range v {
				if boolVal, isBool := opValue.(bool); isBool {
					// Special exception: $exists operator can legitimately use boolean
					if opKey == "$exists" {
						continue
					}
					return fmt.Errorf("filter field '%s' has operator '%s' with boolean value %t. Boolean values in comparisons are usually mistakes. Did you mean to check a numeric/string field or use {'$exists': true/false}?", field, opKey, boolVal)
				}
			}

		case nil:
			return fmt.Errorf("filter field '%s' has nil value. Use {\"$eq\": null} or {\"$exists\": false} for null checks", field)

		default:
			// Arrays or other types might be valid for $in operations
			// Allow them through
		}
	}

	return nil
}

// validateScanFilterCondition validates filter conditions for scan/select operations
func (e *ScriptEngine) validateScanFilterCondition(condition any) error {
	// Check for empty string condition
	if strCond, ok := condition.(string); ok {
		if strings.TrimSpace(strCond) == "" {
			return fmt.Errorf("filter condition cannot be empty string")
		}
		// Valid CEL string expression
		return nil
	}

	// Check for map-based conditions
	condMap, ok := condition.(map[string]any)
	if !ok {
		return fmt.Errorf("filter condition must be a map or CEL string, got %T", condition)
	}

	// Check for empty map condition
	if len(condMap) == 0 {
		return fmt.Errorf("filter condition cannot be empty map")
	}

	// Check each filter field for common mistakes
	for field, value := range condMap {
		if strings.TrimSpace(field) == "" {
			return fmt.Errorf("filter field name cannot be empty")
		}

		// Check if value is a scalar (string, number, bool) instead of operator map
		switch v := value.(type) {
		case string:
			// Check if it's an operator that was accidentally used as a scalar
			if strings.HasPrefix(v, "$") {
				return fmt.Errorf("filter field '%s' has operator '%s' as scalar value. Use {\"field\": {\"%s\": value}} format", field, v, v)
			}
			// Check if value looks like a field name (likely a mistake)
			if isLikelyFieldName(v) {
				return fmt.Errorf("filter condition suspicious: '%s' = '%s'. Did you mean to use an operator like {\"$gt\": ...} or {\"$eq\": ...}?", field, v)
			}

		case float64, int, int64, bool:
			// Scalar values without operators - could be equality check or mistake
			if boolVal, isBool := value.(bool); isBool {
				// Boolean true/false as filter value is almost always a mistake
				return fmt.Errorf("filter field '%s' has boolean value %t. Did you mean to use an operator like {'$gt': value}, {'$eq': value}, or {'$exists': true}?", field, boolVal)
			}

		case map[string]any:
			// Operator map like {"$gt": 500}
			// Validate it has at least one operator
			hasOperator := false
			validOperators := map[string]bool{
				"$eq": true, "$ne": true, "$gt": true, "$gte": true,
				"$lt": true, "$lte": true, "$in": true, "$nin": true,
				"$exists": true, "$regex": true, "$contains": true,
			}
			for k := range v {
				if validOperators[k] || strings.HasPrefix(k, "$") {
					hasOperator = true
					break
				}
			}
			if !hasOperator {
				mapKeys := make([]string, 0, len(v))
				for k := range v {
					mapKeys = append(mapKeys, k)
				}
				// Special case: detect nested field references like {"orders": {"total_amount": true}}
				if len(v) == 1 {
					for k, nestedVal := range v {
						if _, isBool := nestedVal.(bool); isBool {
							return fmt.Errorf("filter field '%s' has nested structure {'%s': %v} which looks like incorrect syntax. Use dot notation: '%s.%s' or use proper operator: {'%s': {'$gt': value}}", field, k, nestedVal, field, k, k)
						}
					}
				}
				return fmt.Errorf("filter field '%s' has map value but no operators. Got keys: %v. Expected operators like $gt, $lt, $eq, etc.", field, mapKeys)
			}

			// Validate operator values - boolean true/false as comparison values are almost always mistakes
			for opKey, opValue := range v {
				if boolVal, isBool := opValue.(bool); isBool {
					// Special exception: $exists operator can legitimately use boolean
					if opKey == "$exists" {
						continue
					}
					return fmt.Errorf("filter field '%s' has operator '%s' with boolean value %t. Boolean values in comparisons are usually mistakes. Did you mean to check a numeric/string field or use {'$exists': true/false}?", field, opKey, boolVal)
				}
			}

		case nil:
			return fmt.Errorf("filter field '%s' has nil value. Use {\"$eq\": null} or {\"$exists\": false} for null checks", field)
		}
	}

	return nil
}

// isLikelyFieldName checks if a string looks like a field name rather than a value
func isLikelyFieldName(s string) bool {
	if len(s) == 0 {
		return false
	}
	// Field names typically: lowercase, underscores, no spaces, alphanumeric
	hasUnderscore := false
	for _, r := range s {
		if r == '_' {
			hasUnderscore = true
		} else if r == ' ' || r == '.' || r == ',' {
			return false // Spaces/punctuation suggest it's a value, not a field
		}
	}
	// If it has underscores and no spaces, likely a field name
	return hasUnderscore || (len(s) > 2 && s == strings.ToLower(s))
}

func (e *ScriptEngine) Sort(ctx context.Context, input any, args map[string]any) (any, error) {
	// Sort requires materialization
	var list []any

	if cursor, ok := input.(ScriptCursor); ok {

		for {
			item, ok, err := cursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			list = append(list, item)
		}
	} else if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list or cursor")
	}

	return e.stageSort(list, args)
}

func (e *ScriptEngine) Project(ctx context.Context, input any, args map[string]any) (any, error) {
	log.Debug("Project input", "type", fmt.Sprintf("%T", input))

	parsedFields := parseProjectionFields(args["fields"])

	if cursor, ok := input.(ScriptCursor); ok {
		log.Debug("Project returning ProjectCursor")
		return &ProjectCursor{
			source: cursor,
			fields: parsedFields,
		}, nil
	}

	log.Debug("Project falling back to stageProject (List)", "input_type", fmt.Sprintf("%T", input))

	var list []any
	if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}
	return e.stageProject(list, args)
}

func (e *ScriptEngine) Limit(ctx context.Context, input any, args map[string]any) (any, error) {
	log.Debug("Limit input", "type", fmt.Sprintf("%T", input))
	limitVal, _ := args["limit"].(float64)
	limit := int(limitVal)
	if limit <= 0 {
		limit = 1000
	}

	if cursor, ok := input.(ScriptCursor); ok {
		log.Debug("Limit returning LimitCursor")
		return &LimitCursor{
			source: cursor,
			limit:  limit,
		}, nil
	}

	log.Debug("Limit falling back to stageLimit (List)", "input_type", fmt.Sprintf("%T", input))

	var list []any
	if l, ok := input.([]any); ok {
		list = l
	} else if lMap, ok := input.([]map[string]any); ok {
		list = make([]any, len(lMap))
		for i, v := range lMap {
			list[i] = v
		}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}
	return e.stageLimit(list, args)
}

// handleInto checks for 'into' argument and drains cursor/list to a store.
func (e *ScriptEngine) handleInto(ctx context.Context, input any, args map[string]any) (any, error) {
	intoStoreName, _ := args["into"].(string)
	if intoStoreName == "" {
		return input, nil
	}

	intoStoreName = e.resolveVarName(intoStoreName)

	store, err := e.OpenStore(ctx, map[string]any{"name": intoStoreName, "create": true})
	if err != nil {
		return nil, fmt.Errorf("failed to open/create store '%s': %v", intoStoreName, err)
	}

	cursor, isCursor := input.(ScriptCursor)
	list, isList := input.([]any)

	if !isCursor && !isList {

		if lMap, ok := input.([]map[string]any); ok {
			for _, v := range lMap {
				list = append(list, v)
			}
			isList = true
		} else {
			return nil, fmt.Errorf("cannot pour result of type %T into store (must be cursor or list)", input)
		}
	}

	count := 0

	addToStore := func(item any) error {
		count++

		key := fmt.Sprintf("row_%d_%d", time.Now().UnixNano(), count)
		_, err := store.Add(ctx, key, item)
		return err
	}

	if isCursor {
		defer cursor.Close()
		for {
			item, ok, err := cursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			if err := addToStore(item); err != nil {
				return nil, err
			}
		}
	} else {
		for _, item := range list {
			if err := addToStore(item); err != nil {
				return nil, err
			}
		}
	}

	return store, nil
}

func (e *ScriptEngine) Join(ctx context.Context, input any, args map[string]any) (any, error) {
	rightVar, _ := args["with"].(string)
	if rightVar == "" {
		rightVar, _ = args["store"].(string)
	}
	if rightVar == "" {
		rightVar, _ = args["right_store"].(string)
	}
	if rightVar == "" {
		rightVar, _ = args["right"].(string)
	}

	rightVar = e.resolveVarName(rightVar)

	if rightVar == "" {
		return nil, fmt.Errorf("operation 'join' failed: right input variable '' not found")
	}

	joinType, _ := args["type"].(string)
	if joinType == "" {
		joinType = "inner"
	} else {
		joinType = strings.ToLower(strings.TrimSpace(joinType))
		if strings.Contains(joinType, "outer") {
			joinType = strings.ReplaceAll(joinType, " outer", "")
		}
		if strings.Contains(joinType, "join") {
			joinType = strings.ReplaceAll(joinType, " join", "")
		}
		joinType = strings.TrimSpace(joinType)
	}

	on, _ := args["on"].(map[string]any)
	if on == nil {
		if onList, ok := args["on"].([]any); ok {
			on = make(map[string]any)
			for _, v := range onList {
				if s, ok := v.(string); ok {
					on[s] = s
				}
			}
		}
	}

	if on != nil {
		newOn := make(map[string]any)
		changed := false
		for k, v := range on {
			parts := strings.SplitN(k, ".", 2)
			if len(parts) == 2 {
				prefix := parts[0]
				if val, ok := e.Context.Variables[prefix]; ok {
					// Variable exists
					var realName string
					if s, ok := val.(jsondb.StoreAccessor); ok {
						realName = s.GetStoreInfo().Name
					}

					if realName != "" && realName != prefix {
						newKey := realName + "." + parts[1]
						newOn[newKey] = v
						changed = true
						continue
					}
				}
			}
			newOn[k] = v
		}
		if changed {
			on = newOn
		}
	}

	// Prepare Left Cursor
	var leftCursor ScriptCursor
	if lc, ok := input.(ScriptCursor); ok {
		leftCursor = lc
	} else if list, ok := input.([]map[string]any); ok {
		var anyList []any
		for _, x := range list {
			anyList = append(anyList, x)
		}
		leftCursor = &ListCursor{items: anyList}
	} else if anyList, ok := input.([]any); ok {
		leftCursor = &ListCursor{items: anyList}
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}

	rightStore, isRightStore := e.getStore(rightVar)
	if !isRightStore && rightVar != "" {
		if rightInput, ok := e.Context.Variables[rightVar]; ok {
			if store, ok := rightInput.(jsondb.StoreAccessor); ok && store != nil {
				rightStore = store
				isRightStore = true
			}
		}
	}
	if !isRightStore && rightVar != "" {
		openedStore, openErr := e.OpenStore(ctx, map[string]any{"name": rightVar})
		if openErr == nil && openedStore != nil {
			rightStore = openedStore
			isRightStore = true
			if e.Context.Stores == nil {
				e.Context.Stores = make(map[string]jsondb.StoreAccessor)
			}
			e.Context.Stores[rightVar] = openedStore
		}
	}

	rightAlias, _ := args["right_alias"].(string)
	if rightAlias == "" {
		rightAlias, _ = args["alias"].(string)
	}
	if rightAlias == "" {

		if isRightStore && rightStore != nil {
			rightAlias = rightStore.GetStoreInfo().Name
			if rightAlias == "" {
				rightAlias = rightVar
			}
		} else if a, ok := args["store"].(string); ok && a != "" {
			rightAlias = strings.TrimPrefix(a, "@")
		} else if a, ok := args["right_dataset"].(string); ok && a != "" {
			rightAlias = strings.TrimPrefix(a, "@")
		} else {
			rightAlias = rightVar
		}
	}

	leftAlias, _ := args["left_alias"].(string)

	log.Debug("Join", "RightVar", rightVar, "IsStore", isRightStore, "JoinType", joinType)

	var result any
	var err error

	if joinType == "full" && isRightStore {
		// Materialize Left Input locally (needed for Right Anti-Join part)
		var leftList []any
		if lc, ok := leftCursor.(*ListCursor); ok {
			leftList = lc.items
		} else {

			for {
				item, ok, err := leftCursor.Next(ctx)
				if err != nil {
					return nil, err
				}
				if !ok {
					break
				}
				leftList = append(leftList, item)
			}

			leftCursor = &ListCursor{items: leftList}
		}

		invertedOn := make(map[string]any)
		for k, v := range on {
			invertedOn[fmt.Sprintf("%v", v)] = k
		}

		leftJoinCursor := &JoinRightCursor{
			left:           leftCursor,
			right:          rightStore,
			joinType:       "left",
			on:             on,
			ctx:            ctx,
			engine:         e,
			rightStoreName: rightAlias,
			leftStoreName:  leftAlias,
		}

		rightAntiJoinCursor := &RightOuterJoinStoreCursor{
			rightStore:      rightStore,
			leftList:        leftList,
			on:              invertedOn,
			ctx:             ctx,
			engine:          e,
			rightAlias:      rightAlias,
			suppressMatches: true,
		}

		multi := &MultiCursor{cursors: []ScriptCursor{leftJoinCursor, rightAntiJoinCursor}}
		return e.handleInto(ctx, multi, args)
	}

	if joinType == "right" {

		invertedOn := make(map[string]any)
		for k, v := range on {
			invertedOn[fmt.Sprintf("%v", v)] = k
		}

		// Materialize Left Input locally (as it mimics "Look up" table in this reversed join)
		// We pass the cursor to RightOuterJoinStoreCursor to let it handle materialization (or spilling) lazily.
		var leftList []any
		if lc, ok := leftCursor.(*ListCursor); ok {
			leftList = lc.items
		}

		if isRightStore {

			result, err = &RightOuterJoinStoreCursor{
				rightStore: rightStore,

				leftCursor: leftCursor,
				leftList:   leftList,
				on:         invertedOn,
				ctx:        ctx,
				engine:     e,
				rightAlias: rightAlias,
				leftAlias:  leftAlias,
			}, nil
		} else {

			if len(leftList) == 0 && leftCursor != nil {

				for {
					item, ok, err := leftCursor.Next(ctx)
					if err != nil {
						return nil, err
					}
					if !ok {
						break
					}
					leftList = append(leftList, item)
				}
			}

			rightInput, ok := e.Context.Variables[rightVar]
			if !ok {
				return nil, fmt.Errorf("right input variable '%s' not found", rightVar)
			}
			var rightList []any
			if l, ok := rightInput.([]map[string]any); ok {
				for _, x := range l {
					rightList = append(rightList, x)
				}
			} else if l, ok := rightInput.([]any); ok {
				rightList = l
			} else {
				return nil, fmt.Errorf("right input must be a list of items")
			}

			lAlias := leftAlias
			if lAlias == "" {
				lAlias = "Left"
			}
			rAlias := rightAlias
			if rAlias == "" {
				rAlias = "Right"
			}

			result, err = e.stageJoin(rightList, leftList, "left", invertedOn, lAlias, rAlias)
		}

		if err == nil {
			return e.handleInto(ctx, result, args)
		}
		return result, err
	}

	useLookupJoin := false
	if isRightStore {
		useLookupJoin = true
	}

	if useLookupJoin {

		cursor := &JoinRightCursor{
			left:           leftCursor,
			right:          rightStore,
			joinType:       joinType,
			on:             on,
			ctx:            ctx,
			engine:         e,
			rightStoreName: rightAlias,
		}

		return e.handleInto(ctx, cursor, args)
	}

	rightInput, ok := e.Context.Variables[rightVar]
	if !ok {
		return nil, fmt.Errorf("right input variable '%s' not found", rightVar)
	}

	var rightList []any
	if l, ok := rightInput.([]map[string]any); ok {
		for _, x := range l {
			rightList = append(rightList, x)
		}
	} else if l, ok := rightInput.([]any); ok {
		rightList = l
	} else {
		return nil, fmt.Errorf("right input must be a list of items")
	}

	// Materialize Left Cursor
	var leftList []any
	if lc, ok := leftCursor.(*ListCursor); ok {
		leftList = lc.items
	} else {

		for {
			item, ok, err := leftCursor.Next(ctx)
			if err != nil {
				return nil, err
			}
			if !ok {
				break
			}
			leftList = append(leftList, item)
		}
	}

	result, err = e.stageJoin(leftList, rightList, joinType, on, rightAlias, leftAlias)
	if err == nil {

		return e.handleInto(ctx, result, args)
	}
	return result, err
}
