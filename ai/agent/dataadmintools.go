package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/jsondb"
)

// registerTools registers all available tools for the DataAdminAgent.
func (a *DataAdminAgent) registerTools() {
	if a.registry == nil {
		a.registry = NewRegistry()
	}

	a.registry.Register("list_databases", "Lists all available databases.", "()", a.toolListDatabases)
	a.registry.Register("list_stores", "Lists all stores in the current or specified database.", "(database: string)", a.toolListStores)
	a.registry.Register("select", "Retrieve data from a store. Supports filtering by key subset and value subset. You can optionally specify a list of fields to return. Supports 'action'='delete' to delete matching records, or 'action'='update' to update matching records with 'update_values'. Supports MongoDB-style operators in key_match and value_match: $eq, $ne, $gt, $gte, $lt, $lte (e.g. {age: {$gt: 18}}).", "(database: string, store: string, limit: number, scan_limit: number, key_match: any, value_match: any, fields: []string, action: string, update_values: map[string]any)", a.toolSelect)
	a.registry.Register("manage_transaction", "Manage database transactions (begin, commit, rollback).", "(action: string)", a.toolManageTransaction)
	a.registry.Register("delete", "Delete an item from a store.", "(store: string, key: any)", a.toolDelete)
	a.registry.Register("add", "Add an item to a store. You can pass the value as a single 'value' argument, or pass individual fields as arguments.", "(store: string, key: any, value: any, ...fields)", a.toolAdd)
	a.registry.Register("update", "Update an item in a store. You can pass the value as a single 'value' argument, or pass individual fields as arguments.", "(store: string, key: any, value: any, ...fields)", a.toolUpdate)
	a.registry.Register("list_macros", "Lists all available macros.", "()", a.toolListMacros)
	a.registry.Register("get_macro_details", "Get details of a specific macro.", "(name: string)", a.toolGetMacroDetails)
	a.registry.Register("macro_insert_step", "Insert a step into a macro.", "(macro: string, index: number, type: string, ...params)", a.toolMacroInsertStep)
	a.registry.Register("macro_delete_step", "Delete a step from a macro.", "(macro: string, index: number)", a.toolMacroDeleteStep)
	a.registry.Register("macro_update_step", "Update a step in a macro.", "(macro: string, index: number, ...params)", a.toolMacroUpdateStep)
	a.registry.Register("macro_reorder_steps", "Move a step in a macro to a new position.", "(macro: string, from_index: number, to_index: number)", a.toolMacroReorderSteps)

	// Navigation tools
	a.registry.Register("find", "Find an item in a store. Returns exact match only. You can optionally specify a list of fields to return.", "(store: string, key: any, fields: []string)", a.toolFind)
	a.registry.Register("find_nearest", "Find an item in a store. If no exact match, returns the nearest items (previous and next). You can optionally specify a list of fields to return.", "(store: string, key: any, fields: []string)", a.toolFindNearest)
	a.registry.Register("next", "Move to the next item in a store.", "(store: string)", a.toolNext)
	a.registry.Register("previous", "Move to the previous item in a store.", "(store: string)", a.toolPrevious)
	a.registry.Register("first", "Move to the first item in a store.", "(store: string)", a.toolFirst)
	a.registry.Register("last", "Move to the last item in a store.", "(store: string)", a.toolLast)
	a.registry.Register("refactor_last_interaction", "Refactor the last interaction's steps into a new macro or block.", "(mode: string, name: string)", a.toolRefactorMacro)
	a.registry.Register("join", "Join two stores. Supports inner, left, right, full joins. Note: 'right' and 'full' joins are only supported when joining on primary keys (left_join_field='key' and right_join_field='key'). Supports 'action'='delete_left' to delete matching records from the left store, or 'action'='update_left' to update them with 'update_values'.", "(database: string, left_store: string, right_database: string, right_store: string, left_join_field: string, right_join_field: string, join_type: string, limit: number, action: string, update_values: map[string]any)", a.toolJoin)
}

func (a *DataAdminAgent) toolListDatabases(ctx context.Context, args map[string]any) (string, error) {
	var names []string
	for k := range a.databases {
		names = append(names, k)
	}
	sort.Strings(names)

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Databases: %s", strings.Join(names, ", ")))

	if a.systemDB != nil {
		sb.WriteString("\nSystem Database: system")
	}

	return sb.String(), nil
}

func (a *DataAdminAgent) toolListStores(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	if db == nil {
		return "", fmt.Errorf("database not found or not selected")
	}

	// Need transaction
	var tx sop.Transaction
	var autoCommit bool

	if p.Transaction != nil {
		if t, ok := p.Transaction.(sop.Transaction); ok {
			tx = t
		}
	}
	if tx == nil {
		// If no transaction in payload, try to start one on the DB
		if db != nil {
			var err error
			tx, err = db.BeginTransaction(ctx, sop.ForReading)
			if err != nil {
				return "", fmt.Errorf("failed to begin transaction: %w", err)
			}
			// Auto-commit if we started it locally
			autoCommit = true
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
	}
	stores, err := tx.GetStores(ctx)
	if err != nil {
		if autoCommit {
			tx.Rollback(ctx)
		}
		return "", fmt.Errorf("failed to list stores: %w", err)
	}
	sort.Strings(stores)
	if autoCommit {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	return fmt.Sprintf("Stores: %s", strings.Join(stores, ", ")), nil
}

func (a *DataAdminAgent) toolSelect(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	if db == nil {
		return "", fmt.Errorf("database not found or not selected")
	}

	storeName, _ := args["store"].(string)
	storeName = strings.TrimSpace(storeName)

	limit, _ := args["limit"].(float64)
	if limit == 0 {
		limit = 10
	}

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

	// Check if storeName is a macro (View)
	if a.systemDB != nil {
		sysTx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
		if err == nil {
			defer sysTx.Rollback(ctx)
			macroStore, err := a.systemDB.OpenModelStore(ctx, "macros", sysTx)
			if err == nil {
				var macro ai.Macro
				if err := macroStore.Load(ctx, "general", storeName, &macro); err == nil {
					// Prepare Streamer
					var targetStreamer ai.ResultStreamer
					var buffer *BufferingStreamer

					// Check for existing streamer in context
					if s, ok := ctx.Value(ai.CtxKeyResultStreamer).(ai.ResultStreamer); ok {
						targetStreamer = s
					} else {
						buffer = &BufferingStreamer{}
						targetStreamer = buffer
					}

					// Wrap in FilteringStreamer
					fs := &FilteringStreamer{
						wrapped: targetStreamer,
						fields:  fields,
						limit:   int(limit),
					}

					// Inject into context
					stepCtx := context.WithValue(ctx, ai.CtxKeyResultStreamer, fs)

					// Execute Macro (ignoring string result, relying on streamer)
					_, err := a.runMacroRaw(stepCtx, macro, args)
					if err != nil {
						return "", fmt.Errorf("failed to execute macro '%s': %w", storeName, err)
					}

					// If we buffered, return the result
					if buffer != nil {
						b, _ := json.Marshal(buffer.Items)
						return string(b), nil
					}

					// If we streamed to the caller, return empty string (or whatever convention)
					return "", nil
				}
			}
		}
	}

	scanLimit, _ := args["scan_limit"].(float64)
	if scanLimit == 0 {
		scanLimit = 1000
	}
	// Ensure we can at least scan enough to find 'limit' items if they are contiguous
	if scanLimit < limit {
		scanLimit = limit
	}

	keyMatch, hasKeyMatch := args["key_match"]
	valueMatch, hasValueMatch := args["value_match"]

	action, _ := args["action"].(string)
	isDelete := action == "delete"
	isUpdate := action == "update"

	var updateValues map[string]any
	if isUpdate {
		if uv, ok := args["update_values"].(map[string]any); ok {
			updateValues = uv
		} else {
			return "", fmt.Errorf("update_values (map) is required for action='update'")
		}
	}

	var tx sop.Transaction
	var autoCommit bool

	mode := sop.ForReading
	if isDelete || isUpdate {
		mode = sop.ForWriting
	}

	tx, autoCommit, err := a.resolveTransaction(ctx, db, dbName, mode)
	if err != nil {
		return "", err
	}

	// Determine if store uses complex keys
	// var indexSpec *jsondb.IndexSpecification

	var store jsondb.StoreAccessor

	// Try to get from cache
	var cache map[string]jsondb.StoreAccessor
	if p.Variables == nil {
		p.Variables = make(map[string]any)
	}
	if c, found := p.Variables["opened_stores"]; found {
		if typedCache, ok := c.(map[string]jsondb.StoreAccessor); ok {
			cache = typedCache
		}
	}
	if cache == nil {
		cache = make(map[string]jsondb.StoreAccessor)
		p.Variables["opened_stores"] = cache
	}

	// Only use cache if we are not in auto-commit mode (i.e. we are in a long-running transaction)
	if !autoCommit {
		if s, found := cache[storeName]; found {
			store = s
		}
	}

	if store == nil {
		var dbOpts sop.DatabaseOptions
		if opts, ok := a.databases[dbName]; ok {
			dbOpts = opts
		}

		store, err = jsondb.OpenStore(ctx, dbOpts, storeName, tx)
		if err != nil {
			if autoCommit {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to open store: %w", err)
		}
		if !autoCommit {
			cache[storeName] = store
		}
	}

	// Determine if store uses complex keys
	var indexSpec *jsondb.IndexSpecification
	si := store.GetStoreInfo()
	if si.MapKeyIndexSpecification != "" {
		var is jsondb.IndexSpecification
		if err := json.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
			indexSpec = &is
		}
	}

	// SMART SELECT: Auto-detect Key fields in value_match and move them to key_match
	if indexSpec != nil && hasValueMatch {
		vmMap, isVmMap := valueMatch.(map[string]any)
		if isVmMap {
			// Initialize keyMatch as map if it doesn't exist or isn't a map
			var kmMap map[string]any
			if hasKeyMatch {
				if existingKm, ok := keyMatch.(map[string]any); ok {
					kmMap = existingKm
				} else {
					// keyMatch is primitive, can't merge map fields into it easily without conflict
					// but we can try if the user provided a mixed bag.
					// For now, only proceed if keyMatch is nil or map.
				}
			} else {
				kmMap = make(map[string]any)
				hasKeyMatch = true
			}

			if kmMap != nil {
				movedCount := 0
				for _, field := range indexSpec.IndexFields {
					if val, found := vmMap[field.FieldName]; found {
						// Check if it's already in keyMatch (don't overwrite explicit key match)
						if _, exists := kmMap[field.FieldName]; !exists {
							kmMap[field.FieldName] = val
							delete(vmMap, field.FieldName)
							movedCount++
						}
					}
				}

				if movedCount > 0 {
					keyMatch = kmMap
					// If valueMatch is empty now, we can unset it to avoid unnecessary value checking
					if len(vmMap) == 0 {
						hasValueMatch = false
						valueMatch = nil
					} else {
						valueMatch = vmMap
					}
					log.Info("Smart Select: Moved fields from value_match to key_match based on schema", "count", movedCount, "store", storeName)
				}
			}
		}
	}

	// SMART CLEAN: Auto-detect Value fields in key_match and move them to value_match
	if indexSpec != nil && hasKeyMatch {
		kmMap, isKmMap := keyMatch.(map[string]any)
		if isKmMap {
			// Initialize valueMatch as map if it doesn't exist or isn't a map
			var vmMap map[string]any
			if hasValueMatch {
				if existingVm, ok := valueMatch.(map[string]any); ok {
					vmMap = existingVm
				} else {
					// valueMatch is primitive? Unlikely if we are here, but handle safely.
				}
			} else {
				vmMap = make(map[string]any)
				hasValueMatch = true
			}

			if vmMap != nil {
				// Build set of valid key fields
				validKeys := make(map[string]bool)
				for _, field := range indexSpec.IndexFields {
					validKeys[field.FieldName] = true
				}

				movedCount := 0
				// Iterate over keyMatch fields
				var fieldsToMove []string
				for k := range kmMap {
					if !validKeys[k] {
						fieldsToMove = append(fieldsToMove, k)
					}
				}

				for _, k := range fieldsToMove {
					val := kmMap[k]
					// Only move if not already in valueMatch (conflict resolution? prefer valueMatch?)
					if _, exists := vmMap[k]; !exists {
						vmMap[k] = val
						delete(kmMap, k)
						movedCount++
					} else {
						// If it exists in both, we should probably just remove it from keyMatch
						// as it's definitely not a key field.
						delete(kmMap, k)
					}
				}

				if movedCount > 0 || len(fieldsToMove) > 0 {
					valueMatch = vmMap
					// If keyMatch is empty now? That's fine, it means full scan (or FindOne with empty key?)
					if len(kmMap) == 0 {
						hasKeyMatch = false
						keyMatch = nil
					}
					log.Info("Smart Clean: Moved fields from key_match to value_match based on schema", "count", len(fieldsToMove), "store", storeName)
				}
			}
		}
	}

	// Check for ResultStreamer
	var streamer ai.ResultStreamer
	if s, ok := ctx.Value(ai.CtxKeyResultStreamer).(ai.ResultStreamer); ok {
		streamer = s
	}

	var ok bool
	isExactLookup := false

	// Optimization: Try to use FindOne for direct lookup or range start
	startKey := getOptimizationKey(keyMatch)
	if hasKeyMatch && startKey != nil {
		// Peek to align type
		if ok, _ := store.First(ctx); ok {
			currKey, _ := store.GetCurrentKey()
			startKey = alignType(startKey, currKey)
		}

		// If it's a primitive exact match, we mark it so we stop scanning after one mismatch
		if !isMap(keyMatch) {
			isExactLookup = true
			ok, err = store.FindOne(ctx, startKey, true)
		} else {
			// Range query or operator match - use FindOne to seek, but allow scanning
			// FindOne(..., false) finds the first item >= key
			ok, err = store.FindOne(ctx, startKey, false)
		}
	} else {
		ok, err = store.First(ctx)
	}

	var sb strings.Builder
	if streamer == nil {
		sb.WriteString("[\n")
	} else {
		streamer.BeginArray()
	}
	itemsFound := false
	count := 0

	if ok && err == nil {
		// Safety break to prevent infinite scanning if no matches found
		scanned := 0

		for {
			if scanned >= int(scanLimit) {
				break
			}
			scanned++

			k, err := store.GetCurrentKey()
			if err != nil {
				break
			}

			// Apply filter if needed
			var v any
			var vLoaded bool

			if hasKeyMatch {
				matched, unwrappedField := matchesKey(k, keyMatch)
				if !matched {
					if isExactLookup {
						break
					}
					if ok, _ := store.Next(ctx); !ok {
						break
					}
					continue
				}

				// Safety Check: If we unwrapped a field (e.g. "employee_id"), check if it exists in Value.
				// If it does, we must validate it there too to avoid false positives.
				if unwrappedField != "" {
					v, err = store.GetCurrentValue(ctx)
					if err != nil {
						break
					}
					vLoaded = true

					if vMap, ok := v.(map[string]any); ok {
						if valField, exists := vMap[unwrappedField]; exists {
							// Retrieve the filter for this field from keyMatch
							if kmMap, ok := keyMatch.(map[string]any); ok {
								if filterVal, ok := kmMap[unwrappedField]; ok {
									// Check if value matches the filter
									valMatched, _ := matchesKey(valField, filterVal)
									if !valMatched {
										// False positive!
										if ok, _ := store.Next(ctx); !ok {
											break
										}
										continue
									}
								}
							}
						}
					}
				}
			}

			// Apply value filter if needed
			if hasValueMatch {
				if !vLoaded {
					v, err = store.GetCurrentValue(ctx)
					if err != nil {
						break
					}
					vLoaded = true
				}
				matched, _ := matchesKey(v, valueMatch)
				if !matched {
					if ok, _ := store.Next(ctx); !ok {
						break
					}
					continue
				}
			}

			if isUpdate {
				v, err = store.GetCurrentValue(ctx)
				if err != nil {
					break
				}
				var newVal any
				if vMap, ok := v.(map[string]any); ok {
					newVal = mergeMap(vMap, updateValues)
				} else {
					// If original is not a map, replace it with the update map
					newVal = updateValues
				}
				if ok, err := store.Update(ctx, k, newVal); err != nil || !ok {
					return "", fmt.Errorf("failed to update item: %v (found=%v)", err, ok)
				}
				itemsFound = true
				count++
				if count >= int(limit) {
					break
				}
				if ok, _ = store.Next(ctx); !ok {
					break
				}
				continue
			}

			if isDelete {
				// Delete action
				if _, err := store.Remove(ctx, k); err != nil {
					return "", fmt.Errorf("failed to delete item: %w", err)
				}
				itemsFound = true // Mark as found so we return something meaningful
				count++
				// After delete, we must be careful with Next().
				// In SOP, Remove might invalidate the cursor's current position.
				// However, since we are iterating, we should have peeked or we rely on the store to handle it.
				// Safest approach if store doesn't support Next() after Remove() is to re-seek or use a separate collection phase.
				// Assuming SOP StoreAccessor supports Next() after Remove() or we need to restart scan?
				// Actually, standard B-Tree iterators often become invalid.
				// Let's assume we need to re-position or use a safer strategy.
				// Strategy: Since we are in a loop, let's try Next(). If it fails, we might need to re-seek.
				// But wait, if we deleted 'k', 'k' is gone. Next() from 'k' is undefined.
				// We should have moved to Next BEFORE deleting?
				// But we need to delete 'k'.
				// Correct pattern: Get Next Key -> Delete Current -> Seek Next Key.
				// But we don't have "Seek". We have FindOne.
				// Let's try to get Next key first.
				// But we can't move cursor and then delete previous.
				// Let's just use the fact that we are deleting.
				// If we delete, the "Next" item becomes the "Current" item in some implementations, or we need to search again.
				// Simple approach: Collect keys to delete, then delete them? No, we want streaming/bulk.
				// Let's assume for now we can just continue. If not, we might need to fix SOP iterator.
				// Actually, let's use a safer approach:
				// 1. Get Current Key (k)
				// 2. Move Next (ok, _ = store.Next())
				// 3. Delete k (store.Remove(ctx, k))
				// This requires us to store 'k' and delete it after moving.
				// But 'store' is the cursor. If we move it, we can't delete 'k' easily unless Remove takes a key (it does!).
				// So:
				// kToDelete := k
				// ok, _ = store.Next(ctx)
				// store.Remove(ctx, kToDelete)
				// This works!
				kToDelete := k
				if ok, _ = store.Next(ctx); !ok {
					// End of store, just delete and break
					store.Remove(ctx, kToDelete)
					break
				}
				store.Remove(ctx, kToDelete)
				if count >= int(limit) {
					break
				}
				continue
			}

			v, err = store.GetCurrentValue(ctx)
			if err != nil {
				break
			}

			// Format key if it's a map and we have an index spec
			var keyFormatted any = k
			if indexSpec != nil {
				if m, ok := k.(map[string]any); ok {
					keyFormatted = OrderedKey{m: m, spec: indexSpec}
				}
			}

			itemMap := map[string]any{"key": keyFormatted, "value": v}
			finalItem := filterFields(itemMap, fields)

			if streamer != nil {
				streamer.WriteItem(finalItem)
				itemsFound = true
			} else {
				// Stream item to buffer
				if itemsFound {
					sb.WriteString(",\n")
				}
				itemsFound = true

				b, err := json.Marshal(finalItem)
				if err != nil {
					// Skip invalid items? Or fail?
					// Let's skip for now to be safe
				} else {
					sb.WriteString("  ")
					sb.Write(b)
				}
			}

			count++
			if count >= int(limit) {
				break
			}
			if ok, _ := store.Next(ctx); !ok {
				break
			}
		}
	}

	if streamer != nil {
		streamer.EndArray()
	} else {
		sb.WriteString("\n]")
	}

	if isDelete {
		if autoCommit {
			if err := tx.Commit(ctx); err != nil {
				return "", fmt.Errorf("failed to commit delete transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"deleted_count": %d}`, count), nil
	}

	if isUpdate {
		if autoCommit {
			if err := tx.Commit(ctx); err != nil {
				return "", fmt.Errorf("failed to commit update transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"updated_count": %d}`, count), nil
	}

	if !itemsFound {
		if autoCommit {
			tx.Commit(ctx)
		}
		if streamer != nil {
			return "", nil
		}
		return "[]", nil
	}

	if autoCommit {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	if streamer != nil {
		return "", nil
	}
	return sb.String(), nil
}

func (a *DataAdminAgent) toolManageTransaction(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	action, _ := args["action"].(string)
	if action == "" {
		return "", fmt.Errorf("action is required")
	}

	if _, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
		return fmt.Sprintf("Transaction action '%s' recorded as macro step. (Skipped execution during recording)", action), nil
	}

	switch action {
	case "begin":
		// Check if transaction exists for this database
		if p.Transactions != nil {
			if _, ok := p.Transactions[dbName]; ok {
				return fmt.Sprintf("Transaction already active for database '%s'", dbName), nil
			}
		}
		// Legacy check
		if p.Transaction != nil && (dbName == "" || dbName == p.CurrentDB) {
			return "Transaction already active", nil
		}

		if db == nil {
			return "", fmt.Errorf("no database selected")
		}
		tx, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return "", fmt.Errorf("failed to begin transaction: %w", err)
		}

		if p.Transactions == nil {
			p.Transactions = make(map[string]any)
		}
		p.Transactions[dbName] = tx

		// Update legacy field if this is the current DB
		if dbName == p.CurrentDB {
			p.Transaction = tx
		}

		p.ExplicitTransaction = true
		return "Transaction started", nil

	case "commit":
		var tx sop.Transaction
		// Find transaction
		if p.Transactions != nil {
			if tAny, ok := p.Transactions[dbName]; ok {
				tx, _ = tAny.(sop.Transaction)
			}
		}
		if tx == nil && p.Transaction != nil && (dbName == "" || dbName == p.CurrentDB) {
			tx, _ = p.Transaction.(sop.Transaction)
		}

		if tx == nil {
			return fmt.Sprintf("No active transaction to commit for database '%s'", dbName), nil
		}

		commitErr := tx.Commit(ctx)

		// Cleanup
		if p.Transactions != nil {
			delete(p.Transactions, dbName)
		}
		if dbName == p.CurrentDB {
			p.Transaction = nil
		}
		p.ExplicitTransaction = false
		p.Variables = nil // Clear cache

		// Auto-restart logic (preserve existing behavior)
		if db != nil {
			newTx, beginErr := db.BeginTransaction(ctx, sop.ForWriting)
			if beginErr != nil {
				if commitErr != nil {
					return "", fmt.Errorf("commit failed: %v. AND failed to auto-start new one: %v", commitErr, beginErr)
				}
				return "Transaction committed, but failed to auto-start new one: " + beginErr.Error(), nil
			}

			if p.Transactions == nil {
				p.Transactions = make(map[string]any)
			}
			p.Transactions[dbName] = newTx
			if dbName == p.CurrentDB {
				p.Transaction = newTx
			}

			if commitErr != nil {
				return fmt.Sprintf("New transaction started, but previous commit failed: %v", commitErr), commitErr
			}
			return "Transaction committed (and new one started)", nil
		}

		if commitErr != nil {
			return "", fmt.Errorf("commit failed: %w", commitErr)
		}
		return "Transaction committed", nil

	case "rollback":
		var tx sop.Transaction
		// Find transaction
		if p.Transactions != nil {
			if tAny, ok := p.Transactions[dbName]; ok {
				tx, _ = tAny.(sop.Transaction)
			}
		}
		if tx == nil && p.Transaction != nil && (dbName == "" || dbName == p.CurrentDB) {
			tx, _ = p.Transaction.(sop.Transaction)
		}

		if tx == nil {
			return fmt.Sprintf("No active transaction to rollback for database '%s'", dbName), nil
		}

		if err := tx.Rollback(ctx); err != nil {
			return "", fmt.Errorf("rollback failed: %w", err)
		}

		// Cleanup
		if p.Transactions != nil {
			delete(p.Transactions, dbName)
		}
		if dbName == p.CurrentDB {
			p.Transaction = nil
		}
		p.ExplicitTransaction = false
		p.Variables = nil

		// Auto-restart logic
		if db != nil {
			newTx, err := db.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return "Transaction rolled back, but failed to auto-start new one: " + err.Error(), nil
			}
			if p.Transactions == nil {
				p.Transactions = make(map[string]any)
			}
			p.Transactions[dbName] = newTx
			if dbName == p.CurrentDB {
				p.Transaction = newTx
			}
			return "Transaction rolled back (and new one started)", nil
		}

		return "Transaction rolled back", nil

	default:
		return "", fmt.Errorf("unknown action: %s", action)
	}
}

func (a *DataAdminAgent) toolDelete(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	storeName, _ := args["store"].(string)
	var key any
	if k, ok := args["key"]; ok {
		key = k
	}

	if storeName == "" || key == nil {
		return "", fmt.Errorf("store and key are required")
	}

	var tx sop.Transaction
	var localTx bool

	tx, localTx, err := a.resolveTransaction(ctx, db, dbName, sop.ForWriting)
	if err != nil {
		return "", err
	}

	var isPrimitiveKey bool
	if t2, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, storeName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
		}
	}

	// Prepare key based on store type
	if !isPrimitiveKey {
		if keyStr, ok := key.(string); ok {
			var keyMap map[string]any
			if err := json.Unmarshal([]byte(keyStr), &keyMap); err != nil {
				if localTx {
					tx.Rollback(ctx)
				}
				return "", fmt.Errorf("failed to parse complex key JSON: %v", err)
			}
			key = keyMap
		}
		if _, ok := key.(map[string]any); !ok {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("key must be a map or JSON string for complex key store")
		}
	} else {
		if _, ok := key.(string); !ok {
			key = fmt.Sprintf("%v", key)
		}
	}

	var store jsondb.StoreAccessor

	// Check cache first
	cacheKey := fmt.Sprintf("store_%s", storeName)
	if p.Variables != nil {
		if s, ok := p.Variables[cacheKey].(jsondb.StoreAccessor); ok {
			store = s
		}
	}

	if store == nil {
		store, err = jsondb.OpenStore(ctx, db.Config(), storeName, tx)
		if err != nil {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
		}
		// Cache it only if we are in a long-running transaction (not local/auto-commit)
		if !localTx {
			if p.Variables == nil {
				p.Variables = make(map[string]any)
			}
			p.Variables[cacheKey] = store
		}
	}

	var found bool
	found, err = store.Remove(ctx, key)
	if err != nil {
		if localTx {
			tx.Rollback(ctx)
		}
		return "", fmt.Errorf("failed to delete item '%v': %w", key, err)
	}

	if !found {
		if localTx {
			tx.Rollback(ctx)
		}
		return fmt.Sprintf("Item '%v' not found in store '%s'", key, storeName), nil
	}

	if localTx {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit delete transaction: %w", err)
		}
	}

	return fmt.Sprintf("Item '%v' deleted from store '%s'", key, storeName), nil
}

func (a *DataAdminAgent) toolAdd(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	storeName, _ := args["store"].(string)
	var key any
	if k, ok := args["key"]; ok {
		key = k
	}
	var value any
	if v, ok := args["value"]; ok {
		value = v
	} else {
		// If "value" is not explicitly provided, try to construct it from other arguments
		// This allows the LLM to pass fields directly as arguments (e.g. name="John", age=30)
		valMap := make(map[string]any)
		for k, v := range args {
			if k != "store" && k != "key" && k != "database" && k != "action" {
				valMap[k] = v
			}
		}
		if len(valMap) > 0 {
			value = valMap
		}
	}

	if storeName == "" || key == nil || value == nil {
		return "", fmt.Errorf("store, key and value (or fields) are required")
	}

	var tx sop.Transaction
	var localTx bool

	tx, localTx, err := a.resolveTransaction(ctx, db, dbName, sop.ForWriting)
	if err != nil {
		return "", err
	}

	var isPrimitiveKey bool
	if t2, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, storeName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
		}
	}

	// Prepare key based on store type
	if !isPrimitiveKey {
		if keyStr, ok := key.(string); ok {
			var keyMap map[string]any
			if err := json.Unmarshal([]byte(keyStr), &keyMap); err != nil {
				if localTx {
					tx.Rollback(ctx)
				}
				return "", fmt.Errorf("failed to parse complex key JSON: %v", err)
			}
			key = keyMap
		}
		if _, ok := key.(map[string]any); !ok {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("key must be a map or JSON string for complex key store")
		}
	} else {
		if _, ok := key.(string); !ok {
			key = fmt.Sprintf("%v", key)
		}
	}

	// Prepare value (try to unmarshal if string)
	if valStr, ok := value.(string); ok {
		var valMap map[string]any
		if err := json.Unmarshal([]byte(valStr), &valMap); err == nil {
			value = valMap
		} else {
			var valArr []any
			if err := json.Unmarshal([]byte(valStr), &valArr); err == nil {
				value = valArr
			}
		}
	}

	var store jsondb.StoreAccessor

	// Check cache first
	cacheKey := fmt.Sprintf("store_%s", storeName)
	if p.Variables != nil {
		if s, ok := p.Variables[cacheKey].(jsondb.StoreAccessor); ok {
			store = s
		}
	}

	if store == nil {
		store, err = jsondb.OpenStore(ctx, db.Config(), storeName, tx)
		if err != nil {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
		}
		// Cache it only if we are in a long-running transaction (not local/auto-commit)
		if !localTx {
			if p.Variables == nil {
				p.Variables = make(map[string]any)
			}
			p.Variables[cacheKey] = store
		}
	}

	var ok bool
	ok, err = store.Add(ctx, key, value)
	if err != nil {
		if localTx {
			tx.Rollback(ctx)
		}
		return "", fmt.Errorf("failed to add item: %w", err)
	}
	if !ok {
		if localTx {
			tx.Rollback(ctx)
		}
		return fmt.Sprintf("Item with key '%v' already exists in store '%s'", key, storeName), nil
	}

	if localTx {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit add transaction: %w", err)
		}
	}

	return fmt.Sprintf("Item added to store '%s'", storeName), nil
}

func (a *DataAdminAgent) toolUpdate(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	storeName, _ := args["store"].(string)
	var key any
	if k, ok := args["key"]; ok {
		key = k
	}
	var value any
	if v, ok := args["value"]; ok {
		value = v
	} else {
		// If "value" is not explicitly provided, try to construct it from other arguments
		// This allows the LLM to pass fields directly as arguments (e.g. name="John", age=30)
		valMap := make(map[string]any)
		for k, v := range args {
			if k != "store" && k != "key" && k != "database" && k != "action" {
				valMap[k] = v
			}
		}
		if len(valMap) > 0 {
			value = valMap
		}
	}

	if storeName == "" || key == nil || value == nil {
		return "", fmt.Errorf("store, key and value (or fields) are required")
	}

	var tx sop.Transaction
	var localTx bool

	tx, localTx, err := a.resolveTransaction(ctx, db, dbName, sop.ForWriting)
	if err != nil {
		return "", err
	}

	var isPrimitiveKey bool
	if t2, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, storeName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
		}
	}

	// Prepare key based on store type
	if !isPrimitiveKey {
		if keyStr, ok := key.(string); ok {
			var keyMap map[string]any
			if err := json.Unmarshal([]byte(keyStr), &keyMap); err != nil {
				if localTx {
					tx.Rollback(ctx)
				}
				return "", fmt.Errorf("failed to parse complex key JSON: %v", err)
			}
			key = keyMap
		}
		if _, ok := key.(map[string]any); !ok {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("key must be a map or JSON string for complex key store")
		}
	} else {
		if _, ok := key.(string); !ok {
			key = fmt.Sprintf("%v", key)
		}
	}

	// Prepare value (try to unmarshal if string)
	if valStr, ok := value.(string); ok {
		var valMap map[string]any
		if err := json.Unmarshal([]byte(valStr), &valMap); err == nil {
			value = valMap
		} else {
			var valArr []any
			if err := json.Unmarshal([]byte(valStr), &valArr); err == nil {
				value = valArr
			}
		}
	}

	var store jsondb.StoreAccessor

	// Check cache first
	cacheKey := fmt.Sprintf("store_%s", storeName)
	if p.Variables != nil {
		if s, ok := p.Variables[cacheKey].(jsondb.StoreAccessor); ok {
			store = s
		}
	}

	if store == nil {
		store, err = jsondb.OpenStore(ctx, db.Config(), storeName, tx)
		if err != nil {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
		}
		// Cache it only if we are in a long-running transaction (not local/auto-commit)
		if !localTx {
			if p.Variables == nil {
				p.Variables = make(map[string]any)
			}
			p.Variables[cacheKey] = store
		}
	}

	var ok bool
	ok, err = store.Update(ctx, key, value)
	if err != nil {
		if localTx {
			tx.Rollback(ctx)
		}
		return "", fmt.Errorf("failed to update item: %w", err)
	}
	if !ok {
		if localTx {
			tx.Rollback(ctx)
		}
		return fmt.Sprintf("Item with key '%v' not found in store '%s'", key, storeName), nil
	}

	if localTx {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit update transaction: %w", err)
		}
	}

	return fmt.Sprintf("Item updated in store '%s'", storeName), nil
}

func (a *DataAdminAgent) toolListMacros(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Look for "system" database
	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}

	if db == nil {
		return "", fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		return "", fmt.Errorf("failed to open macros store: %w", err)
	}

	names, err := store.List(ctx, "general")
	if err != nil {
		return "", fmt.Errorf("failed to list macros: %w", err)
	}

	if len(names) == 0 {
		return "No macros found.", nil
	}
	sort.Strings(names)
	return fmt.Sprintf("Macros: %s", strings.Join(names, ", ")), nil
}

func (a *DataAdminAgent) toolGetMacroDetails(ctx context.Context, args map[string]any) (string, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return "", fmt.Errorf("macro name required")
	}

	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Look for "system" database
	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}

	if db == nil {
		return "", fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return "", fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		return "", fmt.Errorf("failed to open macros store: %w", err)
	}

	var macro ai.Macro
	if err := store.Load(ctx, "general", name, &macro); err != nil {
		return "", fmt.Errorf("failed to load macro '%s': %w", name, err)
	}

	// Format details
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Macro: %s\n", macro.Name))
	if macro.Description != "" {
		sb.WriteString(fmt.Sprintf("Description: %s\n", macro.Description))
	}
	if macro.Database != "" {
		sb.WriteString(fmt.Sprintf("Database: %s\n", macro.Database))
	}
	if macro.Portable {
		sb.WriteString("Portable: true\n")
	}
	sb.WriteString("Steps:\n")
	printSteps(&sb, macro.Steps, 0)

	return sb.String(), nil
}

func printSteps(sb *strings.Builder, steps []ai.MacroStep, indent int) {
	indentStr := strings.Repeat("  ", indent)
	for i, step := range steps {
		desc := step.Message
		if step.Type == "ask" {
			desc = step.Prompt
		} else if step.Type == "macro" {
			desc = fmt.Sprintf("Run '%s'", step.MacroName)
		} else if step.Type == "command" {
			argsJSON, _ := json.Marshal(step.Args)
			desc = fmt.Sprintf("Execute '%s' %s", step.Command, string(argsJSON))
		} else if step.Type == "block" {
			desc = fmt.Sprintf("Sequence of %d steps", len(step.Steps))
		} else if step.Type == "loop" {
			desc = fmt.Sprintf("For %s in %s", step.Iterator, step.List)
		} else if step.Type == "if" {
			desc = fmt.Sprintf("If %s", step.Condition)
		}

		sb.WriteString(fmt.Sprintf("%s%d. [%s] %s\n", indentStr, i+1, step.Type, desc))

		if step.Type == "block" || step.Type == "loop" {
			printSteps(sb, step.Steps, indent+1)
		} else if step.Type == "if" {
			if len(step.Then) > 0 {
				sb.WriteString(fmt.Sprintf("%s  Then:\n", indentStr))
				printSteps(sb, step.Then, indent+2)
			}
			if len(step.Else) > 0 {
				sb.WriteString(fmt.Sprintf("%s  Else:\n", indentStr))
				printSteps(sb, step.Else, indent+2)
			}
		}
	}
}

func (a *DataAdminAgent) toolFind(ctx context.Context, args map[string]any) (string, error) {
	return a.runNavigation(ctx, args, func(ctx context.Context, store jsondb.StoreAccessor) (bool, error) {
		key, ok := args["key"]
		if !ok {
			return false, fmt.Errorf("key is required")
		}
		return store.FindOne(ctx, key, true)
	})
}

func (a *DataAdminAgent) toolFindNearest(ctx context.Context, args map[string]any) (string, error) {
	return a.runNavigation(ctx, args, func(ctx context.Context, store jsondb.StoreAccessor) (bool, error) {
		key, ok := args["key"]
		if !ok {
			return false, fmt.Errorf("key is required")
		}
		return store.FindOne(ctx, key, true)
	}, true)
}

func (a *DataAdminAgent) toolNext(ctx context.Context, args map[string]any) (string, error) {
	return a.runNavigation(ctx, args, func(ctx context.Context, store jsondb.StoreAccessor) (bool, error) {
		return store.Next(ctx)
	})
}

func (a *DataAdminAgent) toolPrevious(ctx context.Context, args map[string]any) (string, error) {
	return a.runNavigation(ctx, args, func(ctx context.Context, store jsondb.StoreAccessor) (bool, error) {
		return store.Previous(ctx)
	})
}

func (a *DataAdminAgent) toolFirst(ctx context.Context, args map[string]any) (string, error) {
	return a.runNavigation(ctx, args, func(ctx context.Context, store jsondb.StoreAccessor) (bool, error) {
		return store.First(ctx)
	})
}

func (a *DataAdminAgent) toolLast(ctx context.Context, args map[string]any) (string, error) {
	return a.runNavigation(ctx, args, func(ctx context.Context, store jsondb.StoreAccessor) (bool, error) {
		return store.Last(ctx)
	})
}

func (a *DataAdminAgent) runNavigation(ctx context.Context, args map[string]any, op func(context.Context, jsondb.StoreAccessor) (bool, error), showNearest ...bool) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName == "" {
		dbName = p.CurrentDB
	}

	if dbName != "" {
		if dbName == "system" && a.systemDB != nil {
			db = a.systemDB
		} else if opts, ok := a.databases[dbName]; ok {
			db = database.NewDatabase(opts)
		}
	}

	storeName, _ := args["store"].(string)
	if storeName == "" {
		return "", fmt.Errorf("store name is required")
	}

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

	var tx sop.Transaction
	var localTx bool

	tx, localTx, err := a.resolveTransaction(ctx, db, dbName, sop.ForReading)
	if err != nil {
		return "", err
	}

	// Navigation requires a persistent transaction for stateful cursors.
	// If we started a new transaction, persist it in the session.
	if localTx {
		p.Transaction = tx
		// Update CurrentDB to match the new transaction
		if dbName != "" {
			p.CurrentDB = dbName
		}
	}

	// Check cache for store
	var store jsondb.StoreAccessor
	cacheKey := fmt.Sprintf("store_%s", storeName)
	if p.Variables != nil {
		if s, ok := p.Variables[cacheKey].(jsondb.StoreAccessor); ok {
			store = s
		}
	}

	if store == nil {
		store, err = jsondb.OpenStore(ctx, db.Config(), storeName, tx)
		if err != nil {
			return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
		}
		if p.Variables == nil {
			p.Variables = make(map[string]any)
		}
		p.Variables[cacheKey] = store
	}

	// Check for ResultStreamer
	var streamer ai.ResultStreamer
	if s, ok := ctx.Value(ai.CtxKeyResultStreamer).(ai.ResultStreamer); ok {
		streamer = s
	}

	// Determine if store uses complex keys
	var indexSpec *jsondb.IndexSpecification
	si := store.GetStoreInfo()
	if si.MapKeyIndexSpecification != "" {
		var is jsondb.IndexSpecification
		if err := json.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
			indexSpec = &is
		}
	}

	found, err := op(ctx, store)
	if err != nil {
		return "", fmt.Errorf("navigation failed: %w", err)
	}

	if !found {
		if len(showNearest) > 0 && showNearest[0] {
			var neighbors []map[string]any

			// 1. Check if we are at a valid item (this is the "Current" neighbor, usually >= key)
			k, err := store.GetCurrentKey()
			if err == nil && k != nil {
				v, _ := store.GetCurrentValue(ctx)

				// Format key if it's a map and we have an index spec
				var keyFormatted any = k
				if indexSpec != nil {
					if m, ok := k.(map[string]any); ok {
						keyFormatted = OrderedKey{m: m, spec: indexSpec}
					}
				}

				neighbors = append(neighbors, map[string]any{"key": keyFormatted, "value": v, "relation": "next_or_equal"})
			}

			// 2. Check previous item
			if k != nil {
				// We are at some item. Try to peek previous.
				if ok, _ := store.Previous(ctx); ok {
					k2, _ := store.GetCurrentKey()
					v2, _ := store.GetCurrentValue(ctx)

					// Format key if it's a map and we have an index spec
					var keyFormatted2 any = k2
					if indexSpec != nil {
						if m, ok := k2.(map[string]any); ok {
							keyFormatted2 = OrderedKey{m: m, spec: indexSpec}
						}
					}

					neighbors = append(neighbors, map[string]any{"key": keyFormatted2, "value": v2, "relation": "previous"})

					// Restore
					store.Next(ctx)
				}
			} else {
				// We are at End.
				if ok, _ := store.Previous(ctx); ok {
					// This is the Last item (Previous neighbor)
					k2, _ := store.GetCurrentKey()
					v2, _ := store.GetCurrentValue(ctx)

					// Format key if it's a map and we have an index spec
					var keyFormatted2 any = k2
					if indexSpec != nil {
						if m, ok := k2.(map[string]any); ok {
							keyFormatted2 = OrderedKey{m: m, spec: indexSpec}
						}
					}

					neighbors = append(neighbors, map[string]any{"key": keyFormatted2, "value": v2, "relation": "previous"})

					// We are now at Last. The original state was "End".
					// Restore to "End"
					store.Next(ctx)
				}
			}

			if len(neighbors) > 0 {
				if streamer != nil {
					streamer.BeginArray()
					for _, n := range neighbors {
						streamer.WriteItem(filterFields(n, fields))
					}
					streamer.EndArray()
					return "", nil
				}

				// Return JSON array string
				var filteredNeighbors []any
				for _, n := range neighbors {
					filteredNeighbors = append(filteredNeighbors, filterFields(n, fields))
				}
				b, _ := json.Marshal(filteredNeighbors)
				return string(b), nil
			}
		}

		if streamer != nil {
			streamer.BeginArray()
			streamer.EndArray()
			return "", nil
		}
		return "[]", nil
	}

	k, _ := store.GetCurrentKey()
	v, _ := store.GetCurrentValue(ctx)

	// Format key if it's a map and we have an index spec
	var keyFormatted any = k
	if indexSpec != nil {
		if m, ok := k.(map[string]any); ok {
			keyFormatted = OrderedKey{m: m, spec: indexSpec}
		}
	}

	item := map[string]any{"key": keyFormatted, "value": v}
	finalItem := filterFields(item, fields)

	if streamer != nil {
		streamer.BeginArray()
		streamer.WriteItem(finalItem)
		streamer.EndArray()
		return "", nil
	}

	// Return JSON representation
	b, _ := json.Marshal([]any{finalItem})
	return string(b), nil
}

func (a *DataAdminAgent) toolRefactorMacro(ctx context.Context, args map[string]any) (string, error) {
	mode, _ := args["mode"].(string)
	if mode == "" {
		return "", fmt.Errorf("mode is required (macro or block)")
	}
	name, _ := args["name"].(string)

	recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder)
	if !ok {
		return "", fmt.Errorf("no macro recorder available")
	}

	// We don't know the count here, but RefactorLastSteps can handle count=0 (use buffer)
	if err := recorder.RefactorLastSteps(0, mode, name); err != nil {
		return "", fmt.Errorf("refactor failed: %w", err)
	}

	if mode == "macro" {
		return fmt.Sprintf("Last interaction refactored into new macro '%s' and added as step.", name), nil
	}
	return "Last interaction refactored into a block step.", nil
}

func (a *DataAdminAgent) toolMacroInsertStep(ctx context.Context, args map[string]any) (string, error) {
	macroName, _ := args["macro"].(string)
	index, _ := args["index"].(float64)
	stepType, _ := args["type"].(string)

	if macroName == "" {
		return "", fmt.Errorf("macro name required")
	}
	if stepType == "" {
		return "", fmt.Errorf("step type required")
	}

	newStep := ai.MacroStep{
		Type: stepType,
	}

	// Populate fields based on args
	if v, ok := args["prompt"].(string); ok {
		newStep.Prompt = v
	}
	if v, ok := args["message"].(string); ok {
		newStep.Message = v
	}
	if v, ok := args["macro_name"].(string); ok {
		newStep.MacroName = v
	}
	if v, ok := args["command"].(string); ok {
		newStep.Command = v
	}
	if v, ok := args["condition"].(string); ok {
		newStep.Condition = v
	}
	if v, ok := args["iterator"].(string); ok {
		newStep.Iterator = v
	}
	if v, ok := args["list"].(string); ok {
		newStep.List = v
	}

	if v, ok := args["args"].(map[string]any); ok {
		newStep.Args = v
	}

	err := a.updateMacro(ctx, macroName, func(m *ai.Macro) error {
		idx := int(index)
		if idx < 0 || idx > len(m.Steps) {
			return fmt.Errorf("index out of bounds")
		}
		// Insert
		m.Steps = append(m.Steps[:idx], append([]ai.MacroStep{newStep}, m.Steps[idx:]...)...)
		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step inserted into macro '%s' at index %d", macroName, int(index)), nil
}

func (a *DataAdminAgent) toolMacroDeleteStep(ctx context.Context, args map[string]any) (string, error) {
	macroName, _ := args["macro"].(string)
	index, _ := args["index"].(float64)

	if macroName == "" {
		return "", fmt.Errorf("macro name required")
	}

	err := a.updateMacro(ctx, macroName, func(m *ai.Macro) error {
		idx := int(index)
		if idx < 0 || idx >= len(m.Steps) {
			return fmt.Errorf("index out of bounds")
		}
		// Delete
		m.Steps = append(m.Steps[:idx], m.Steps[idx+1:]...)
		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step deleted from macro '%s' at index %d", macroName, int(index)), nil
}

func (a *DataAdminAgent) toolMacroUpdateStep(ctx context.Context, args map[string]any) (string, error) {
	macroName, _ := args["macro"].(string)
	index, _ := args["index"].(float64)

	if macroName == "" {
		return "", fmt.Errorf("macro name required")
	}

	err := a.updateMacro(ctx, macroName, func(m *ai.Macro) error {
		idx := int(index)
		if idx < 0 || idx >= len(m.Steps) {
			return fmt.Errorf("index out of bounds")
		}

		step := &m.Steps[idx]

		// Update fields if present
		if v, ok := args["type"].(string); ok {
			step.Type = v
		}
		if v, ok := args["prompt"].(string); ok {
			step.Prompt = v
		}
		if v, ok := args["message"].(string); ok {
			step.Message = v
		}
		if v, ok := args["macro_name"].(string); ok {
			step.MacroName = v
		}
		if v, ok := args["command"].(string); ok {
			step.Command = v
		}
		if v, ok := args["condition"].(string); ok {
			step.Condition = v
		}
		if v, ok := args["iterator"].(string); ok {
			step.Iterator = v
		}
		if v, ok := args["list"].(string); ok {
			step.List = v
		}
		if v, ok := args["args"].(map[string]any); ok {
			step.Args = v
		}

		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step updated in macro '%s' at index %d", macroName, int(index)), nil
}

func (a *DataAdminAgent) toolMacroReorderSteps(ctx context.Context, args map[string]any) (string, error) {
	macroName, _ := args["macro"].(string)
	fromIndex, _ := args["from_index"].(float64)
	toIndex, _ := args["to_index"].(float64)

	if macroName == "" {
		return "", fmt.Errorf("macro name required")
	}

	err := a.updateMacro(ctx, macroName, func(m *ai.Macro) error {
		from := int(fromIndex)
		to := int(toIndex)
		if from < 0 || from >= len(m.Steps) {
			return fmt.Errorf("from_index out of bounds")
		}
		if to < 0 || to > len(m.Steps) { // Allow moving to end
			return fmt.Errorf("to_index out of bounds")
		}

		step := m.Steps[from]
		// Remove
		m.Steps = append(m.Steps[:from], m.Steps[from+1:]...)

		// Adjust 'to' if we removed an item before it
		if from < to {
			to--
		}

		// Insert
		m.Steps = append(m.Steps[:to], append([]ai.MacroStep{step}, m.Steps[to:]...)...)
		return nil
	})

	if err != nil {
		return "", err
	}
	return fmt.Sprintf("Step moved from %d to %d in macro '%s'", int(fromIndex), int(toIndex), macroName), nil
}

// Helper to update a macro transactionally
func (a *DataAdminAgent) updateMacro(ctx context.Context, name string, updateFunc func(*ai.Macro) error) error {
	// Look for "system" database
	db := a.systemDB
	if db == nil {
		if opts, ok := a.databases["system"]; ok {
			db = database.NewDatabase(opts)
		}
	}
	if db == nil {
		return fmt.Errorf("system database not found")
	}

	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	store, err := db.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		return fmt.Errorf("failed to open macros store: %w", err)
	}

	var macro ai.Macro
	if err := store.Load(ctx, "general", name, &macro); err != nil {
		return fmt.Errorf("failed to load macro '%s': %w", name, err)
	}

	if err := updateFunc(&macro); err != nil {
		return err
	}

	if err := store.Save(ctx, "general", name, &macro); err != nil {
		return fmt.Errorf("failed to save macro '%s': %w", name, err)
	}

	return tx.Commit(ctx)
}

// matchesKey checks if itemKey contains all fields and values from filterKey.
// Currently supports map[string]any for both.
// Returns match status and the name of the field that was "unwrapped" if any (for ambiguity checks).
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
			if v, ok := item[f]; ok {
				om.keys = append(om.keys, f)
				om.m[f] = v
			}
		}
		return om
	}

	// We must preserve the Key/Value structure for API consistency.
	// However, we want to respect the order of fields requested within Key and Value.

	var newKey any = nil
	var newValue any = nil

	// Helper to check if a field is requested
	isRequested := func(f string) bool {
		for _, field := range fields {
			if field == f {
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
				if v, ok := keyMap[f]; ok {
					om.keys = append(om.keys, f)
					om.m[f] = v
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
				if v, ok := keyMap[f]; ok {
					om.keys = append(om.keys, f)
					om.m[f] = v
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
				if v, ok := valMap[f]; ok {
					om.keys = append(om.keys, f)
					om.m[f] = v
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

// resolveTransaction resolves the transaction to use for the operation.
// It prefers the session transaction if available and compatible with the target database.
// Otherwise, it starts a new local transaction.
func (a *DataAdminAgent) resolveTransaction(ctx context.Context, db *database.Database, dbName string, mode sop.TransactionMode) (sop.Transaction, bool, error) {
	p := ai.GetSessionPayload(ctx)
	var tx sop.Transaction
	var localTx bool

	if p != nil {
		// 1. Check Transactions map (Multi-DB support)
		if p.Transactions != nil {
			if tAny, ok := p.Transactions[dbName]; ok {
				if t, ok := tAny.(sop.Transaction); ok {
					tx = t
				}
			}
		}

		// 2. Fallback to legacy Transaction field if not found in map
		// Only use if it matches the target database (or if dbName is empty/default)
		if tx == nil && p.Transaction != nil {
			if dbName == "" || dbName == p.CurrentDB {
				if t, ok := p.Transaction.(sop.Transaction); ok {
					tx = t
				}
			}
		}
	}

	if tx == nil {
		if db != nil {
			var err error
			tx, err = db.BeginTransaction(ctx, mode)
			if err != nil {
				return nil, false, fmt.Errorf("failed to begin transaction: %w", err)
			}
			localTx = true
		} else {
			return nil, false, fmt.Errorf("no active transaction and no database to start one")
		}
	}
	return tx, localTx, nil
}

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
	leftField, _ := args["left_join_field"].(string)
	rightField, _ := args["right_join_field"].(string)
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
	if leftField == "" {
		return "", fmt.Errorf("left_join_field is required")
	}
	if rightField == "" {
		rightField = "key"
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
	leftStore, err := jsondb.OpenStore(ctx, leftDb.Options(), leftStoreName, leftTx)
	if err != nil {
		return "", fmt.Errorf("failed to open left store: %w", err)
	}
	rightStore, err := jsondb.OpenStore(ctx, rightDb.Options(), rightStoreName, rightTx)
	if err != nil {
		return "", fmt.Errorf("failed to open right store: %w", err)
	}

	// Check for ResultStreamer
	var streamer ai.ResultStreamer
	if s, ok := ctx.Value(ai.CtxKeyResultStreamer).(ai.ResultStreamer); ok {
		streamer = s
	}

	var sb strings.Builder
	if streamer == nil {
		sb.WriteString("[\n")
	} else {
		streamer.BeginArray()
	}
	itemsFound := false

	// Helper to emit result
	emit := func(res map[string]any) {
		if streamer != nil {
			streamer.WriteItem(res)
			itemsFound = true
		} else {
			if itemsFound {
				sb.WriteString(",\n")
			}
			itemsFound = true
			b, _ := json.Marshal(res)
			sb.WriteString("  ")
			sb.Write(b)
		}
	}

	count := 0

	// Optimization: Key-Key Join (Merge Join Strategy)
	if leftField == "key" && rightField == "key" {
		lOk, _ := leftStore.First(ctx)
		rOk, _ := rightStore.First(ctx)

		for (lOk || rOk) && count < int(limit) {
			// Optimization for Inner Join: if either is EOF, we are done
			if joinType == "inner" && (!lOk || !rOk) {
				break
			}

			var cmp int
			// Determine comparison state
			if lOk && rOk {
				lKey, _ := leftStore.GetCurrentKey()
				rKey, _ := rightStore.GetCurrentKey()
				cmp = btree.Compare(lKey, rKey)
			} else if lOk {
				cmp = -1 // Right EOF, Left remains -> Left < "Infinity"
			} else {
				cmp = 1 // Left EOF, Right remains -> "Infinity" > Right
			}

			if cmp == 0 {
				// Match
				lVal, _ := leftStore.GetCurrentValue(ctx)
				rVal, _ := rightStore.GetCurrentValue(ctx)

				if isDeleteLeft {
					// Delete Left Item
					lKey, _ := leftStore.GetCurrentKey()
					if _, err := leftStore.Remove(ctx, lKey); err != nil {
						return "", fmt.Errorf("failed to delete left item: %w", err)
					}
					// For Merge Join, we need to be careful.
					// If we delete Left, we advance Left.
					// But wait, if we delete, does Next() work?
					// Similar to Select, let's assume we need to advance carefully.
					// Actually, in Merge Join, we control the cursor explicitly.
					// If we delete, we should advance.
					// Let's use the same pattern: Advance then Delete?
					// Or Delete then Advance?
					// If we delete, the cursor might be invalid.
					// Safe pattern:
					// 1. Get Next Key for Left (peek)
					// 2. Delete Current Left
					// 3. Reset Left Cursor to Next Key?
					// Since we are doing a linear scan, we can just:
					// lKeyToDelete := lKey
					// lOk, _ = leftStore.Next(ctx)
					// leftStore.Remove(ctx, lKeyToDelete)
					// But we also need to advance Right?
					// If it's a match, we advance both.
					lKeyToDelete := lKey
					lOk, _ = leftStore.Next(ctx)
					rOk, _ = rightStore.Next(ctx)
					leftStore.Remove(ctx, lKeyToDelete)
					count++
				} else if isUpdateLeft {
					lKey, _ := leftStore.GetCurrentKey()
					var newVal any
					if lMap, ok := lVal.(map[string]any); ok {
						newVal = mergeMap(lMap, updateValues)
					} else {
						newVal = updateValues
					}
					if ok, err := leftStore.Update(ctx, lKey, newVal); err != nil || !ok {
						return "", fmt.Errorf("failed to update left item: %v", err)
					}
					count++
					lOk, _ = leftStore.Next(ctx)
					rOk, _ = rightStore.Next(ctx)
				} else {
					emit(map[string]any{"left": lVal, "right": rVal})
					count++
					lOk, _ = leftStore.Next(ctx)
					rOk, _ = rightStore.Next(ctx)
				}
			} else if cmp < 0 {
				// Left < Right (Unmatched Left)
				if joinType == "left" || joinType == "full" {
					lVal, _ := leftStore.GetCurrentValue(ctx)
					if isDeleteLeft {
						// Delete Left Item (for Left/Full join where right is missing? No, this is just unmatched left)
						// If action is delete_left, do we delete unmatched lefts?
						// Usually 'delete_left' implies deleting records that satisfy the join condition.
						// If join_type is 'left', then unmatched records satisfy the join.
						// So yes, delete them.
						lKey, _ := leftStore.GetCurrentKey()
						lKeyToDelete := lKey
						lOk, _ = leftStore.Next(ctx)
						leftStore.Remove(ctx, lKeyToDelete)
						count++
					} else if isUpdateLeft {
						lKey, _ := leftStore.GetCurrentKey()
						var newVal any
						if lMap, ok := lVal.(map[string]any); ok {
							newVal = mergeMap(lMap, updateValues)
						} else {
							newVal = updateValues
						}
						if ok, err := leftStore.Update(ctx, lKey, newVal); err != nil || !ok {
							return "", fmt.Errorf("failed to update left item: %v", err)
						}
						count++
						lOk, _ = leftStore.Next(ctx)
					} else {
						emit(map[string]any{"left": lVal})
						count++
						lOk, _ = leftStore.Next(ctx)
					}
				} else {
					// Skip Left (Inner/Right join)
					// Optimization: Jump Left to Right Key
					if rOk {
						rKey, _ := rightStore.GetCurrentKey()
						// FindOne(rKey, false) -> finds first item >= rKey
						if found, _ := leftStore.FindOne(ctx, rKey, false); found {
							lOk = true
						} else {
							lOk = false
						}
					} else {
						lOk, _ = leftStore.Next(ctx)
					}
				}
			} else {
				// Right < Left (Unmatched Right)
				if joinType == "right" || joinType == "full" {
					rVal, _ := rightStore.GetCurrentValue(ctx)
					emit(map[string]any{"right": rVal})
					count++
					rOk, _ = rightStore.Next(ctx)
				} else {
					// Skip Right (Inner/Left join)
					// Optimization: Jump Right to Left Key
					if lOk {
						lKey, _ := leftStore.GetCurrentKey()
						if found, _ := rightStore.FindOne(ctx, lKey, false); found {
							rOk = true
						} else {
							rOk = false
						}
					} else {
						rOk, _ = rightStore.Next(ctx)
					}
				}
			}
		}
	} else {
		if joinType == "right" || joinType == "full" {
			return "", fmt.Errorf("right and full joins are only supported when joining on primary keys (left_join_field='key' and right_join_field='key')")
		}

		// Standard Nested Loop Join (Left Scan + Right Lookup)
		lOk, _ := leftStore.First(ctx)
		if !lOk {
			if streamer != nil {
				streamer.EndArray()
				return "", nil
			}
			return "[]", nil
		}

		for {
			if count >= int(limit) {
				break
			}

			k, _ := leftStore.GetCurrentKey()
			v, _ := leftStore.GetCurrentValue(ctx)

			// Extract Join Value from Left
			var joinVal any
			if leftField == "key" {
				joinVal = k
			} else {
				// Assume v is a map
				if vm, ok := v.(map[string]any); ok {
					joinVal = vm[leftField]
				}
			}

			var rightItem any
			var rightFound bool

			if joinVal != nil {
				if rightField == "key" {
					// Lookup by key
					var err error
					rightFound, err = rightStore.FindOne(ctx, joinVal, false)
					if err == nil && rightFound {
						rightItem, _ = rightStore.GetCurrentValue(ctx)
					}
				} else {
					return "", fmt.Errorf("joining on non-key field '%s' in right store is not yet supported", rightField)
				}
			}

			// Join Logic
			include := false
			if joinType == "inner" && rightFound {
				include = true
			} else if joinType == "left" {
				include = true
			}

			if include {
				if isDeleteLeft {
					// Delete Left Item
					// Same pattern: Advance then Delete
					kToDelete := k
					if ok, _ := leftStore.Next(ctx); !ok {
						// End of store
						leftStore.Remove(ctx, kToDelete)
						count++
						break
					}
					leftStore.Remove(ctx, kToDelete)
					count++
					continue
				}

				if isUpdateLeft {
					var newVal any
					if vMap, ok := v.(map[string]any); ok {
						newVal = mergeMap(vMap, updateValues)
					} else {
						newVal = updateValues
					}
					if ok, err := leftStore.Update(ctx, k, newVal); err != nil || !ok {
						return "", fmt.Errorf("failed to update left item: %v", err)
					}
					count++
				} else {
					res := map[string]any{
						"left": v,
					}
					if rightFound {
						res["right"] = rightItem
					}
					emit(res)
					count++
				}
			}

			if ok, _ := leftStore.Next(ctx); !ok {
				break
			}
		}
	}

	if streamer != nil {
		streamer.EndArray()
	} else {
		sb.WriteString("\n]")
	}

	if isDeleteLeft {
		if leftAutoCommit {
			if err := leftTx.Commit(ctx); err != nil {
				return "", fmt.Errorf("failed to commit delete transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"deleted_count": %d}`, count), nil
	}

	if isUpdateLeft {
		if leftAutoCommit {
			if err := leftTx.Commit(ctx); err != nil {
				return "", fmt.Errorf("failed to commit update transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"updated_count": %d}`, count), nil
	}

	if streamer == nil {
		return sb.String(), nil
	}
	return "", nil
}

// --- Streamer Helpers ---

type BufferingStreamer struct {
	Items []any
}

func (bs *BufferingStreamer) BeginArray() {}
func (bs *BufferingStreamer) EndArray()   {}
func (bs *BufferingStreamer) WriteItem(item any) {
	bs.Items = append(bs.Items, item)
}

type FilteringStreamer struct {
	wrapped ai.ResultStreamer
	fields  []string
	limit   int
	count   int
}

func (fs *FilteringStreamer) BeginArray() {
	fs.wrapped.BeginArray()
}

func (fs *FilteringStreamer) WriteItem(item any) {
	if fs.limit > 0 && fs.count >= fs.limit {
		return
	}

	var filtered any
	if len(fs.fields) > 0 {
		if mapItem, ok := item.(map[string]any); ok {
			filtered = filterFields(mapItem, fs.fields)
		} else {
			filtered = item
		}
	} else {
		filtered = item
	}

	fs.wrapped.WriteItem(filtered)
	fs.count++
}

func (fs *FilteringStreamer) EndArray() {
	fs.wrapped.EndArray()
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
	case int8:
		return float64(val)
	case int16:
		return float64(val)
	case int32:
		return float64(val)
	case int64:
		return float64(val)
	case float32:
		return float64(val)
	case float64:
		return val
	}
	return v
}

func convertToInt(v any) any {
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	}
	return v
}

func convertToString(v any) any {
	if isMap(v) {
		m := v.(map[string]any)
		newM := make(map[string]any)
		for k, val := range m {
			newM[k] = convertToString(val)
		}
		return newM
	}
	return fmt.Sprintf("%v", v)
}
