package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/jsondb"
)

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

	storeName, _ := args["store"].(string)
	if storeName == "" {
		return "", fmt.Errorf("store name is required")
	}

	// Check if storeName is a Macro (View)
	if a.systemDB != nil {
		// Try to find macro in "general" category (default)
		// We need a transaction for systemDB
		sysTx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
		if err == nil {
			macroStore, err := a.systemDB.OpenModelStore(ctx, "macros", sysTx)
			if err == nil {
				var macro ai.Macro
				if err := macroStore.Load(ctx, "general", storeName, &macro); err == nil {
					sysTx.Commit(ctx)
					// Found Macro! Execute it.
					return a.executeMacroView(ctx, macro, args)
				}
			}
			sysTx.Rollback(ctx)
		}
	}

	// Parse Fields
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

	// Parse Limit
	limit, _ := args["limit"].(float64)
	if limit <= 0 {
		limit = 100
	}

	// Parse Key Match
	var keyMatch any
	if k, ok := args["key"]; ok {
		keyMatch = k
	} else if k, ok := args["key_match"]; ok {
		keyMatch = k
	}

	// Parse Value Match
	var valueMatch any
	if v, ok := args["value"]; ok {
		valueMatch = v
	} else if v, ok := args["value_match"]; ok {
		valueMatch = v
	} else {
		// If "value" is not explicitly provided, check if there are other args that are not reserved
		// This allows "select(store='users', age=30)" style
		valMap := make(map[string]any)
		for k, v := range args {
			if strings.HasPrefix(k, "_") {
				continue
			}
			if k != "store" && k != "key" && k != "key_match" && k != "database" && k != "fields" && k != "limit" && k != "action" && k != "update_values" && k != "value_match" {
				valMap[k] = v
			}
		}
		if len(valMap) > 0 {
			valueMatch = valMap
		}
	}

	// Parse Action
	action, _ := args["action"].(string)
	isDelete := action == "delete"
	isUpdate := action == "update"
	updateValues, _ := args["update_values"].(map[string]any)

	if isUpdate && len(updateValues) == 0 {
		return "", fmt.Errorf("update_values required for update action")
	}

	mode := sop.ForReading
	if isDelete || isUpdate {
		mode = sop.ForWriting
	}

	tx, autoCommit, err := a.resolveTransaction(ctx, db, dbName, mode)
	if err != nil {
		return "", err
	}
	if autoCommit {
		defer tx.Rollback(ctx)
	}

	store, err := jsondb.OpenStore(ctx, db.Config(), storeName, tx)
	if err != nil {
		return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
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

	emitter := NewResultEmitter(ctx)
	count := 0

	var ok bool
	fmt.Printf("keyMatch: %v (type: %T)\n", keyMatch, keyMatch)
	if keyMatch != nil {
		// Check if keyMatch is a simple value or a map with operators
		isSimpleKey := true
		if m, ok := keyMatch.(map[string]any); ok {
			for k := range m {
				if k == "$gt" || k == "$gte" || k == "$lt" || k == "$lte" || k == "$eq" || k == "$ne" {
					isSimpleKey = false
					break
				}
			}
		}

		if isSimpleKey {
			// Try direct lookup first
			_, err = store.FindOne(ctx, keyMatch, false)
			if err != nil {
				// Not found or error
			}
			// Check if current key matches criteria (or is start of range)
			k, _ := store.GetCurrentKey()
			if k != nil {
				ok = true
			}
		} else {
			ok, err = store.First(ctx)
			fmt.Printf("store.First returned: %v, %v\n", ok, err)
		}
	} else {
		ok, _ = store.First(ctx)
	}

	// Iterate
	for ok {
		k, _ := store.GetCurrentKey()
		if k == nil {
			break
		}

		// Check Key Match
		matched, _ := matchesKey(k, keyMatch)
		if matched {
			// Check Value Match
			var v any
			var vLoaded bool
			hasValueMatch := valueMatch != nil

			if hasValueMatch {
				v, err = store.GetCurrentValue(ctx)
				if err != nil {
					break
				}
				vLoaded = true
				matched, _ = matchesKey(v, valueMatch)
			}

			if matched {
				if !vLoaded {
					v, err = store.GetCurrentValue(ctx)
					if err != nil {
						break
					}
				}

				if isUpdate {
					var newVal any
					if vMap, ok := v.(map[string]any); ok {
						newVal = mergeMap(vMap, updateValues)
					} else {
						newVal = updateValues
					}
					if ok, err := store.Update(ctx, k, newVal); err != nil || !ok {
						return "", fmt.Errorf("failed to update item: %v", err)
					}
					count++
				} else if isDelete {
					kToDelete := k
					hasNext, _ := store.Next(ctx)
					if _, err := store.Remove(ctx, kToDelete); err != nil {
						return "", fmt.Errorf("failed to delete item: %w", err)
					}
					count++

					if count >= int(limit) {
						break
					}
					if !hasNext {
						break
					}
					continue
				} else {
					// Select
					var keyFormatted any = k
					if indexSpec != nil {
						if m, ok := k.(map[string]any); ok {
							keyFormatted = OrderedKey{m: m, spec: indexSpec}
						}
					}

					itemMap := map[string]any{"key": keyFormatted, "value": v}
					finalItem := filterFields(itemMap, fields)
					emitter.Emit(finalItem)
					count++
				}

				if count >= int(limit) {
					break
				}
			}
		}

		ok, _ = store.Next(ctx)
	}

	if isDelete {
		emitter.Finalize()
		if autoCommit {
			if err := tx.Commit(ctx); err != nil {
				return "", fmt.Errorf("failed to commit delete transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"deleted_count": %d}`, count), nil
	}

	if isUpdate {
		emitter.Finalize()
		if autoCommit {
			if err := tx.Commit(ctx); err != nil {
				return "", fmt.Errorf("failed to commit update transaction: %w", err)
			}
		}
		return fmt.Sprintf(`{"updated_count": %d}`, count), nil
	}

	return emitter.Finalize(), nil
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
