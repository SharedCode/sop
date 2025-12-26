package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
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
	a.registry.Register("select", "Retrieve data from a store.", "(database: string, store: string, limit: number)", a.toolSelect)
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
	a.registry.Register("find", "Find an item in a store.", "(store: string, key: any)", a.toolFind)
	a.registry.Register("next", "Move to the next item in a store.", "(store: string)", a.toolNext)
	a.registry.Register("previous", "Move to the previous item in a store.", "(store: string)", a.toolPrevious)
	a.registry.Register("first", "Move to the first item in a store.", "(store: string)", a.toolFirst)
	a.registry.Register("last", "Move to the last item in a store.", "(store: string)", a.toolLast)
	a.registry.Register("refactor_last_interaction", "Refactor the last interaction's steps into a new macro or block.", "(mode: string, name: string)", a.toolRefactorMacro)
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
			autoCommit = true
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
	}

	// Determine if store uses complex keys
	var isPrimitiveKey bool
	// var indexSpec *jsondb.IndexSpecification

	if t2, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, storeName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
			if !isPrimitiveKey && stores[0].MapKeyIndexSpecification != "" {
				// var is jsondb.IndexSpecification
				// if err := encoding.DefaultMarshaler.Unmarshal([]byte(stores[0].MapKeyIndexSpecification), &is); err == nil {
				// 	indexSpec = &is
				// }
			}
		}
	}

	var resultItems []map[string]any

	var store jsondb.StoreAccessor
	var err error

	// Check cache first
	cacheKey := fmt.Sprintf("store_%s", storeName)
	// fmt.Printf("DEBUG: toolSelect store='%s' cacheKey='%s' p.Variables=%v\n", storeName, cacheKey, p.Variables)
	if p.Variables != nil {
		if s, ok := p.Variables[cacheKey].(jsondb.StoreAccessor); ok {
			store = s
			// fmt.Printf("DEBUG: Cache HIT for %s\n", cacheKey)
		}
	}

	if store == nil {
		store, err = jsondb.OpenStore(ctx, db.Config(), storeName, tx)
		if err != nil {
			// Debugging info for cache failure
			hasCache := p.Variables != nil
			var cachedType string
			if hasCache {
				if v, ok := p.Variables[cacheKey]; ok {
					cachedType = fmt.Sprintf("%T", v)
				} else {
					cachedType = "missing"
				}
			}
			if autoCommit {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to open store '%s' (payload=%p, cache=%v, key='%s', cachedVal=%s): %w", storeName, p, hasCache, cacheKey, cachedType, err)
		}
		// Cache it only if we are in a long-running transaction (not auto-commit)
		if !autoCommit {
			if p.Variables == nil {
				p.Variables = make(map[string]any)
			}
			p.Variables[cacheKey] = store
			// fmt.Printf("DEBUG: Cached %s\n", cacheKey)
		}
	}

	if ok, err := store.First(ctx); ok && err == nil {
		count := 0
		for {
			k, err := store.GetCurrentKey()
			if err != nil {
				break
			}
			v, err := store.GetCurrentValue(ctx)
			if err != nil {
				break
			}
			resultItems = append(resultItems, map[string]any{"key": k, "value": v})
			count++
			if count >= int(limit) {
				break
			}
			if ok, _ := store.Next(ctx); !ok {
				break
			}
		}
	}

	if len(resultItems) == 0 {
		if autoCommit {
			tx.Commit(ctx)
		}
		return "No items found.", nil
	}

	// Always return JSON. The client/UI is responsible for formatting (e.g. to CSV).
	b, err := json.MarshalIndent(resultItems, "", "  ")
	if err != nil {
		if autoCommit {
			tx.Rollback(ctx)
		}
		return "", err
	}
	if autoCommit {
		if err := tx.Commit(ctx); err != nil {
			return "", fmt.Errorf("failed to commit transaction: %w", err)
		}
	}
	return string(b), nil
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
		if p.Transaction != nil {
			return "Transaction already active", nil
		}
		if db == nil {
			return "", fmt.Errorf("no database selected")
		}
		tx, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return "", fmt.Errorf("failed to begin transaction: %w", err)
		}
		p.Transaction = tx
		return "Transaction started", nil

	case "commit":
		if p.Transaction == nil {
			return "No active transaction to commit", nil
		}
		if tx, ok := p.Transaction.(sop.Transaction); ok {
			commitErr := tx.Commit(ctx)
			p.Transaction = nil

			// Clear cached variables (stores) as they are bound to the transaction
			p.Variables = nil

			if db != nil {
				newTx, beginErr := db.BeginTransaction(ctx, sop.ForWriting)
				if beginErr != nil {
					if commitErr != nil {
						return "", fmt.Errorf("commit failed: %v. AND failed to auto-start new one: %v", commitErr, beginErr)
					}
					return "Transaction committed, but failed to auto-start new one: " + beginErr.Error(), nil
				}
				p.Transaction = newTx

				if recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
					// We don't need to record the auto-start of the new transaction here,
					// because the user's intent was just to commit.
					// The system's auto-restart is an implementation detail for the session.
					_ = recorder
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
		}
		return "", fmt.Errorf("invalid transaction object")

	case "rollback":
		if p.Transaction == nil {
			return "No active transaction to rollback", nil
		}
		if tx, ok := p.Transaction.(sop.Transaction); ok {
			if err := tx.Rollback(ctx); err != nil {
				return "", fmt.Errorf("rollback failed: %w", err)
			}
			p.Transaction = nil
			// Clear cached variables (stores) as they are bound to the transaction
			p.Variables = nil

			if db != nil {
				newTx, err := db.BeginTransaction(ctx, sop.ForWriting)
				if err != nil {
					return "Transaction rolled back, but failed to auto-start new one: " + err.Error(), nil
				}
				p.Transaction = newTx

				if recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
					// We don't need to record the auto-start of the new transaction here.
					_ = recorder
				}
				return "Transaction rolled back (and new one started)", nil
			}
			return "Transaction rolled back", nil
		}
		return "", fmt.Errorf("invalid transaction object")

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
	if p.Transaction != nil {
		if t, ok := p.Transaction.(sop.Transaction); ok {
			tx = t
		}
	}

	localTx := false
	if tx == nil {
		if db != nil {
			var err error
			tx, err = db.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return "", fmt.Errorf("failed to begin transaction: %w", err)
			}
			localTx = true
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
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
	var err error

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

	found, err := store.Remove(ctx, key)
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
	if p.Transaction != nil {
		if t, ok := p.Transaction.(sop.Transaction); ok {
			tx = t
		}
	}

	localTx := false
	if tx == nil {
		if db != nil {
			var err error
			tx, err = db.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return "", fmt.Errorf("failed to begin transaction: %w", err)
			}
			localTx = true
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
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
	var err error

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

	ok, err := store.Add(ctx, key, value)
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
	if p.Transaction != nil {
		if t, ok := p.Transaction.(sop.Transaction); ok {
			tx = t
		}
	}

	localTx := false
	if tx == nil {
		if db != nil {
			var err error
			tx, err = db.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return "", fmt.Errorf("failed to begin transaction: %w", err)
			}
			localTx = true
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
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
	var err error

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

	ok, err := store.Update(ctx, key, value)
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

	names, err := store.List(ctx, "macros")
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
	if err := store.Load(ctx, "macros", name, &macro); err != nil {
		return "", fmt.Errorf("failed to load macro '%s': %w", name, err)
	}

	// Format details
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Macro: %s\n", macro.Name))
	if macro.Category != "" {
		sb.WriteString(fmt.Sprintf("Category: %s\n", macro.Category))
	}
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

func (a *DataAdminAgent) runNavigation(ctx context.Context, args map[string]any, op func(context.Context, jsondb.StoreAccessor) (bool, error)) (string, error) {
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

	var tx sop.Transaction
	if p.Transaction != nil {
		if t, ok := p.Transaction.(sop.Transaction); ok {
			tx = t
		}
	}

	// Navigation requires a transaction (read-only is fine)
	if tx == nil {
		if db != nil {
			var err error
			tx, err = db.BeginTransaction(ctx, sop.ForReading)
			if err != nil {
				return "", fmt.Errorf("failed to begin transaction: %w", err)
			}
			// Auto-start transaction and persist it in session for stateful navigation
			p.Transaction = tx
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
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
		var err error
		store, err = jsondb.OpenStore(ctx, db.Config(), storeName, tx)
		if err != nil {
			return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
		}
		if p.Variables == nil {
			p.Variables = make(map[string]any)
		}
		p.Variables[cacheKey] = store
	}

	found, err := op(ctx, store)
	if err != nil {
		return "", fmt.Errorf("navigation failed: %w", err)
	}

	if !found {
		return "No item found", nil
	}

	k, _ := store.GetCurrentKey()
	v, _ := store.GetCurrentValue(ctx)

	// Return JSON representation
	b, _ := json.Marshal(map[string]any{"key": k, "value": v})
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
	if err := store.Load(ctx, "macros", name, &macro); err != nil {
		return fmt.Errorf("failed to load macro '%s': %w", name, err)
	}

	if err := updateFunc(&macro); err != nil {
		return err
	}

	if err := store.Save(ctx, "macros", name, &macro); err != nil {
		return fmt.Errorf("failed to save macro '%s': %w", name, err)
	}

	return tx.Commit(ctx)
}
