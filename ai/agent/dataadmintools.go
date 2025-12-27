package agent

import (
	"bytes"
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
	a.registry.Register("select", "Retrieve data from a store. Supports filtering by key subset. You can optionally specify a list of fields to return.", "(database: string, store: string, limit: number, scan_limit: number, key_match: any, fields: []string)", a.toolSelect)
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
	scanLimit, _ := args["scan_limit"].(float64)
	if scanLimit == 0 {
		scanLimit = 1000
	}
	// Ensure we can at least scan enough to find 'limit' items if they are contiguous
	if scanLimit < limit {
		scanLimit = limit
	}

	keyMatch, hasKeyMatch := args["key_match"]

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
	var autoCommit bool

	tx, autoCommit, err := a.resolveTransaction(ctx, db, dbName, sop.ForReading)
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

	// Check for ResultStreamer
	var streamer ai.ResultStreamer
	if s, ok := ctx.Value(ai.CtxKeyResultStreamer).(ai.ResultStreamer); ok {
		streamer = s
	}

	var ok bool
	ok, err = store.First(ctx)

	var sb strings.Builder
	if streamer == nil {
		sb.WriteString("[\n")
	} else {
		streamer.BeginArray()
	}
	itemsFound := false

	if ok && err == nil {
		count := 0
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
			if hasKeyMatch {
				if !matchesKey(k, keyMatch) {
					if ok, _ := store.Next(ctx); !ok {
						break
					}
					continue
				}
			}

			v, err := store.GetCurrentValue(ctx)
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
		p.ExplicitTransaction = true
		return "Transaction started", nil

	case "commit":
		if p.Transaction == nil {
			return "No active transaction to commit", nil
		}
		if tx, ok := p.Transaction.(sop.Transaction); ok {
			commitErr := tx.Commit(ctx)
			p.Transaction = nil
			p.ExplicitTransaction = false

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
				var filteredNeighbors []map[string]any
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
func matchesKey(itemKey, filterKey any) bool {
	// If both are maps
	if mItem, ok := itemKey.(map[string]any); ok {
		if mFilter, ok := filterKey.(map[string]any); ok {
			for k, v := range mFilter {
				// Simple equality check. For nested objects, this might need recursion.
				// But for now, we assume flat keys or strict equality on values.
				if itemVal, exists := mItem[k]; !exists || itemVal != v {
					return false
				}
			}
			return true
		}
	}
	// If primitives, strict equality
	return itemKey == filterKey
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

func filterFields(item map[string]any, fields []string) map[string]any {
	if len(fields) == 0 {
		return item
	}

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
			filtered := make(map[string]any)
			for k, v := range keyMap {
				if isRequested(k) {
					filtered[k] = v
				}
			}
			if len(filtered) > 0 {
				newKey = filtered
			}
		} else if orderedKey, ok := originalKey.(OrderedKey); ok {
			keyMap := orderedKey.m
			filtered := make(map[string]any)
			for k, v := range keyMap {
				if isRequested(k) {
					filtered[k] = v
				}
			}
			if len(filtered) > 0 {
				newKey = filtered
			}
		}
	}

	// 2. Handle Value
	originalValue := item["value"]
	if isRequested("value") || isRequested("Value") {
		newValue = originalValue
	} else {
		if valMap, ok := originalValue.(map[string]any); ok {
			filtered := make(map[string]any)
			for k, v := range valMap {
				if isRequested(k) {
					filtered[k] = v
				}
			}
			if len(filtered) > 0 {
				newValue = filtered
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

	if p != nil && p.Transaction != nil {
		// Only use session transaction if it matches the target database
		// Note: dbName is the resolved database name for the operation.
		// p.CurrentDB is the database the session transaction is bound to.
		if dbName == "" || dbName == p.CurrentDB {
			if t, ok := p.Transaction.(sop.Transaction); ok {
				tx = t
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
