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
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/jsondb"
)

// registerTools registers all available tools for the DataAdminAgent.
func (a *DataAdminAgent) registerTools() {
	if a.registry == nil {
		a.registry = NewRegistry()
	}

	a.registry.Register("list_databases", "Lists all available databases.", "()", a.toolListDatabases)
	a.registry.Register("list_stores", "Lists all stores in the current or specified database.", "(database: string)", a.toolListStores)
	a.registry.Register("select", "Retrieve data from a store.", "(database: string, store: string, limit: number, format: string)", a.toolSelect)
	a.registry.Register("manage_transaction", "Manage database transactions (begin, commit, rollback).", "(action: string)", a.toolManageTransaction)
	a.registry.Register("delete", "Delete an item from a store.", "(store: string, key: any)", a.toolDelete)
}

func (a *DataAdminAgent) toolListDatabases(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}
	var names []string
	for k := range p.Databases {
		names = append(names, k)
	}
	return fmt.Sprintf("Databases: %v", names), nil
}

func (a *DataAdminAgent) toolListStores(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName != "" {
		if val, ok := p.Databases[dbName]; ok {
			if d, ok := val.(*database.Database); ok {
				db = d
			}
		}
	} else {
		if d, ok := p.CurrentDB.(*database.Database); ok {
			db = d
		}
	}

	if db == nil {
		return "", fmt.Errorf("database not found or not selected")
	}

	// Need transaction
	var tx sop.Transaction
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
			defer tx.Rollback(ctx)
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
	}
	return "Stores: [Not Implemented in MVP]", nil
}

func (a *DataAdminAgent) toolSelect(ctx context.Context, args map[string]any) (string, error) {
	p := ai.GetSessionPayload(ctx)
	if p == nil {
		return "", fmt.Errorf("no session payload found")
	}

	// Resolve Database
	var db *database.Database
	dbName, _ := args["database"].(string)
	if dbName != "" {
		if val, ok := p.Databases[dbName]; ok {
			if d, ok := val.(*database.Database); ok {
				db = d
			}
		}
	} else {
		if d, ok := p.CurrentDB.(*database.Database); ok {
			db = d
		}
	}

	if db == nil {
		return "", fmt.Errorf("database not found or not selected")
	}

	storeName, _ := args["store"].(string)
	limit, _ := args["limit"].(float64)
	if limit == 0 {
		limit = 10
	}

	var tx sop.Transaction
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
			defer tx.Rollback(ctx)
		} else {
			return "", fmt.Errorf("no active transaction and no database to start one")
		}
	}

	// Determine if store uses complex keys
	var isPrimitiveKey bool
	var indexSpec *jsondb.IndexSpecification

	if t2, ok := tx.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, storeName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
			if !isPrimitiveKey && stores[0].MapKeyIndexSpecification != "" {
				var is jsondb.IndexSpecification
				if err := encoding.DefaultMarshaler.Unmarshal([]byte(stores[0].MapKeyIndexSpecification), &is); err == nil {
					indexSpec = &is
				}
			}
		}
	}

	var resultItems []map[string]any

	var store jsondb.StoreAccessor
	// Normalize store name for caching
	dbPrefix := ""
	if dbName != "" {
		dbPrefix = dbName + "_"
	}
	cacheKey := fmt.Sprintf("_opened_store_%s%s", dbPrefix, strings.ToLower(storeName))
	if cached, ok := p.Variables[cacheKey]; ok {
		if s, ok := cached.(jsondb.StoreAccessor); ok {
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
		return "No items found.", nil
	}

	// Check for requested format
	format, _ := args["format"].(string)
	if strings.ToLower(format) == "json" {
		b, err := json.MarshalIndent(resultItems, "", "  ")
		if err != nil {
			return "", err
		}
		return string(b), nil
	}

	// CSV Formatting Logic
	keyFieldSet := make(map[string]bool)
	var keyIsMap bool
	valueFieldSet := make(map[string]bool)
	var valueIsMap bool

	for _, item := range resultItems {
		if kMap, ok := item["key"].(map[string]any); ok {
			keyIsMap = true
			for k := range kMap {
				keyFieldSet[k] = true
			}
		}
		if vMap, ok := item["value"].(map[string]any); ok {
			valueIsMap = true
			for k := range vMap {
				valueFieldSet[k] = true
			}
		}
	}

	var kFields []string
	if keyIsMap {
		if indexSpec != nil {
			for _, f := range indexSpec.IndexFields {
				kFields = append(kFields, f.FieldName)
			}
			var extra []string
			for k := range keyFieldSet {
				found := false
				for _, f := range kFields {
					if f == k {
						found = true
						break
					}
				}
				if !found {
					extra = append(extra, k)
				}
			}
			sort.Strings(extra)
			kFields = append(kFields, extra...)
		} else {
			for k := range keyFieldSet {
				kFields = append(kFields, k)
			}
			sort.Strings(kFields)
		}
	}

	var vFields []string
	if valueIsMap {
		for k := range valueFieldSet {
			vFields = append(vFields, k)
		}
		sort.Strings(vFields)
	}

	var csvHeaders []string
	if keyIsMap {
		for _, k := range kFields {
			if len(k) > 0 {
				csvHeaders = append(csvHeaders, strings.ToUpper(k[:1])+k[1:])
			} else {
				csvHeaders = append(csvHeaders, k)
			}
		}
	} else {
		csvHeaders = append(csvHeaders, "Key")
	}
	if valueIsMap {
		for _, v := range vFields {
			if len(v) > 0 {
				csvHeaders = append(csvHeaders, strings.ToUpper(v[:1])+v[1:])
			} else {
				csvHeaders = append(csvHeaders, v)
			}
		}
	}
	var sb strings.Builder
	sb.WriteString(strings.Join(csvHeaders, ", "))
	sb.WriteString("\n")

	for _, item := range resultItems {
		var row []string
		if keyIsMap {
			kMap, _ := item["key"].(map[string]any)
			for _, f := range kFields {
				if v, ok := kMap[f]; ok {
					row = append(row, fmt.Sprintf("%v", v))
				} else {
					row = append(row, "")
				}
			}
		} else {
			row = append(row, fmt.Sprintf("%v", item["key"]))
		}
		if valueIsMap {
			vMap, _ := item["value"].(map[string]any)
			for _, f := range vFields {
				if v, ok := vMap[f]; ok {
					row = append(row, fmt.Sprintf("%v", v))
				} else {
					row = append(row, "")
				}
			}
		} else {
			row = append(row, fmt.Sprintf("%v", item["value"]))
		}
		sb.WriteString(strings.Join(row, ", "))
		sb.WriteString("\n")
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
	if d, ok := p.CurrentDB.(*database.Database); ok {
		db = d
	}

	action, _ := args["action"].(string)
	if action == "" {
		return "", fmt.Errorf("action is required")
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
					recorder.RecordStep(ai.MacroStep{
						Type:    "command",
						Command: "manage_transaction",
						Args:    map[string]any{"action": "begin"},
					})
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

			if db != nil {
				newTx, err := db.BeginTransaction(ctx, sop.ForWriting)
				if err != nil {
					return "Transaction rolled back, but failed to auto-start new one: " + err.Error(), nil
				}
				p.Transaction = newTx

				if recorder, ok := ctx.Value(ai.CtxKeyMacroRecorder).(ai.MacroRecorder); ok {
					recorder.RecordStep(ai.MacroStep{
						Type:    "command",
						Command: "manage_transaction",
						Args:    map[string]any{"action": "begin"},
					})
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
	if d, ok := p.CurrentDB.(*database.Database); ok {
		db = d
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

	var found bool
	if !isPrimitiveKey {
		store, err := jsondb.OpenJsonBtreeMapKey(ctx, db.Config(), storeName, tx)
		if err != nil {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
		}

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

		keyMap, ok := key.(map[string]any)
		if !ok {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("key must be a map or JSON string for complex key store")
		}

		found, err = store.Remove(ctx, []map[string]any{keyMap})
		if err != nil {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to delete item '%v': %w", key, err)
		}
	} else {
		store, err := db.OpenBtree(ctx, storeName, tx)
		if err != nil {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to open store '%s': %w", storeName, err)
		}

		keyStr, ok := key.(string)
		if !ok {
			keyStr = fmt.Sprintf("%v", key)
		}

		found, err = store.Remove(ctx, keyStr)
		if err != nil {
			if localTx {
				tx.Rollback(ctx)
			}
			return "", fmt.Errorf("failed to delete item '%v': %w", key, err)
		}
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
