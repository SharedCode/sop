package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/jsondb"
)

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
	} else {
		fmt.Printf("DEBUG: toolAdd finishing. localTx=false. HasBegun=%v\n", tx.HasBegun())
	}

	return fmt.Sprintf("Item updated in store '%s'", storeName), nil
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
			p.ExplicitTransaction = true
			return "Transaction already active (promoted to explicit)", nil
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
