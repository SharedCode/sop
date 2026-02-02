package agent

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/jsondb"
)

// toolAtomicOpWrapper creates a tool function that executes a single atomic operation
// using the shared session ScriptEngine.
func (a *DataAdminAgent) toolAtomicOpWrapper(op string) func(context.Context, map[string]any) (string, error) {
	return func(ctx context.Context, args map[string]any) (string, error) {
		// Ensure session context exists (Scoped to Request via generic helper)
		scriptCtx := getOrInitScriptContext(ctx)

		// Initialize Engine reusing the session context
		resolver := func(name string) (Database, error) {
			if name == "" || name == "@db" || name == "current" {
				if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
					name = p.CurrentDB
				}
			}
			return a.resolveDatabase(name)
		}

		engine := NewScriptEngine(scriptCtx, resolver)
		engine.StoreOpener = a.StoreOpener
		// Function handler not strictly needed for single atomic ops unless they call scripts
		engine.FunctionHandler = func(c context.Context, name string, args map[string]any) (any, error) {
			return a.opCallScript(c, engine.Context, map[string]any{"name": name, "params": args})
		}

		// Get the operation function
		opFunc, err := bindOperation(op)
		if err != nil {
			return "", fmt.Errorf("unknown operation '%s': %v", op, err)
		}

		// Use LastResult from agent state as input (Implicit Piping)
		input := getLastAtomicResult(ctx)
		engine.LastResult = input

		// Execute
		result, err := opFunc(ctx, engine, args, input)
		if err != nil {
			return "", fmt.Errorf("operation '%s' failed: %v", op, err)
		}

		// Update LastResult in agent state
		setLastAtomicResult(ctx, result)
		engine.LastResult = result

		// Handle result_var if present in args
		resVar, _ := args["result_var"].(string)

		// Convention: Set Default Variable Names if not specified
		if resVar == "" {
			if op == "begin_tx" {
				resVar = "tx"
			} else if op == "open_store" {
				if name, ok := args["name"].(string); ok && name != "" {
					resVar = name
				}
			} else if op == "open_db" {
				if name, ok := args["name"].(string); ok && name != "" {
					resVar = name
				}
			}
		}

		if resVar != "" {
			if engine.Context.Variables == nil {
				engine.Context.Variables = make(map[string]any)
			}
			engine.Context.Variables[resVar] = result

			// Register specific types
			if db, ok := result.(Database); ok {
				if engine.Context.Databases == nil {
					engine.Context.Databases = make(map[string]Database)
				}
				engine.Context.Databases[resVar] = db
			}
			if tx, ok := result.(sop.Transaction); ok {
				if engine.Context.Transactions == nil {
					engine.Context.Transactions = make(map[string]sop.Transaction)
				}
				engine.Context.Transactions[resVar] = tx
			}
			if store, ok := result.(jsondb.StoreAccessor); ok {
				if engine.Context.Stores == nil {
					engine.Context.Stores = make(map[string]jsondb.StoreAccessor)
				}
				engine.Context.Stores[resVar] = store
			}
		}

		return serializeResult(ctx, result)
	}
}

// Helper to register all atomic tools
func (a *DataAdminAgent) registerAtomicTools() {
	ops := []string{
		"open_db", "begin_tx", "commit_tx", "rollback_tx",
		"open_store", "scan", "filter", "sort", "project", "limit",
		"join_right", "assign", "return", // Removed colliding ops: "join", "add", "update", "delete" to preserve Legacy Tool precedence
		"inspect", "if", "loop", "call_function",
		"list_new", "list_append", "map_merge",
		"first", "last", "next", "previous", "find",
		"get_current_key", "get_current_value",
	}

	for _, op := range ops {
		// Use a closure to capture 'op'
		opName := op
		a.registry.Register(opName, fmt.Sprintf("Atomic operation: %s", opName), "(args: object)", a.toolAtomicOpWrapper(opName))
	}
}
