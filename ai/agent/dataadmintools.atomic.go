package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	log "log/slog"
	"sort"
	"strings"
	"time"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/jsondb"
)

// Database interface for script execution
type Database interface {
	BeginTransaction(ctx context.Context, mode sop.TransactionMode, maxTime ...time.Duration) (sop.Transaction, error)
	Config() sop.DatabaseOptions
}

// ScriptInstruction represents a single operation in the script.
type ScriptInstruction struct {
	Op        string         `json:"op"`         // Operation name
	Args      map[string]any `json:"args"`       // Arguments
	InputVar  string         `json:"input_var"`  // Variable to use as input (optional)
	ResultVar string         `json:"result_var"` // Variable to store result (optional)
}

// ScriptContext holds the state of the script execution.
type ScriptContext struct {
	Variables      map[string]any
	Transactions   map[string]sop.Transaction
	Stores         map[string]jsondb.StoreAccessor
	Databases      map[string]Database
	LastUpdatedVar string // helper to track the prioritization of variable draining
}

// ScriptCursor represents a streaming iterator for script operations.
type ScriptCursor interface {
	Next(ctx context.Context) (any, bool, error)
	Close() error
}

// OrderedFieldsProvider allows cursors to expose the list of fields in order.
type OrderedFieldsProvider interface {
	GetOrderedFields() []string
}

// SpecProvider allows cursors to expose IndexSpecifications for field ordering.
type SpecProvider interface {
	GetIndexSpecs() map[string]*jsondb.IndexSpecification
}

// StoreCursor wraps a StoreAccessor to provide a ScriptCursor.
type StoreCursor struct {
	store     jsondb.StoreAccessor
	indexSpec *jsondb.IndexSpecification
	ctx       context.Context
	limit     int
	count     int
	filter    map[string]any
	engine    *ScriptEngine
	isDesc    bool
	prefix    any
	started   bool
	closed    bool
}

func (sc *StoreCursor) GetIndexSpecs() map[string]*jsondb.IndexSpecification {
	if sc.indexSpec != nil {
		return map[string]*jsondb.IndexSpecification{
			"key": sc.indexSpec,
		}
	}
	return nil
}

func (sc *StoreCursor) Next(ctx context.Context) (any, bool, error) {
	if sc.closed {
		return nil, false, nil
	}
	if sc.limit > 0 && sc.count >= sc.limit {
		return nil, false, nil
	}

	var ok bool
	var err error

	// Initial positioning is done in Scan, but we need to advance
	if !sc.started {
		// Scan already positioned the cursor at the first element
		sc.started = true
		// Check if current position is valid
		k, _ := sc.store.GetCurrentKey()
		if k == nil {
			return nil, false, nil
		}
		ok = true
	} else {
		if sc.isDesc {
			ok, err = sc.store.Previous(ctx)
		} else {
			ok, err = sc.store.Next(ctx)
		}
		if err != nil {
			return nil, false, err
		}
	}

	for ok {
		k, _ := sc.store.GetCurrentKey()
		v, _ := sc.store.GetCurrentValue(ctx)

		// Prefix Check
		if sc.prefix != nil {
			if kStr, isStr := k.(string); isStr {
				pStr := fmt.Sprintf("%v", sc.prefix)
				if !strings.HasPrefix(kStr, pStr) {
					return nil, false, nil // End of prefix range
				}
			}
		}

		// Filter Check
		item := renderItem(k, v, nil)

		if sc.filter != nil {
			match, err := sc.engine.evaluateCondition(item, sc.filter)
			if err != nil {
				return nil, false, err
			}
			if !match {
				if sc.isDesc {
					ok, err = sc.store.Previous(ctx)
				} else {
					ok, err = sc.store.Next(ctx)
				}
				continue
			}
		}

		sc.count++
		return item, true, nil
	}

	return nil, false, nil
}

func (sc *StoreCursor) Close() error {
	sc.closed = true
	return nil
}

// Join Strategy Constants
const (
	StrategyUnset     = 0
	StrategyIndexSeek = 1
	StrategyInMemory  = 2
	StrategyFullScan  = 3
)

type JoinPlan struct {
	Strategy     int
	IndexFields  []string // Ordered list of fields in the Index
	PrefixFields []string // Fields from ON clause that match the Index Prefix
	IsComposite  bool     // True if the Store uses a Map Key (Composite)
	Ascending    bool     // True if the first prefix field is Ascending
}

// JoinRightCursor performs a streaming join with probing and scanning support.
// It replaces both JoinCursor (Lookup) and NestedLoopJoinCursor (Scan).
type JoinRightCursor struct {
	left      ScriptCursor
	right     jsondb.StoreAccessor
	joinType  string
	on        map[string]any
	ctx       context.Context
	engine    *ScriptEngine
	currentL  any
	matched   bool
	rightIter bool

	// Execution Plan
	plan      JoinPlan
	planReady bool

	// Legacy / Runtime State
	useFallback  bool  // optimization: materialization fallback
	fallbackList []any // fallback: in-memory list
	fallbackIdx  int
	closed       bool
}

func (jc *JoinRightCursor) Next(ctx context.Context) (any, bool, error) {
	if jc.closed {
		return nil, false, nil
	}
	val, ok, err := jc.NextOptimized(ctx)
	if ok && err == nil {
		// b, _ := json.Marshal(val)

		// Attempt to inspect if the result is an OrderedMap
		var fields []string
		if om, isOm := val.(*OrderedMap); isOm {
			fields = om.keys
		} else if om, isOm := val.(OrderedMap); isOm {
			fields = om.keys
		} else if m, isM := val.(map[string]any); isM {
			// standard map, extract keys but they won't be ordered
			for k := range m {
				fields = append(fields, k)
			}
			sort.Strings(fields)
		}

		// log.Debug("payload contents:",
		// 	"Function", "JoinRightCursor.Next",
		// 	"json", string(b),
		// 	"fields_found_in_result", fields,
		// )
	}
	return val, ok, err
}

func (jc *JoinRightCursor) Close() error {
	jc.closed = true
	return jc.left.Close()
}

// FilterCursor filters a stream.
type FilterCursor struct {
	source ScriptCursor
	filter map[string]any
	engine *ScriptEngine
	closed bool
}

func (fc *FilterCursor) Next(ctx context.Context) (any, bool, error) {
	if fc.closed {
		return nil, false, nil
	}
	for {
		item, ok, err := fc.source.Next(ctx)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		match, err := fc.engine.evaluateCondition(item, fc.filter)
		if err != nil {
			return nil, false, err
		}
		if match {
			return item, true, nil
		}
	}
}

func (fc *FilterCursor) Close() error {
	fc.closed = true
	return fc.source.Close()
}

func (fc *FilterCursor) GetOrderedFields() []string {
	if provider, ok := fc.source.(OrderedFieldsProvider); ok {
		return provider.GetOrderedFields()
	}
	return nil
}

// ProjectCursor projects fields from a stream.
type ProjectCursor struct {
	source ScriptCursor
	fields []string
	closed bool
}

func (pc *ProjectCursor) Next(ctx context.Context) (any, bool, error) {
	if pc.closed {
		return nil, false, nil
	}
	item, ok, err := pc.source.Next(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	// Log input to projection
	// if inBytes, err := json.Marshal(item); err == nil {
	// 	log.Debug("payload contents:", "Function", "ProjectCursor.Next (Input)", "json", string(inBytes))
	// } else {
	// 	log.Debug("payload contents:", "Function", "ProjectCursor.Next (Input)", "error", "failed to marshal input", "item", item)
	// }

	resultMap := &OrderedMap{m: make(map[string]any), keys: make([]string, 0)}
	for _, f := range pc.fields {
		val := getField(item, f)
		resultMap.m[f] = val
		resultMap.keys = append(resultMap.keys, f)
	}

	// Logging order preservation in ProjectCursor
	// b, _ := json.Marshal(resultMap)
	// log.Debug("payload contents:", "Function", "ProjectCursor.Next (Output)", "json", string(b))

	return resultMap, true, nil
}

func (pc *ProjectCursor) Close() error {
	pc.closed = true
	return pc.source.Close()
}

func (pc *ProjectCursor) GetOrderedFields() []string {
	return pc.fields
}

// LimitCursor limits a stream.
type LimitCursor struct {
	source ScriptCursor
	limit  int
	count  int
	closed bool
}

func (lc *LimitCursor) GetIndexSpecs() map[string]*jsondb.IndexSpecification {
	if provider, ok := lc.source.(SpecProvider); ok {
		return provider.GetIndexSpecs()
	}
	return nil
}

func (lc *LimitCursor) Next(ctx context.Context) (any, bool, error) {
	if lc.closed {
		return nil, false, nil
	}
	if lc.count >= lc.limit {
		return nil, false, nil
	}
	item, ok, err := lc.source.Next(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	lc.count++
	return item, true, nil
}

func (lc *LimitCursor) Close() error {
	lc.closed = true
	return lc.source.Close()
}

func (lc *LimitCursor) GetOrderedFields() []string {
	if provider, ok := lc.source.(OrderedFieldsProvider); ok {
		return provider.GetOrderedFields()
	}
	return nil
}

func NewScriptContext() *ScriptContext {
	return &ScriptContext{
		Variables:    make(map[string]any),
		Transactions: make(map[string]sop.Transaction),
		Stores:       make(map[string]jsondb.StoreAccessor),
		Databases:    make(map[string]Database),
	}
}

// ScriptEngine executes scripts.
type ScriptEngine struct {
	Context         *ScriptContext
	ResolveDatabase func(name string) (Database, error)
	FunctionHandler func(ctx context.Context, name string, args map[string]any) (any, error)
	LastResult      any
}

func NewScriptEngine(ctx *ScriptContext, dbResolver func(string) (Database, error)) *ScriptEngine {
	if ctx == nil {
		ctx = NewScriptContext()
	}
	return &ScriptEngine{
		Context:         ctx,
		ResolveDatabase: dbResolver,
	}
}

func (a *DataAdminAgent) resolveDatabase(name string) (Database, error) {
	if name == "system" && a.systemDB != nil {
		return a.systemDB, nil
	}
	if opts, ok := a.databases[name]; ok {
		return database.NewDatabase(opts), nil
	}
	return nil, fmt.Errorf("database '%s' not found", name)
}

// toolExecuteScript executes a sequence of instructions.
// Args:
// - script: []ScriptInstruction (JSON array)
func (a *DataAdminAgent) toolExecuteScript(ctx context.Context, args map[string]any) (string, error) {
	log.Debug("toolExecuteScript: Called", "args", args)
	scriptRaw, ok := args["script"]
	if !ok {
		return "", fmt.Errorf("script argument is required")
	}

	// Parse Script first to ensure it's valid and normalized
	var script []ScriptInstruction
	if pStr, ok := scriptRaw.(string); ok {
		if err := json.Unmarshal([]byte(pStr), &script); err != nil {
			return "", fmt.Errorf("failed to parse script JSON: %v", err)
		}
	} else if pSlice, ok := scriptRaw.([]any); ok {
		bytes, _ := json.Marshal(pSlice)
		if err := json.Unmarshal(bytes, &script); err != nil {
			return "", fmt.Errorf("failed to parse script array: %v", err)
		}
	} else {
		return "", fmt.Errorf("script must be a JSON string or array")
	}

	// Stub Mode Check
	if a.Config.StubMode {
		// Log the NORMALIZED script for debugging
		bytes, err := json.MarshalIndent(script, "", "  ")
		if err != nil {
			return "", fmt.Errorf("failed to marshal script for display: %v", err)
		}

		log.Debug("toolExecuteScript called in STUB MODE", "script", string(bytes))
		return fmt.Sprintf("Script prepared successfully (STUBBED). execution skipped.\nGenerated Script:\n```json\n%s\n```", string(bytes)), nil
	}

	// Initialize Engine
	var scriptCtx *ScriptContext
	if a.sessionContext != nil {
		scriptCtx = a.sessionContext
	} else {
		scriptCtx = NewScriptContext()
	}

	// Wrap resolver to handle default database from context
	resolver := func(name string) (Database, error) {
		if name == "" || name == "@db" || name == "current" {
			if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
				name = p.CurrentDB
			}
		}
		return a.resolveDatabase(name)
	}

	engine := NewScriptEngine(scriptCtx, resolver)
	engine.FunctionHandler = func(c context.Context, name string, args map[string]any) (any, error) {
		// Bridge to Agent's script execution
		// We need to implement this properly, reusing opCallScript logic or similar
		// For now, we can call a method on Agent if we expose it, or inline the logic.
		// Since opCallScript is being moved to Engine, we can just call Engine.CallFunction?
		// But Engine.CallFunction needs access to SystemDB.
		// So we should pass SystemDB to Engine or handle it here.
		return a.opCallScript(c, engine.Context, map[string]any{"name": name, "params": args})
	}

	// Execute
	compiled, err := engine.Compile(script)
	if err != nil {
		return "", fmt.Errorf("compilation failed: %v", err)
	}
	if err := compiled(ctx, engine); err != nil {
		return "", err
	}

	// Return Result
	// Priority 0: Explicit 'return' instruction
	// This should be handled during execution loop if we supported early exit.
	// Since we execute the whole list, we check if the last instruction was a return or provided a return value?
	// Actually, the engine.Execute loop doesn't handle 'return' control flow yet.
	// But assuming we add a 'return' OP, we would look for that.

	// Priority 1: Check for 'output' variable (Explicit override)
	if val, ok := scriptCtx.Variables["output"]; ok {
		return serializeResult(val)
	}

	// Priority 2: Check for 'final_result' variable (Deprecated: Standard convention for query chains)
	if val, ok := scriptCtx.Variables["final_result"]; ok {
		return serializeResult(val)
	}

	// Priority 3: Check the last instruction's result variable (Fallback for simple scripts)
	if len(script) > 0 {
		lastInstr := script[len(script)-1]

		// If last instruction is specifically a "return" or "output" op (hypothetically)
		if lastInstr.Op == "return" {
			// If arguments has "value", return it
			if val, ok := lastInstr.Args["value"]; ok {
				// We need to resolve the value if it's a variable reference
				if sVal, ok := val.(string); ok && strings.HasPrefix(sVal, "@") {
					varName := strings.TrimPrefix(sVal, "@") // Basic variable resolution
					// Handle @variable syntax slightly better if needed, but Engine does it.
					// However, at this point, we are outside engine execution loop.
					// The engine should have surely resolved it if we had a return op.
					// But our engine is simple: it executes list of steps.

					// Let's rely on Context variables.
					if v, found := scriptCtx.Variables[varName]; found {
						return serializeResult(v)
					}
				}
				return serializeResult(val)
			}
		}

		if lastInstr.ResultVar != "" {
			if val, ok := scriptCtx.Variables[lastInstr.ResultVar]; ok {
				return serializeResult(val)
			}
		}
	}

	return "Script executed successfully.", nil
}

// CompiledScript is a function that executes the compiled script against an engine.
type CompiledScript func(ctx context.Context, e *ScriptEngine) error

type CachedScript struct {
	Script CompiledScript
	Hash   string
}

func (e *ScriptEngine) Execute(ctx context.Context, script []ScriptInstruction) error {
	compiled, err := CompileScript(script)
	if err != nil {
		return err
	}
	return compiled(ctx, e)
}

func (e *ScriptEngine) Compile(script []ScriptInstruction) (CompiledScript, error) {
	return CompileScript(script)
}

func CompileScript(script []ScriptInstruction) (CompiledScript, error) {
	var steps []func(context.Context, *ScriptEngine) error

	for i, instr := range script {
		// 1. Pre-process Arguments (Template Parsing)
		argResolvers := make(map[string]func(*ScriptEngine) any)
		for k, v := range instr.Args {
			val := v
			if str, ok := val.(string); ok && strings.Contains(str, "{{") {
				// Simple template parser: {{var.field}}
				argResolvers[k] = func(e *ScriptEngine) any {
					return e.resolveTemplate(str)
				}
			} else {
				argResolvers[k] = func(_ *ScriptEngine) any { return val }
			}
		}

		// 2. Bind Operation
		opFunc, err := bindOperation(instr.Op)
		if err != nil {
			return nil, fmt.Errorf("instruction %d: %v", i, err)
		}

		// 3. Create Step Closure
		step := func(ctx context.Context, e *ScriptEngine) error {
			// Resolve Args
			args := make(map[string]any)
			for k, resolver := range argResolvers {
				args[k] = resolver(e)
			}

			// Resolve Input Var
			var input any
			if instr.InputVar != "" {
				var ok bool
				inputVar := e.resolveVarName(instr.InputVar)
				input, ok = e.Context.Variables[inputVar]
				if !ok {
					return fmt.Errorf("input variable '%s' not found", instr.InputVar)
				}
			} else {
				// Implicit piping: Use result of last operation
				input = e.LastResult
			}

			// Execute Operation
			result, err := opFunc(ctx, e, args, input)
			if err != nil {
				return fmt.Errorf("operation '%s' failed: %v", instr.Op, err)
			}

			// Update LastResult for implicit piping
			e.LastResult = result

			// Store Result
			if instr.ResultVar != "" {
				e.Context.Variables[instr.ResultVar] = result
				e.Context.LastUpdatedVar = instr.ResultVar
				// Type Registration
				if db, ok := result.(Database); ok {
					e.Context.Databases[instr.ResultVar] = db
				}
				if tx, ok := result.(sop.Transaction); ok {
					e.Context.Transactions[instr.ResultVar] = tx
				}
				if store, ok := result.(jsondb.StoreAccessor); ok {
					e.Context.Stores[instr.ResultVar] = store
				}
			}
			return nil
		}
		steps = append(steps, step)
	}

	return func(ctx context.Context, e *ScriptEngine) error {
		for _, step := range steps {
			if err := step(ctx, e); err != nil {
				return err
			}
		}
		return nil
	}, nil
}

func (e *ScriptEngine) resolveTemplate(tmpl string) any {
	// Handle {{var.field}}
	// Remove {{ and }}
	trimmed := strings.TrimSuffix(strings.TrimPrefix(tmpl, "{{"), "}}")
	parts := strings.Split(trimmed, ".")

	var current any
	var ok bool

	// First part is variable name
	if len(parts) > 0 {
		current, ok = e.Context.Variables[parts[0]]
		if !ok {
			log.Debug("resolveTemplate var not found", "var", parts[0])
			return tmpl // Return original if var not found? Or nil?
		}
	}

	// Traverse fields
	for _, part := range parts[1:] {
		if currentMap, ok := current.(map[string]any); ok {
			current, ok = currentMap[part]
			if !ok {
				// Try "value" wrapper if not found directly?
				if valMap, ok := currentMap["value"].(map[string]any); ok {
					current, ok = valMap[part]
				}
			}
			if !ok {
				log.Debug("resolveTemplate field not found", "field", part, "map", currentMap)
				return nil // Field not found
			}
		} else {
			log.Debug("resolveTemplate current is not a map", "current", current)
			return nil // Not a map
		}
	}
	log.Debug("resolveTemplate result", "tmpl", tmpl, "current", current)
	return current
}

func bindOperation(op string) (func(context.Context, *ScriptEngine, map[string]any, any) (any, error), error) {
	switch op {
	case "open_db":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.OpenDB(args)
		}, nil
	case "begin_tx":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.BeginTx(ctx, args)
		}, nil
	case "commit_tx":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.CommitTx(ctx, args)
		}, nil
	case "rollback_tx":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.RollbackTx(ctx, args)
		}, nil
	case "open_store":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.OpenStore(ctx, args)
		}, nil
	case "scan":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Scan(ctx, args)
		}, nil
	case "filter":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Filter(ctx, input, args)
		}, nil
	case "sort":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Sort(ctx, input, args)
		}, nil
	case "project":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Project(ctx, input, args)
		}, nil
	case "limit":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Limit(ctx, input, args)
		}, nil
	case "join":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.Join(ctx, input, args)
		}, nil
	case "join_right":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			return e.JoinRight(ctx, input, args)
		}, nil
	case "update":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			if l, ok := input.([]map[string]any); ok {
				return e.Update(ctx, l, args)
			}
			return nil, fmt.Errorf("input must be list of maps")
		}, nil
	case "delete":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			if l, ok := input.([]map[string]any); ok {
				return e.Delete(ctx, l, args)
			}
			return nil, fmt.Errorf("input must be list of maps")
		}, nil
	case "inspect":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Inspect(ctx, args)
		}, nil
	case "assign":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, input any) (any, error) {
			if val, ok := args["value"]; ok {
				return val, nil
			}
			return input, nil
		}, nil
	case "if":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.If(ctx, args)
		}, nil
	case "loop":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.Loop(ctx, args)
		}, nil
	case "call_function":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.CallFunction(ctx, args)
		}, nil
	case "list_new":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return make([]any, 0), nil
		}, nil
	case "list_append":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return nil, e.ListAppend(ctx, args)
		}, nil
	case "map_merge":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.MapMerge(ctx, args)
		}, nil
	case "first":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.First(ctx, args)
		}, nil
	case "last":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Last(ctx, args)
		}, nil
	case "next":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Next(ctx, args)
		}, nil
	case "previous":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Previous(ctx, args)
		}, nil
	case "find":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			res, err := e.Find(ctx, args)
			log.Debug("Find result", "result", res, "err", err)
			return res, err
		}, nil
	case "add":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.Add(ctx, args)
		}, nil
	case "get_current_key":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			res, err := e.GetCurrentKey(ctx, args)
			log.Debug("GetCurrentKey result", "result", res, "err", err)
			return res, err
		}, nil
	case "get_current_value":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			return e.GetCurrentValue(ctx, args)
		}, nil
	case "return":
		return func(ctx context.Context, e *ScriptEngine, args map[string]any, _ any) (any, error) {
			if val, ok := args["value"]; ok {
				return val, nil
			}
			return nil, nil
		}, nil
	default:
		return nil, fmt.Errorf("unknown operation: %s", op)
	}
}

func serializeResult(val any) (string, error) {
	// log.Debug("serializeResult called", "type", fmt.Sprintf("%T", val), "val", val)

	if cursor, ok := val.(ScriptCursor); ok {
		var results []any
		defer cursor.Close()

		// Helper to wrap fields if spec available
		var specs map[string]*jsondb.IndexSpecification
		if provider, ok := cursor.(SpecProvider); ok {
			specs = provider.GetIndexSpecs()
		}

		// Helper to force field order if provider available
		var orderedFields []string
		if provider, ok := cursor.(OrderedFieldsProvider); ok {
			orderedFields = provider.GetOrderedFields()
		}

		/*
			log.Debug("serializeResult: Consuming Cursor",
				"cursorType", fmt.Sprintf("%T", cursor),
				"hasOrderedFields", len(orderedFields) > 0,
				"orderedFields", orderedFields)
		*/

		for {
			itemObj, ok, err := cursor.Next(context.Background())
			if err != nil {
				return "", fmt.Errorf("failed to read cursor: %v", err)
			}
			if !ok {
				break
			}

			var itemMap map[string]any
			if m, ok := itemObj.(map[string]any); ok {
				itemMap = m
			} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
				itemMap = om.m
			} else if om, ok := itemObj.(OrderedMap); ok {
				itemMap = om.m
			}

			// Apply ordering if specs available
			if itemMap != nil && len(specs) > 0 {
				for fieldName, spec := range specs {
					if val, ok := itemMap[fieldName]; ok {
						if m, ok := val.(map[string]any); ok {
							itemMap[fieldName] = OrderedKey{m: m, spec: spec}
						}
					}
				}
			}

			var item any = itemObj
			// Apply top-level field ordering
			if itemMap != nil && len(orderedFields) > 0 {
				// log.Debug("serializeResult calling filterFields", "orderedFields", orderedFields)
				item = filterFields(itemMap, orderedFields)
				/*
					if om, ok := item.(*OrderedMap); ok {
						log.Debug("serializeResult received OrderedMap", "keys", om.keys)
					} else {
						log.Debug("serializeResult received item", "type", fmt.Sprintf("%T", item))
					}
					// Debug log to trace what we actually got
					if om, ok := item.(OrderedMap); ok {
						b, _ := json.Marshal(om)
						log.Debug("payload contents:", "Function", "serializeResult", "keys", om.keys, "json", string(b))
					} else if om, ok := item.(*OrderedMap); ok {
						b, _ := json.Marshal(om)
						log.Debug("payload contents:", "Function", "serializeResult", "keys", om.keys, "json", string(b))
					} else {
						log.Debug("payload contents:", "Function", "serializeResult", "type", fmt.Sprintf("%T", item), "val", item)
					}
				*/
			}

			results = append(results, item)
		}

		val = results
	}

	bytes, err := json.MarshalIndent(val, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to serialize result: %v", err)
	}
	resStr := string(bytes)
	// log.Debug("payload contents:", "json", resStr[:min(len(resStr), 500)], "Function", "serializeResult")
	return resStr, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (e *ScriptEngine) CallScript(ctx context.Context, args map[string]any) (any, error) {
	name, _ := args["name"].(string)
	params, _ := args["params"].(map[string]any)

	if e.FunctionHandler != nil {
		return e.FunctionHandler(ctx, name, params)
	}
	return nil, fmt.Errorf("function handler not available")
}

// Deprecated: Use Compile/Execute instead
func (e *ScriptEngine) Dispatch(ctx context.Context, instr ScriptInstruction) error {
	var input any
	if instr.InputVar != "" {
		var ok bool
		input, ok = e.Context.Variables[instr.InputVar]
		if !ok {
			return fmt.Errorf("input variable '%s' not found", instr.InputVar)
		}
	}

	var result any
	var err error

	switch instr.Op {
	case "open_db":
		result, err = e.OpenDB(instr.Args)
	case "begin_tx":
		result, err = e.BeginTx(ctx, instr.Args)
	case "commit_tx":
		err = e.CommitTx(ctx, instr.Args)
	case "rollback_tx":
		err = e.RollbackTx(ctx, instr.Args)
	case "open_store":
		result, err = e.OpenStore(ctx, instr.Args)
	case "scan":
		result, err = e.Scan(ctx, instr.Args)
	case "filter":
		result, err = e.Filter(ctx, input, instr.Args)
	case "sort":
		result, err = e.Sort(ctx, input, instr.Args)
	case "project":
		result, err = e.Project(ctx, input, instr.Args)
	case "limit":
		result, err = e.Limit(ctx, input, instr.Args)
	case "join":
		result, err = e.Join(ctx, input, instr.Args)
	case "update":
		result, err = e.Update(ctx, input, instr.Args)
	case "delete":
		result, err = e.Delete(ctx, input, instr.Args)
	case "inspect":
		result, err = e.Inspect(ctx, instr.Args)
	case "assign":
		// Simple assignment or literal
		if val, ok := instr.Args["value"]; ok {
			result = val
		} else {
			result = input
		}
	case "if":
		err = e.If(ctx, instr.Args)
	case "loop":
		err = e.Loop(ctx, instr.Args)
	case "call_script":
		result, err = e.CallScript(ctx, instr.Args)
	// List / Map Operations (Memory)
	case "list_new":
		result = make([]any, 0)
	case "list_append":
		err = e.ListAppend(ctx, instr.Args)
	case "map_merge":
		result, err = e.MapMerge(ctx, instr.Args)
	// Cursor / Store Operations
	case "first":
		result, err = e.First(ctx, instr.Args)
	case "last":
		result, err = e.Last(ctx, instr.Args)
	case "next":
		result, err = e.Next(ctx, instr.Args)
	case "previous":
		result, err = e.Previous(ctx, instr.Args)
	case "find":
		result, err = e.Find(ctx, instr.Args)
		log.Debug("Find result", "result", result, "err", err)
	case "add":
		result, err = e.Add(ctx, instr.Args)
	case "get_current_key":
		result, err = e.GetCurrentKey(ctx, instr.Args)
		log.Debug("GetCurrentKey result", "result", result, "err", err)
	case "get_current_value":
		result, err = e.GetCurrentValue(ctx, instr.Args)
	default:
		return fmt.Errorf("unknown operation: %s", instr.Op)
	}

	if err != nil {
		return err
	}

	if instr.ResultVar != "" {
		e.Context.Variables[instr.ResultVar] = result
		// Also register specific types if needed
		if db, ok := result.(Database); ok {
			e.Context.Databases[instr.ResultVar] = db
		}
		if tx, ok := result.(sop.Transaction); ok {
			e.Context.Transactions[instr.ResultVar] = tx
		}
		if store, ok := result.(jsondb.StoreAccessor); ok {
			e.Context.Stores[instr.ResultVar] = store
		}
	}

	return nil
}

// --- Operations ---

// resolveVarName strips the optional '@' prefix from a variable name.
func (e *ScriptEngine) resolveVarName(name string) string {
	return strings.TrimPrefix(name, "@")
}

func (e *ScriptEngine) OpenDB(args map[string]any) (Database, error) {
	name, _ := args["name"].(string)
	if name == "" {
		return nil, fmt.Errorf("database name required")
	}
	if e.ResolveDatabase == nil {
		return nil, fmt.Errorf("database resolver not configured")
	}
	return e.ResolveDatabase(name)
}

func (e *ScriptEngine) BeginTx(ctx context.Context, args map[string]any) (sop.Transaction, error) {
	dbName, _ := args["database"].(string) // Variable name of DB
	dbName = e.resolveVarName(dbName)
	modeStr, _ := args["mode"].(string)

	var db Database
	if dbObj, ok := e.Context.Databases[dbName]; ok {
		db = dbObj
	} else {
		// Try to resolve by name if not a variable
		if e.ResolveDatabase == nil {
			return nil, fmt.Errorf("database resolver not configured")
		}
		var err error
		db, err = e.ResolveDatabase(dbName)
		if err != nil {
			return nil, fmt.Errorf("database '%s' not found in context or registry", dbName)
		}
	}

	mode := sop.ForReading
	if modeStr == "write" {
		mode = sop.ForWriting
	}

	return db.BeginTransaction(ctx, mode)
}

func (e *ScriptEngine) CommitTx(ctx context.Context, args map[string]any) error {
	txName, _ := args["transaction"].(string)
	txName = e.resolveVarName(txName)
	tx, ok := e.Context.Transactions[txName]
	if !ok {
		return fmt.Errorf("transaction '%s' not found", txName)
	}

	// Trigger: Commit Transaction
	// Action: Materialize all active cursors to ensure they are captured before transaction closes.

	drain := func(name string, cursor ScriptCursor) error {
		results := make([]any, 0)

		// Helper to wrap fields if spec available
		var specs map[string]*jsondb.IndexSpecification
		if provider, ok := cursor.(SpecProvider); ok {
			specs = provider.GetIndexSpecs()
		}

		// Drain cursor
		for {
			itemObj, ok, err := cursor.Next(ctx)
			if err != nil {
				return fmt.Errorf("failed to materialize cursor '%s' before commit: %v", name, err)
			}
			if !ok {
				break
			}

			var itemMap map[string]any
			if m, ok := itemObj.(map[string]any); ok {
				itemMap = m
			} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
				itemMap = om.m
			} else if om, ok := itemObj.(OrderedMap); ok {
				itemMap = om.m
			}

			// Apply ordering if specs available
			if itemMap != nil && len(specs) > 0 {
				for fieldName, spec := range specs {
					if val, ok := itemMap[fieldName]; ok {
						if m, ok := val.(map[string]any); ok {
							itemMap[fieldName] = OrderedKey{m: m, spec: spec}
						}
					}
				}
			}
			results = append(results, itemObj)
		}
		cursor.Close()
		e.Context.Variables[name] = results
		return nil
	}

	// Prioritize LastUpdatedVar to ensure downstream cursors (limit, etc) get first dibs on consuming shared inputs
	if e.Context.LastUpdatedVar != "" {
		if val, ok := e.Context.Variables[e.Context.LastUpdatedVar]; ok {
			if cursor, ok := val.(ScriptCursor); ok {
				if err := drain(e.Context.LastUpdatedVar, cursor); err != nil {
					return err
				}
			}
		}
	}

	for name, val := range e.Context.Variables {
		if name == e.Context.LastUpdatedVar {
			continue
		}
		if cursor, ok := val.(ScriptCursor); ok {
			if err := drain(name, cursor); err != nil {
				return err
			}
		}
	}

	return tx.Commit(ctx)
}

func (e *ScriptEngine) RollbackTx(ctx context.Context, args map[string]any) error {
	txName, _ := args["transaction"].(string)
	txName = e.resolveVarName(txName)
	tx, ok := e.Context.Transactions[txName]
	if !ok {
		return fmt.Errorf("transaction '%s' not found", txName)
	}
	return tx.Rollback(ctx)
}

func (e *ScriptEngine) OpenStore(ctx context.Context, args map[string]any) (jsondb.StoreAccessor, error) {
	txName, _ := args["transaction"].(string)
	txName = e.resolveVarName(txName)
	storeName, _ := args["name"].(string)

	var tx sop.Transaction
	var ok bool

	if txName == "" {
		// Try to find a default transaction
		if len(e.Context.Transactions) == 1 {
			for _, t := range e.Context.Transactions {
				tx = t
				ok = true
				break
			}
		} else if len(e.Context.Transactions) > 1 {
			return nil, fmt.Errorf("transaction name required (multiple active transactions)")
		} else {
			return nil, fmt.Errorf("no active transaction found")
		}
	} else {
		tx, ok = e.Context.Transactions[txName]
	}

	if !ok {
		return nil, fmt.Errorf("transaction '%s' not found", txName)
	}

	// We need DB config. Assuming TX has access or we need DB variable?
	// jsondb.OpenStore needs sop.DatabaseOptions.
	// We can get it from the DB object if we tracked which DB created the TX.
	// Or we pass the DB variable name too.
	// Let's try to find the DB that owns this TX? Not easy.
	// Let's require 'database' arg (variable name)
	dbName, _ := args["database"].(string)
	dbName = e.resolveVarName(dbName)
	db, ok := e.Context.Databases[dbName]
	if !ok {
		// Fallback: maybe dbName is the actual name?
		if e.ResolveDatabase == nil {
			return nil, fmt.Errorf("database resolver not configured")
		}
		var err error
		db, err = e.ResolveDatabase(dbName)
		if err != nil {
			return nil, fmt.Errorf("database '%s' required for opening store", dbName)
		}
	}

	return jsondb.OpenStore(ctx, db.Config(), storeName, tx)
}

func (e *ScriptEngine) Scan(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string) // Variable name
	storeName = e.resolveVarName(storeName)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	limit, _ := args["limit"].(float64)
	if limit <= 0 {
		limit = 1000
	}
	direction, _ := args["direction"].(string)
	isDesc := strings.ToLower(direction) == "desc"
	startKey := args["start_key"]
	prefix := args["prefix"]
	filter := args["filter"]
	stream, _ := args["stream"].(bool)

	var okIter bool
	var err error

	// Inspect store for IndexSpecification
	var indexSpec *jsondb.IndexSpecification
	info := store.GetStoreInfo()
	if info.MapKeyIndexSpecification != "" {
		var spec jsondb.IndexSpecification
		if err := json.Unmarshal([]byte(info.MapKeyIndexSpecification), &spec); err == nil {
			indexSpec = &spec
		}
	}

	// Positioning
	if startKey != nil {
		if isDesc {
			okIter, err = store.FindInDescendingOrder(ctx, startKey)
		} else {
			okIter, err = store.FindOne(ctx, startKey, true)
		}
	} else if prefix != nil {
		if isDesc {
			okIter, err = store.FindInDescendingOrder(ctx, prefix)
		} else {
			okIter, err = store.FindOne(ctx, prefix, true)
		}
	} else {
		if isDesc {
			okIter, err = store.Last(ctx)
		} else {
			okIter, err = store.First(ctx)
		}
	}

	if err != nil {
		return nil, err
	}

	if !okIter {
		if stream {
			// Return empty cursor
			return &StoreCursor{
				store:     store,
				indexSpec: indexSpec,
				ctx:       ctx,
				limit:     int(limit),
				started:   true, // Started but empty
			}, nil
		}
		return []map[string]any{}, nil
	}

	var filterMap map[string]any
	if filter != nil {
		if m, ok := filter.(map[string]any); ok {
			filterMap = m
		}
	}

	if stream {
		return &StoreCursor{
			store:     store,
			indexSpec: indexSpec,
			ctx:       ctx,
			limit:     int(limit),
			filter:    filterMap,
			engine:    e,
			isDesc:    isDesc,
			prefix:    prefix,
			started:   false, // Not yet advanced past the first element (Scan positions at first)
		}, nil
	}

	var results []map[string]any
	count := 0
	for okIter && count < int(limit) {
		k, _ := store.GetCurrentKey()
		v, _ := store.GetCurrentValue(ctx)

		// Prefix Check
		if prefix != nil {
			if kStr, isStr := k.(string); isStr {
				pStr := fmt.Sprintf("%v", prefix)
				if !strings.HasPrefix(kStr, pStr) {
					break
				}
			}
		}

		// Filter Check (Push Down)
		item := map[string]any{
			"key":   k,
			"value": v,
		}
		if filter != nil {
			match, err := e.evaluateCondition(item, filter.(map[string]any))
			if err != nil {
				// Log error but continue? Or fail?
				// Fail for now
				return nil, fmt.Errorf("filter evaluation failed: %v", err)
			}
			if !match {
				// Skip this item
				if isDesc {
					okIter, _ = store.Previous(ctx)
				} else {
					okIter, _ = store.Next(ctx)
				}
				continue
			}
		}

		results = append(results, item)
		count++

		if isDesc {
			okIter, _ = store.Previous(ctx)
		} else {
			okIter, _ = store.Next(ctx)
		}
	}

	return results, nil
}

func (e *ScriptEngine) evaluateCondition(item any, condition any) (bool, error) {
	// CEL Filter
	if _, ok := condition.(string); ok {
		return false, fmt.Errorf("CEL filter expressions not supported yet")
	}

	// Simple Map Match
	if matchMap, ok := condition.(map[string]any); ok {
		return matchesMap(item, matchMap), nil
	}

	return false, fmt.Errorf("unsupported filter condition type")
}

// Reuse the logic from pipeline for Filter, Sort, Project, Limit
// But adapt to take 'any' input which is expected to be []map[string]any

func (e *ScriptEngine) Filter(ctx context.Context, input any, args map[string]any) (any, error) {
	if cursor, ok := input.(ScriptCursor); ok {
		return &FilterCursor{
			source: cursor,
			filter: args,
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

func (e *ScriptEngine) Sort(ctx context.Context, input any, args map[string]any) (any, error) {
	// Sort requires materialization
	var list []any

	if cursor, ok := input.(ScriptCursor); ok {
		// Materialize
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
	// Parse fields
	var fields []string
	// args itself might be the map of fields if called from bindOperation?
	// No, bindOperation passes 'args'.
	// In 'project' op, args usually contains "fields": ["a", "b"]
	// But wait, the previous implementation of stageProject:
	// func (e *ScriptEngine) stageProject(list []map[string]any, args map[string]any)
	// It expects args to contain the projection spec.

	// Let's look at stageProject implementation (I need to read it or assume).
	// I'll assume standard args["fields"] pattern.

	if f, ok := args["fields"].([]any); ok {
		for _, v := range f {
			if s, ok := v.(string); ok {
				fields = append(fields, s)
			}
		}
	} else if f, ok := args["fields"].([]string); ok {
		fields = f
	}

	if cursor, ok := input.(ScriptCursor); ok {
		log.Debug("Project returning ProjectCursor")
		return &ProjectCursor{
			source: cursor,
			fields: fields,
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
		limit = 1000 // Default?
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

// ListCursor wraps a slice of maps.
type ListCursor struct {
	items []any
	index int
}

func (lc *ListCursor) Next(ctx context.Context) (any, bool, error) {
	if lc.index >= len(lc.items) {
		return nil, false, nil
	}
	item := lc.items[lc.index]
	lc.index++
	return item, true, nil
}

func (lc *ListCursor) Close() error {
	return nil
}

func (e *ScriptEngine) Join(ctx context.Context, input any, args map[string]any) (any, error) {
	rightVar, _ := args["with"].(string)
	if rightVar == "" {
		// Try 'store' arg (alias for join_right)
		rightVar, _ = args["store"].(string)
	}
	rightVar = e.resolveVarName(rightVar)

	joinType, _ := args["type"].(string)
	if joinType == "" {
		joinType = "inner"
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

	// Check if Right is a Store
	rightStore, isRightStore := e.Context.Stores[rightVar]

	log.Debug("Join", "RightVar", rightVar, "IsStore", isRightStore, "JoinType", joinType)

	// DECISION: Prefer Lookup Join (JoinRightCursor) for Stores
	// We rely on JoinRightCursor's internal "Smart Mode":
	// 1. It attempts to find a Key Field (Prefix Match) to do fast Lookups.
	// 2. If it fails to identifying a Probe Key, it falls back to In-Memory Materialization.
	// This covers both "Indexed Join" (Fast) and "Non-Indexed Join" (Safe, No Hang).
	useLookupJoin := false
	if isRightStore {
		useLookupJoin = true
	}

	if useLookupJoin {
		// Use JoinRightCursor for optimized lookup join (or adaptive fallback)
		return &JoinRightCursor{
			left:     leftCursor,
			right:    rightStore,
			joinType: joinType,
			on:       on,
			ctx:      ctx,
			engine:   e,
		}, nil
	}

	// Fallback to In-Memory Nested Loop Join (For Variables, not Stores)
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
		// Drain cursor
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

	return e.stageJoin(leftList, rightList, joinType, on)
}

func (e *ScriptEngine) stageJoin(left, right []any, joinType string, on map[string]any) ([]any, error) {
	var results []any

	for _, lItem := range left {
		matched := false
		for _, rItem := range right {
			match := true
			for lField, rFieldRaw := range on {
				rField := fmt.Sprintf("%v", rFieldRaw)
				lVal := getField(lItem, lField)
				rVal := getField(rItem, rField)
				if fmt.Sprintf("%v", lVal) != fmt.Sprintf("%v", rVal) {
					match = false
					break
				}
			}

			if match {
				matched = true
				merged := make(map[string]any)

				if m, ok := lItem.(map[string]any); ok {
					for k, v := range m {
						merged[k] = v
					}
				} else if om, ok := lItem.(*OrderedMap); ok && om != nil {
					for k, v := range om.m {
						merged[k] = v
					}
				} else if om, ok := lItem.(OrderedMap); ok {
					for k, v := range om.m {
						merged[k] = v
					}
				}

				if m, ok := rItem.(map[string]any); ok {
					for k, v := range m {
						merged[k] = v
					}
				} else if om, ok := rItem.(*OrderedMap); ok && om != nil {
					for k, v := range om.m {
						merged[k] = v
					}
				} else if om, ok := rItem.(OrderedMap); ok {
					for k, v := range om.m {
						merged[k] = v
					}
				}

				results = append(results, merged)
			}
		}
		if !matched && joinType == "left" {
			results = append(results, lItem)
		}
	}
	return results, nil
}

// JoinRight is a pipeline-friendly alias for Join.
// It expects the input to be the Left stream, and the 'store' argument to be the Right store.
func (e *ScriptEngine) JoinRight(ctx context.Context, input any, args map[string]any) (any, error) {
	// Map 'store' to 'with' for Join compatibility
	if store, ok := args["store"]; ok {
		args["with"] = store
	}
	return e.Join(ctx, input, args)
}

func (e *ScriptEngine) Update(ctx context.Context, input any, args map[string]any) ([]any, error) {
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
	} else if l, ok := input.([]map[string]any); ok {
		for _, x := range l {
			list = append(list, x)
		}
	} else if l, ok := input.([]any); ok {
		list = l
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}

	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	return e.stageUpdate(ctx, list, store, args)
}

func (e *ScriptEngine) Delete(ctx context.Context, input any, args map[string]any) ([]any, error) {
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
	} else if l, ok := input.([]map[string]any); ok {
		for _, x := range l {
			list = append(list, x)
		}
	} else if l, ok := input.([]any); ok {
		list = l
	} else {
		return nil, fmt.Errorf("input must be a list of items or a cursor")
	}

	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	return e.stageDelete(ctx, list, store, args)
}

func (e *ScriptEngine) Inspect(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}

	si := store.GetStoreInfo()
	return map[string]any{
		"name":                          si.Name,
		"count":                         si.Count,
		"slot_length":                   si.SlotLength,
		"is_unique":                     si.IsUnique,
		"is_value_data_in_node_segment": si.IsValueDataInNodeSegment,
		"leaf_load_balancing":           si.LeafLoadBalancing,
		"description":                   si.Description,
	}, nil
}

// --- Stage Implementations (In-Memory) ---

func (e *ScriptEngine) stageFilter(input []any, args map[string]any) ([]any, error) {
	conditionRaw := args["condition"]
	if conditionRaw == nil {
		return input, nil
	}

	var output []any

	// CEL Filter
	if _, ok := conditionRaw.(string); ok {
		for _, item := range input {
			match, err := e.evaluateCondition(item, conditionRaw)
			if err == nil && match {
				output = append(output, item)
			}
		}
		return output, nil
	}

	// Simple Map Match
	if matchMap, ok := conditionRaw.(map[string]any); ok {
		for _, item := range input {
			if matchesMap(item, matchMap) {
				output = append(output, item)
			}
		}
		return output, nil
	}

	return input, fmt.Errorf("unsupported filter condition type")
}

func (e *ScriptEngine) stageSort(input []any, args map[string]any) ([]any, error) {
	fieldsRaw, ok := args["fields"].([]any)
	if !ok {
		return input, nil
	}
	var fields []string
	for _, f := range fieldsRaw {
		if s, ok := f.(string); ok {
			fields = append(fields, s)
		}
	}

	if len(fields) == 0 {
		return input, nil
	}

	sort.Slice(input, func(i, j int) bool {
		for _, field := range fields {
			parts := strings.Fields(field)
			fieldName := parts[0]
			desc := len(parts) > 1 && strings.ToLower(parts[1]) == "desc"

			valA := getField(input[i], fieldName)
			valB := getField(input[j], fieldName)

			// Compare
			cmp := compare(valA, valB)
			if cmp != 0 {
				if desc {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	return input, nil
}

func (e *ScriptEngine) stageProject(input []any, args map[string]any) ([]any, error) {
	var fields []string

	if fAny, ok := args["fields"].([]any); ok {
		for _, f := range fAny {
			if s, ok := f.(string); ok {
				fields = append(fields, s)
			}
		}
	} else if fStr, ok := args["fields"].([]string); ok {
		fields = fStr
	} else {
		return input, nil
	}

	log.Debug("stageProject called", "input_len", len(input), "input_fields", fields)

	var output []any
	for i, item := range input {
		newItem := renderItem(nil, item, fields)
		output = append(output, newItem)

		// Log sample of what we are creating
		if i == 0 {
			if om, ok := newItem.(*OrderedMap); ok {
				log.Debug("stageProject first item (OrderedMap)", "keys", om.keys)
			} else if om, ok := newItem.(OrderedMap); ok {
				log.Debug("stageProject first item (OrderedMap Value)", "keys", om.keys)
			} else {
				log.Debug("stageProject first item (Map/Other)", "type", fmt.Sprintf("%T", newItem))
			}
		}
	}

	return output, nil
}

func (e *ScriptEngine) stageLimit(input []any, args map[string]any) ([]any, error) {
	var limit int
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	} else if l, ok := args["count"].(float64); ok {
		limit = int(l)
	} else {
		// Default or error?
		limit = 1000
	}

	log.Debug("stageLimit called", "input_len", len(input), "limit", limit)

	if limit < len(input) {
		return input[:limit], nil
	}
	return input, nil
}

func (e *ScriptEngine) stageUpdate(ctx context.Context, input []any, store jsondb.StoreAccessor, args map[string]any) ([]any, error) {
	if store == nil {
		return nil, fmt.Errorf("update stage requires an active store from a previous scan")
	}
	values, _ := args["values"].(map[string]any)
	if len(values) == 0 {
		return input, nil
	}

	for _, itemObj := range input {
		var item map[string]any
		if m, ok := itemObj.(map[string]any); ok {
			item = m
		} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
			item = om.m
		} else if om, ok := itemObj.(OrderedMap); ok {
			item = om.m
		} else {
			continue
		}

		key := item["key"]
		// Merge values
		currentVal := item["value"]
		var newVal any
		if currMap, ok := currentVal.(map[string]any); ok {
			// Clone to avoid side effects?
			newMap := make(map[string]any)
			for k, v := range currMap {
				newMap[k] = v
			}
			for k, v := range values {
				newMap[k] = v
			}
			newVal = newMap
		} else {
			newVal = values // Overwrite if not a map?
		}

		if _, err := store.Update(ctx, key, newVal); err != nil {
			return nil, err
		}
		// Update item in memory too
		item["value"] = newVal
	}
	return input, nil
}

func (e *ScriptEngine) stageDelete(ctx context.Context, input []any, store jsondb.StoreAccessor, args map[string]any) ([]any, error) {
	if store == nil {
		return nil, fmt.Errorf("delete stage requires an active store from a previous scan")
	}

	for _, itemObj := range input {
		key := getField(itemObj, "key")
		if _, err := store.Remove(ctx, key); err != nil {
			return nil, err
		}
	}
	return []any{}, nil
}

// --- Control Flow Operations ---

func (e *ScriptEngine) If(ctx context.Context, args map[string]any) error {
	conditionRaw, ok := args["condition"]
	if !ok {
		return fmt.Errorf("if operation requires 'condition'")
	}

	// Evaluate Condition
	var result bool
	if _, ok := conditionRaw.(string); ok {
		return fmt.Errorf("CEL if conditions not supported yet")
	} else if condBool, ok := conditionRaw.(bool); ok {
		result = condBool
	}

	if result {
		if thenBlock, ok := args["then"].([]any); ok {
			// Convert []any to []ScriptInstruction
			var thenScript []ScriptInstruction
			bytes, _ := json.Marshal(thenBlock)
			if err := json.Unmarshal(bytes, &thenScript); err != nil {
				return fmt.Errorf("failed to parse 'then' block: %v", err)
			}
			return e.Execute(ctx, thenScript)
		}
	} else {
		if elseBlock, ok := args["else"].([]any); ok {
			var elseScript []ScriptInstruction
			bytes, _ := json.Marshal(elseBlock)
			if err := json.Unmarshal(bytes, &elseScript); err != nil {
				return fmt.Errorf("failed to parse 'else' block: %v", err)
			}
			return e.Execute(ctx, elseScript)
		}
	}

	return nil
}

func (e *ScriptEngine) Loop(ctx context.Context, args map[string]any) error {
	// ForEach loop over a list variable
	collectionVar, _ := args["collection"].(string)
	itemVar, _ := args["item_var"].(string)
	if itemVar == "" {
		itemVar = "item"
	}

	bodyBlock, ok := args["body"].([]any)
	if !ok {
		return fmt.Errorf("loop operation requires 'body' block")
	}

	var bodyScript []ScriptInstruction
	bytes, _ := json.Marshal(bodyBlock)
	if err := json.Unmarshal(bytes, &bodyScript); err != nil {
		return fmt.Errorf("failed to parse loop 'body': %v", err)
	}

	// Get Collection
	collection, ok := e.Context.Variables[collectionVar]
	if !ok {
		return fmt.Errorf("collection variable '%s' not found", collectionVar)
	}

	// Iterate
	// Support []map[string]any or []any
	if list, ok := collection.([]map[string]any); ok {
		for _, item := range list {
			e.Context.Variables[itemVar] = item
			if err := e.Execute(ctx, bodyScript); err != nil {
				return err
			}
		}
	} else if list, ok := collection.([]any); ok {
		for _, item := range list {
			e.Context.Variables[itemVar] = item
			if err := e.Execute(ctx, bodyScript); err != nil {
				return err
			}
		}
	} else {
		return fmt.Errorf("variable '%s' is not a list", collectionVar)
	}

	return nil
}

func (e *ScriptEngine) CallFunction(ctx context.Context, args map[string]any) (any, error) {
	functionName, _ := args["name"].(string)
	if functionName == "" {
		return nil, fmt.Errorf("function name required")
	}

	if e.FunctionHandler == nil {
		return nil, fmt.Errorf("function handler not configured")
	}

	// Handle Parameters (Scoped Variable Injection)
	// We save current values of variables that are being overwritten, and restore them after.
	savedVars := make(map[string]any)
	if params, ok := args["params"].(map[string]any); ok {
		for k, v := range params {
			if oldVal, exists := e.Context.Variables[k]; exists {
				savedVars[k] = oldVal
			}
			e.Context.Variables[k] = v
		}
	}

	// Call Handler
	var params map[string]any
	if p, ok := args["params"].(map[string]any); ok {
		params = p
	}
	res, err := e.FunctionHandler(ctx, functionName, params)

	// Restore Variables
	for k, v := range savedVars {
		e.Context.Variables[k] = v
	}

	return res, err
}

// --- Cursor Operations ---

func (e *ScriptEngine) First(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.First(ctx)
}

func (e *ScriptEngine) Last(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Last(ctx)
}

func (e *ScriptEngine) Next(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Next(ctx)
}

func (e *ScriptEngine) Previous(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	return store.Previous(ctx)
}

func (e *ScriptEngine) Find(ctx context.Context, args map[string]any) (bool, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return false, fmt.Errorf("store variable '%s' not found", storeName)
	}
	key := args["key"]
	desc, _ := args["desc"].(bool)

	if desc {
		return store.FindInDescendingOrder(ctx, key)
	}
	return store.FindOne(ctx, key, true)
}

func (e *ScriptEngine) Add(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	key := args["key"]
	value := args["value"]
	return store.Add(ctx, key, value)
}

func (e *ScriptEngine) GetCurrentKey(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	k, err := store.GetCurrentKey()
	return k, err
}

func (e *ScriptEngine) GetCurrentValue(ctx context.Context, args map[string]any) (any, error) {
	storeName, _ := args["store"].(string)
	store, ok := e.Context.Stores[storeName]
	if !ok {
		return nil, fmt.Errorf("store variable '%s' not found", storeName)
	}
	v, err := store.GetCurrentValue(ctx)
	return v, err
}

// --- List / Map Operations ---

func (e *ScriptEngine) ListAppend(ctx context.Context, args map[string]any) error {
	listVar, _ := args["list"].(string)
	item := args["item"]

	list, ok := e.Context.Variables[listVar]
	if !ok {
		return fmt.Errorf("list variable '%s' not found", listVar)
	}

	// Append
	if l, ok := list.([]any); ok {
		e.Context.Variables[listVar] = append(l, item)
	} else if l, ok := list.([]map[string]any); ok {
		// If item is map, ok.
		if m, ok := item.(map[string]any); ok {
			e.Context.Variables[listVar] = append(l, m)
		} else {
			// Convert to []any? Or fail?
			// Let's upgrade the list to []any
			newList := make([]any, len(l))
			for i, v := range l {
				newList[i] = v
			}
			e.Context.Variables[listVar] = append(newList, item)
		}
	} else {
		return fmt.Errorf("variable '%s' is not a list", listVar)
	}
	return nil
}

func (e *ScriptEngine) MapMerge(ctx context.Context, args map[string]any) (map[string]any, error) {
	map1Raw := args["map1"]
	map2Raw := args["map2"]

	result := make(map[string]any)

	if m1, ok := map1Raw.(map[string]any); ok {
		for k, v := range m1 {
			result[k] = v
		}
	}
	if m2, ok := map2Raw.(map[string]any); ok {
		for k, v := range m2 {
			result[k] = v
		}
	}
	return result, nil
}

func (a *DataAdminAgent) opCallScript(ctx context.Context, scriptCtx *ScriptContext, args map[string]any) (any, error) {
	scriptName, _ := args["name"].(string)
	if scriptName == "" {
		return nil, fmt.Errorf("script name required")
	}

	if a.systemDB == nil {
		return nil, fmt.Errorf("system database not available")
	}

	sysTx, err := a.systemDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return nil, fmt.Errorf("failed to begin system tx: %v", err)
	}
	defer sysTx.Rollback(ctx)

	funcStore, err := jsondb.OpenStore(ctx, a.systemDB.Config(), "scripts", sysTx)
	if err != nil {
		return nil, fmt.Errorf("failed to open scripts store: %v", err)
	}

	found, err := funcStore.FindOne(ctx, scriptName, true)
	if err != nil {
		return nil, fmt.Errorf("failed to find script: %v", err)
	}
	if !found {
		return nil, fmt.Errorf("script '%s' not found", scriptName)
	}

	val, err := funcStore.GetCurrentValue(ctx)
	if err != nil {
		return nil, err
	}

	funcMap, ok := val.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("invalid script format")
	}

	stepsRaw, ok := funcMap["steps"].([]any)
	if !ok {
		return nil, nil // Empty script
	}

	// Detect Script Type (Legacy vs Atomic)
	isAtomic := false
	if len(stepsRaw) > 0 {
		if firstStep, ok := stepsRaw[0].(map[string]any); ok {
			if _, hasOp := firstStep["op"]; hasOp {
				isAtomic = true
			}
		}
	}

	if isAtomic {
		// Serialize to detect changes
		bytes, err := json.Marshal(stepsRaw)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal script steps: %v", err)
		}

		sum := sha256.Sum256(bytes)
		currentHash := hex.EncodeToString(sum[:])

		// Check Cache
		a.compiledScriptsMu.RLock()
		cachedEntry, cached := a.compiledScripts[scriptName]
		a.compiledScriptsMu.RUnlock()

		var compiled CompiledScript

		if !cached || cachedEntry.Hash != currentHash {
			var script []ScriptInstruction
			if err := json.Unmarshal(bytes, &script); err != nil {
				return nil, fmt.Errorf("failed to parse script: %v", err)
			}

			compiled, err = CompileScript(script)
			if err != nil {
				return nil, fmt.Errorf("failed to compile script '%s': %v", scriptName, err)
			}

			// Update Cache
			a.compiledScriptsMu.Lock()
			a.compiledScripts[scriptName] = CachedScript{Script: compiled, Hash: currentHash}
			a.compiledScriptsMu.Unlock()
		} else {
			compiled = cachedEntry.Script
		}

		// Execute in CURRENT context (inherits variables, txs)
		// Wrap resolver to handle default database from context
		resolver := func(name string) (Database, error) {
			if name == "" {
				if p := ai.GetSessionPayload(ctx); p != nil && p.CurrentDB != "" {
					name = p.CurrentDB
				}
			}
			return a.resolveDatabase(name)
		}
		engine := NewScriptEngine(scriptCtx, resolver)
		engine.FunctionHandler = func(c context.Context, name string, args map[string]any) (any, error) {
			return a.opCallScript(c, engine.Context, map[string]any{"name": name, "params": args})
		}

		if err := compiled(ctx, engine); err != nil {
			return nil, err
		}

		// Return Result
		// Priority 1: Check for 'output' variable (Explicit override)
		if val, ok := engine.Context.Variables["output"]; ok {
			return val, nil
		}

		return nil, nil
	}

	// Legacy Tool Script
	for _, step := range stepsRaw {
		stepMap, _ := step.(map[string]any)
		toolName, _ := stepMap["tool"].(string)
		toolArgs, _ := stepMap["args"].(map[string]any)

		if a.registry == nil {
			return nil, fmt.Errorf("registry not initialized")
		}

		toolDef, ok := a.registry.Get(toolName)
		if !ok {
			return nil, fmt.Errorf("tool '%s' not found", toolName)
		}
		_, err := toolDef.Handler(ctx, toolArgs)
		if err != nil {
			return nil, fmt.Errorf("script step '%s' failed: %v", toolName, err)
		}
	}

	return nil, nil
}

// --- Helper Functions ---

func matchesMap(item any, condition map[string]any) bool {
	for k, v := range condition {
		itemVal := getField(item, k)

		// Check for operators map
		if ops, ok := v.(map[string]any); ok {
			isOperator := false
			for op := range ops {
				if strings.HasPrefix(op, "$") {
					isOperator = true
					break
				}
			}

			if isOperator {
				for op, opVal := range ops {
					switch op {
					case "$gt":
						if compare(itemVal, opVal) <= 0 {
							return false
						}
					case "$gte":
						if compare(itemVal, opVal) < 0 {
							return false
						}
					case "$lt":
						if compare(itemVal, opVal) >= 0 {
							return false
						}
					case "$lte":
						if compare(itemVal, opVal) > 0 {
							return false
						}
					case "$ne":
						if compare(itemVal, opVal) == 0 {
							return false
						}
					case "$in":
						found := false
						if list, ok := opVal.([]any); ok {
							for _, val := range list {
								if compare(itemVal, val) == 0 {
									found = true
									break
								}
							}
						}
						if !found {
							return false
						}
					case "$nin":
						if list, ok := opVal.([]any); ok {
							for _, val := range list {
								if compare(itemVal, val) == 0 {
									return false
								}
							}
						}
					}
				}
				continue
			}
		}

		// Direct Equality check
		if compare(itemVal, v) != 0 {
			return false
		}
	}
	return true
}

func getField(itemObj any, field string) any {
	var item map[string]any
	if m, ok := itemObj.(map[string]any); ok {
		item = m
	} else if om, ok := itemObj.(OrderedMap); ok {
		item = om.m
	} else if om, ok := itemObj.(*OrderedMap); ok && om != nil {
		item = om.m
	} else {
		return nil
	}

	// Support "value.age" or just "age" (implicit value)
	if v, ok := item[field]; ok {
		return v
	}

	// Support relaxed aliased lookup (e.g. "a.name" -> "name") provided the alias didn't match
	if idx := strings.Index(field, "."); idx != -1 {
		suffix := field[idx+1:]
		if v, ok := item[suffix]; ok {
			return v
		}
	}

	// Check for key field (alias or direct)
	if field == "key" {
		if v, ok := item["key"]; ok {
			return v
		}
	}

	// Check inside key if it's a map (Composite Key support)
	if keyMap, ok := item["key"].(map[string]any); ok {
		if v, ok := keyMap[field]; ok {
			return v
		}
	}

	// Implicit check in "value" map
	if valMap, ok := item["value"].(map[string]any); ok {
		if v, ok := valMap[field]; ok {
			return v
		}
	}

	// Implicit check in "key" if it is a map (Composite Key)
	if keyMap, ok := item["key"].(map[string]any); ok {
		if v, ok := keyMap[field]; ok {
			return v
		}
	}

	return nil
}

// Helper: Compare two values
func compare(a, b any) int {
	// Handle nil
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	// Handle Strings
	sa, okA := a.(string)
	sb, okB := b.(string)
	if okA && okB {
		if sa < sb {
			return -1
		}
		if sa > sb {
			return 1
		}
		return 0
	}

	// Handle Numbers
	fa, okA := toFloat(a)
	fb, okB := toFloat(b)
	if okA && okB {
		if fa < fb {
			return -1
		}
		if fa > fb {
			return 1
		}
		return 0
	}

	// Fallback to string representation
	sa = fmt.Sprintf("%v", a)
	sb = fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	}
	if sa > sb {
		return 1
	}
	return 0
}

func toFloat(v any) (float64, bool) {
	switch i := v.(type) {
	case int:
		return float64(i), true
	case int64:
		return float64(i), true
	case float64:
		return i, true
	case float32:
		return float64(i), true
	case int32:
		return float64(i), true
	case int16:
		return float64(i), true
	case int8:
		return float64(i), true
	case uint:
		return float64(i), true
	case uint64:
		return float64(i), true
	case uint32:
		return float64(i), true
	case uint16:
		return float64(i), true
	case uint8:
		return float64(i), true
	}
	return 0, false
}
