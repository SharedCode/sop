package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"text/template"

	log "log/slog"

	"golang.org/x/sync/errgroup"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

type contextKey string

const CtxKeyJSONStreamer contextKey = "json_streamer"

type StepExecutionResult struct {
	Type    string `json:"type"`
	Command string `json:"command,omitempty"`
	Prompt  string `json:"prompt,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   string `json:"error,omitempty"`
}

// JSONStreamer handles streaming JSON array elements
type JSONStreamer struct {
	w     io.Writer
	mu    sync.Mutex
	first bool
}

func NewJSONStreamer(w io.Writer) *JSONStreamer {
	return &JSONStreamer{
		w:     w,
		first: true,
	}
}

func (s *JSONStreamer) Write(step StepExecutionResult) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.first {
		fmt.Fprint(s.w, ",\n")
	}
	s.first = false

	// Marshal the step
	bytes, _ := json.MarshalIndent(step, "  ", "  ")
	fmt.Fprint(s.w, string(bytes))
}

func (s *Service) runStep(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	switch step.Type {
	case "ask":
		return s.runStepAsk(ctx, step, scope, scopeMu, sb, db)
	case "command":
		return s.runStepCommand(ctx, step, scope, scopeMu, sb)
	case "set", "assignment":
		return s.runStepSet(ctx, step, scope, scopeMu, sb)
	case "if", "condition":
		return s.runStepIf(ctx, step, scope, scopeMu, sb, db)
	case "loop":
		return s.runStepLoop(ctx, step, scope, scopeMu, sb, db)
	case "fetch":
		return s.runStepFetch(ctx, step, scope, scopeMu, sb, db)
	case "say", "print":
		return s.runStepSay(ctx, step, scope, scopeMu, sb)
	case "macro":
		return s.runStepMacro(ctx, step, scope, scopeMu, sb, db)
	case "block":
		return s.runStepBlock(ctx, step, scope, scopeMu, sb, db)
	}
	return nil
}

func (s *Service) runStepBlock(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	for _, subStep := range step.Steps {
		if err := s.runStep(ctx, subStep, scope, scopeMu, sb, db); err != nil {
			if !step.ContinueOnError {
				return err
			}
		}
	}
	return nil
}

func (s *Service) runStepAsk(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	prompt := step.Prompt
	prompt = s.resolveTemplate(prompt, scope, scopeMu)

	// Only print prompt if NOT using streamer (legacy mode)
	if ctx.Value(CtxKeyJSONStreamer) == nil {
		msg := fmt.Sprintf("> %s\n", prompt)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}
	}

	var opts []ai.Option
	if db != nil {
		opts = append(opts, ai.WithDatabase(db))
	}

	// Check for compiled mode
	if isCompiled, ok := ctx.Value("is_compiled").(bool); ok && isCompiled {
		return nil
	}

	resp, err := s.Ask(ctx, prompt, opts...)
	if err != nil {
		msg := fmt.Sprintf("Error: %v\n", err)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}
		// Continue on error, but log it.
	} else {
		// Check if resp is a tool call
		var toolCall struct {
			Tool string         `json:"tool"`
			Args map[string]any `json:"args"`
		}
		if json.Unmarshal([]byte(resp), &toolCall) == nil && toolCall.Tool != "" {
			// It's a tool call! Execute it.
			if executor, ok := ctx.Value(ai.CtxKeyExecutor).(ai.ToolExecutor); ok && executor != nil {
				toolResp, err := executor.Execute(ctx, toolCall.Tool, toolCall.Args)
				if err != nil {
					resp = fmt.Sprintf("Error executing tool '%s': %v", toolCall.Tool, err)
				} else {
					resp = toolResp
				}
			}
		}

		if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
			var resultAny any = resp
			// Try to unmarshal if it looks like JSON
			trimmed := strings.TrimSpace(resp)
			if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				// Optimization: Use RawMessage
				resultAny = json.RawMessage(resp)
			}

			streamer.Write(StepExecutionResult{
				Type:   "ask",
				Prompt: prompt,
				Result: resultAny,
			})
		} else {
			msg := fmt.Sprintf("%s\n", resp)
			sb.WriteString(msg)
			if w != nil {
				fmt.Fprint(w, msg)
			}
		}

		// Support "Variable" field for backward compatibility
		outVar := step.OutputVariable
		if outVar == "" {
			outVar = step.Variable
		}

		if outVar != "" {
			if scopeMu != nil {
				scopeMu.Lock()
				defer scopeMu.Unlock()
			}
			scope[outVar] = strings.TrimSpace(resp)
		}
	}
	return nil
}

func (s *Service) runStepCommand(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	// Resolve templates in Args
	resolvedArgs := make(map[string]any)
	for k, v := range step.Args {
		if strVal, ok := v.(string); ok {
			resolvedArgs[k] = s.resolveTemplate(strVal, scope, scopeMu)
		} else {
			resolvedArgs[k] = v
		}
	}

	// Execute Tool
	val := ctx.Value(ai.CtxKeyExecutor)
	if val == nil {
		// If no executor, but command is empty, we can skip it.
		if step.Command == "" {
			return nil
		}
		return fmt.Errorf("no tool executor available (ctx value is nil)")
	}

	executor, ok := val.(ai.ToolExecutor)
	if !ok {
		return fmt.Errorf("no tool executor available (type assertion failed, type: %T)", val)
	}

	if executor != nil {
		// Skip empty commands
		if step.Command == "" {
			return nil
		}

		// msg := fmt.Sprintf("Executing command '%s'...\n", step.Command)
		// sb.WriteString(msg)
		// if w != nil {
		// 	fmt.Fprint(w, msg)
		// }

		resp, err := executor.Execute(ctx, step.Command, resolvedArgs)
		if err != nil {
			return fmt.Errorf("command execution failed: %w", err)
		}

		// Stream result if streamer is present
		if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
			var resultAny any = resp
			// Try to unmarshal if it looks like JSON
			trimmed := strings.TrimSpace(resp)
			if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
				// Optimization: Use RawMessage to avoid full unmarshal/marshal cycle
				// This assumes the tool output is valid JSON.
				resultAny = json.RawMessage(resp)
			}

			streamer.Write(StepExecutionResult{
				Type:    "command",
				Command: step.Command,
				Result:  resultAny,
			})
		} else {
			// Fallback to string builder if no streamer (legacy mode)
			msg := fmt.Sprintf("%s\n", resp)
			sb.WriteString(msg)
			if w != nil {
				fmt.Fprint(w, msg)
			}
		}

		// Output Variable
		if step.OutputVariable != "" {
			if scopeMu != nil {
				scopeMu.Lock()
				defer scopeMu.Unlock()
			}
			scope[step.OutputVariable] = strings.TrimSpace(resp)
		}
	} else {
		// Should be unreachable due to checks above
		return fmt.Errorf("no tool executor available")
	}
	return nil
}

func (s *Service) runStepSet(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder) error {
	val := step.Value
	val = s.resolveTemplate(val, scope, scopeMu)

	if step.Variable != "" {
		if scopeMu != nil {
			scopeMu.Lock()
			defer scopeMu.Unlock()
		}
		scope[step.Variable] = val
	}
	return nil
}

func (s *Service) runStepIf(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	cond := step.Condition

	// Evaluate Expression using template: {{ if .Expression }}true{{end}}
	condTmpl := fmt.Sprintf("{{if %s}}true{{end}}", cond)

	if scopeMu != nil {
		scopeMu.RLock()
		defer scopeMu.RUnlock()
	}

	if tmpl, err := template.New("cond").Parse(condTmpl); err == nil {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, scope); err == nil {
			if buf.String() == "true" {
				thenSteps := step.Then
				return s.runSteps(ctx, thenSteps, scope, scopeMu, sb, db)
			} else {
				if len(step.Else) > 0 {
					return s.runSteps(ctx, step.Else, scope, scopeMu, sb, db)
				}
			}
		} else {
			if ctx.Value(CtxKeyJSONStreamer) == nil {
				msg := fmt.Sprintf("Error evaluating condition '%s': %v\n", cond, err)
				sb.WriteString(msg)
				if w != nil {
					fmt.Fprint(w, msg)
				}
			}
		}
	}
	return nil
}

func (s *Service) runStepLoop(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	listExpr := step.List
	iterator := step.Iterator
	body := step.Steps

	if scopeMu != nil {
		scopeMu.RLock()
	}
	val, ok := scope[listExpr]
	if scopeMu != nil {
		scopeMu.RUnlock()
	}

	if ok {
		if strVal, ok := val.(string); ok {
			lines := strings.Split(strVal, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				if scopeMu != nil {
					scopeMu.Lock()
				}
				scope[iterator] = line
				if scopeMu != nil {
					scopeMu.Unlock()
				}
				if err := s.runSteps(ctx, body, scope, scopeMu, sb, db); err != nil {
					return err
				}
			}
		} else if sliceVal, ok := val.([]any); ok {
			for _, item := range sliceVal {
				if scopeMu != nil {
					scopeMu.Lock()
				}
				scope[iterator] = item
				if scopeMu != nil {
					scopeMu.Unlock()
				}
				if err := s.runSteps(ctx, body, scope, scopeMu, sb, db); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *Service) runStepFetch(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, currentDB *database.Database) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	resource := step.Resource
	outVar := step.Variable // Fetch uses Variable for output in both schemas

	if step.Source == "btree" && resource != "" {
		var db *database.Database

		// 1. Resolve Database
		if step.Database != "" {
			// Use Service configuration for resolution
			if opts, ok := s.databases[step.Database]; ok {
				db = database.NewDatabase(opts)
			} else {
				return fmt.Errorf("database '%s' not found in agent configuration", step.Database)
			}
		} else {
			// Use current context database
			db = currentDB
		}

		if db == nil {
			if ctx.Value(CtxKeyJSONStreamer) == nil {
				msg := "Error: No database configured for fetch operation.\n"
				sb.WriteString(msg)
				if w != nil {
					fmt.Fprint(w, msg)
				}
			}
			return fmt.Errorf("no database provided")
		}

		// 2. Fetch Data
		// Check if we have an active transaction in the session payload
		var tx sop.Transaction
		var err error

		if p := ai.GetSessionPayload(ctx); p != nil && p.Transaction != nil {
			if t, ok := p.Transaction.(sop.Transaction); ok {
				tx = t
			}
		}

		// If no active transaction, start a local one (Read-Only)
		localTx := false
		if tx == nil {
			tx, err = db.BeginTransaction(ctx, sop.ForReading)
			if err != nil {
				return fmt.Errorf("failed to begin transaction: %w", err)
			}
			localTx = true
		}

		if localTx {
			defer tx.Rollback(ctx)
		}

		store, err := db.OpenBtree(ctx, resource, tx)
		if err != nil {
			return fmt.Errorf("failed to open store '%s': %w", resource, err)
		}

		// 3. Iterate and Collect
		var items []any
		if ok, err := store.First(ctx); ok && err == nil {
			for {
				k := store.GetCurrentKey()
				v, _ := store.GetCurrentValue(ctx)
				items = append(items, fmt.Sprintf("%v: %v", k, v))
				if ok, _ := store.Next(ctx); !ok {
					break
				}
				if len(items) >= 10 {
					break
				}
			}
		}

		if outVar != "" {
			if scopeMu != nil {
				scopeMu.Lock()
				defer scopeMu.Unlock()
			}
			scope[outVar] = items
		}
	}
	return nil
}

func (s *Service) runStepSay(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	msgText := step.Message
	msgText = s.resolveTemplate(msgText, scope, scopeMu)

	if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
		streamer.Write(StepExecutionResult{
			Type:   "say",
			Result: msgText,
		})
	} else {
		msg := fmt.Sprintf("%s\n", msgText)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}
	}
	return nil
}

func (s *Service) runStepMacro(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	name := step.MacroName
	if name == "" {
		return fmt.Errorf("macro name required")
	}

	// Load the macro
	macroDB := s.getMacroDB()
	if macroDB == nil {
		return fmt.Errorf("no database configured")
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("error opening store: %v", err)
	}

	var macro ai.Macro
	if err := store.Load(ctx, "macros", name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Errorf("error loading macro '%s': %v", name, err)
	}
	tx.Commit(ctx)

	// Prepare scope for the nested macro
	// We inherit the current scope, but we might want to override with MacroArgs
	nestedScope := make(map[string]any)
	if scopeMu != nil {
		scopeMu.RLock()
	}
	for k, v := range scope {
		nestedScope[k] = v
	}
	if scopeMu != nil {
		scopeMu.RUnlock()
	}

	for k, v := range step.MacroArgs {
		// Resolve template in args
		val := s.resolveTemplate(v, scope, scopeMu)
		nestedScope[k] = val
	}

	sb.WriteString(fmt.Sprintf("Running nested macro '%s'...\n", name))
	// Nested macro gets its own mutex because it has its own scope map
	var nestedMu sync.RWMutex

	// Handle Database Switching for Nested Macro
	// Priority: step.Database > macro.Database > inherited db

	// Handle Database Override from Step
	if step.Database != "" {
		macro.Database = step.Database
		macro.Portable = false // Enforce the step's DB
	}

	return s.executeMacro(ctx, &macro, nestedScope, &nestedMu, sb, db)
}

// ToolProvider interface for agents that can execute tools
type ToolProvider interface {
	ExecuteTool(ctx context.Context, toolName string, args map[string]any) (string, error)
}

// ServiceToolExecutor delegates tool execution to registered agents
type ServiceToolExecutor struct {
	s *Service
}

func (e *ServiceToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	for _, agent := range e.s.registry {
		if provider, ok := agent.(ToolProvider); ok {
			resp, err := provider.ExecuteTool(ctx, toolName, args)
			if err == nil {
				return resp, nil
			}
			// If error is "unknown tool", continue to next agent.
			// Otherwise return error.
			// Since we don't have a standard error type for "unknown tool", we check string.
			// DataAdminAgent returns "unknown tool: %s".
			if strings.Contains(err.Error(), "unknown tool") {
				continue
			}
			return "", err
		}
	}
	return "", fmt.Errorf("tool '%s' not found in any registered agent", toolName)
}

func (e *ServiceToolExecutor) ListTools(ctx context.Context) ([]ai.ToolDefinition, error) {
	return nil, nil
}

func (s *Service) executeMacro(ctx context.Context, macro *ai.Macro, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	// Ensure we have a tool executor
	if ctx.Value(ai.CtxKeyExecutor) == nil {
		executor := &ServiceToolExecutor{s: s}
		ctx = context.WithValue(ctx, ai.CtxKeyExecutor, executor)
	} else {
		// Debug: Executor already present
	}

	// Detect compiled mode (if any step is a command)
	isCompiled := false
	for _, step := range macro.Steps {
		if step.Type == "command" {
			isCompiled = true
			break
		}
	}
	ctx = context.WithValue(ctx, "is_compiled", isCompiled)

	return s.runSteps(ctx, macro.Steps, scope, scopeMu, sb, db)
}

// runSteps runs a sequence of macro steps with support for control flow and variables.
func (s *Service) runSteps(ctx context.Context, steps []ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	// Create cancellable context for this macro execution scope to support stopping on error
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Use errgroup for managing concurrency and error propagation
	g, groupCtx := errgroup.WithContext(ctx)

	var syncErr error

	for _, step := range steps {
		// Check if context is already cancelled (by errgroup or sync error)
		if groupCtx.Err() != nil {
			break
		}

		// Refresh DB from payload to ensure we use the current active database
		// This handles cases where a previous step (e.g. a tool) switched the database.
		if p := ai.GetSessionPayload(groupCtx); p != nil {
			dbName := p.GetDatabase()
			if dbName != "" {
				if opts, ok := s.databases[dbName]; ok {
					db = database.NewDatabase(opts)
				}
			}
		}
		log.Debug(fmt.Sprintf("DB from payload: %v", db))

		// If async, run in errgroup
		if step.IsAsync {
			// Check if we are in a transaction.
			// If so, we generally force sync execution to maintain integrity for simple steps.
			// HOWEVER, if the step is a nested "macro", we allow it to run async (Swarm pattern),
			// assuming it will manage its own transaction (e.g. on a different DB).
			p := ai.GetSessionPayload(groupCtx)
			if p != nil && p.Transaction != nil && step.Type != "macro" {
				if ctx.Value(CtxKeyJSONStreamer) == nil {
					msg := fmt.Sprintf("Info: Step '%s' marked async but active transaction exists. Running synchronously.\n", step.Type)
					sb.WriteString(msg)
					if w, ok := ctx.Value(ai.CtxKeyWriter).(io.Writer); ok && w != nil {
						fmt.Fprint(w, msg)
					}
				}
				// Fall through to sync execution
			} else {
				// Detach transaction to enforce isolation (Swarm Computing model)
				// Async steps must not share the parent transaction to avoid race conditions.
				// They should start their own transaction if needed.
				asyncCtx := groupCtx
				if p != nil {
					// Clone payload and remove transaction (though p.Transaction should be nil here based on check above,
					// but good to be safe if logic changes)
					newPayload := *p
					newPayload.Transaction = nil
					asyncCtx = context.WithValue(groupCtx, "session_payload", &newPayload)
				}

				// Capture step for closure
				st := step
				g.Go(func() error {
					// Handle panic in async step
					defer func() {
						if r := recover(); r != nil {
							// We can't easily return the panic as error here without named return,
							// but errgroup expects an error return.
							// We'll just log it (if we had a logger) and return a generic error.
							// For now, let's assume runStep handles its own panics or we let it crash?
							// Better to recover and return error to stop the group.
						}
					}()

					if err := s.runStep(asyncCtx, st, scope, scopeMu, sb, db); err != nil {
						if st.ContinueOnError {
							// Log error but don't stop the group
							// TODO: Add logging
							return nil
						}
						return err // This cancels groupCtx
					}
					return nil
				})
				continue
			}
		}

		// Sync step
		if err := s.runStep(groupCtx, step, scope, scopeMu, sb, db); err != nil {
			if step.ContinueOnError {
				// Log error and continue
				continue
			}
			// Stop everything
			syncErr = err
			cancel() // Cancel context for async tasks
			break
		}
	}

	// Wait for all async tasks
	asyncErr := g.Wait()

	if syncErr != nil {
		return syncErr
	}
	return asyncErr
}

func (s *Service) resolveTemplate(tmplStr string, scope map[string]any, scopeMu *sync.RWMutex) string {
	if tmplStr == "" {
		return ""
	}

	if scopeMu != nil {
		scopeMu.RLock()
		defer scopeMu.RUnlock()
	}

	if tmpl, err := template.New("tmpl").Parse(tmplStr); err == nil {
		var buf bytes.Buffer
		if err := tmpl.Execute(&buf, scope); err == nil {
			return buf.String()
		}
	}
	return tmplStr
}
