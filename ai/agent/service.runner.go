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

// StartStreamingStep starts a new step in the JSON stream and returns a StepStreamer.
// Note: The header is written lazily when the first item is written.
func (s *JSONStreamer) StartStreamingStep(stepType, command, prompt string) *StepStreamer {
	return &StepStreamer{
		parent:   s,
		first:    true,
		stepType: stepType,
		command:  command,
		prompt:   prompt,
	}
}

// StepStreamer implements ai.ResultStreamer to stream result items.
type StepStreamer struct {
	parent   *JSONStreamer
	first    bool
	closed   bool
	used     bool
	stepType string
	command  string
	prompt   string
}

func (ss *StepStreamer) writeHeader() {
	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()

	if !ss.parent.first {
		fmt.Fprint(ss.parent.w, ",\n")
	}
	ss.parent.first = false

	fmt.Fprintf(ss.parent.w, `{"type":%q`, ss.stepType)
	if ss.command != "" {
		fmt.Fprintf(ss.parent.w, `,"command":%q`, ss.command)
	}
	if ss.prompt != "" {
		fmt.Fprintf(ss.parent.w, `,"prompt":%q`, ss.prompt)
	}
	fmt.Fprint(ss.parent.w, `,"result":`)
}

func (ss *StepStreamer) BeginArray() {
	if !ss.used {
		ss.writeHeader()
		ss.used = true
	}
	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()
	fmt.Fprint(ss.parent.w, "[")
}

func (ss *StepStreamer) WriteItem(item any) {
	if !ss.used {
		ss.writeHeader()
		ss.used = true
	}
	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()

	if !ss.first {
		fmt.Fprint(ss.parent.w, ",")
	}
	ss.first = false

	b, _ := json.Marshal(item)
	ss.parent.w.Write(b)
}

func (ss *StepStreamer) EndArray() {
	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()
	fmt.Fprint(ss.parent.w, "]")
}

func (ss *StepStreamer) Close() {
	if ss.closed {
		return
	}
	ss.parent.mu.Lock()
	defer ss.parent.mu.Unlock()
	fmt.Fprint(ss.parent.w, "}")
	ss.closed = true
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
	return s.runSteps(ctx, step.Steps, scope, scopeMu, sb, db)
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
	// If the macro is compiled (has explicit commands), we skip "ask" steps because
	// the logic assumes the commands replace the need for asking the LLM.
	// HOWEVER, for hybrid macros or explicit "ask" steps in a compiled macro,
	// we might want to allow it.
	// The current logic: if isCompiled is true, we SKIP the ask step.
	// This means "ask" steps are ignored in compiled macros.
	// If the user wants to force an ask in a compiled macro, they should use a different type or flag?
	// Or maybe we should only skip if the ask was the *source* of the commands (which we don't track here).
	// For now, let's keep the behavior but document it: "ask" steps are skipped if "command" steps exist.
	if isCompiled, ok := ctx.Value("is_compiled").(bool); ok && isCompiled {
		// But wait! If the user explicitly added an "ask" step to a compiled macro (e.g. for analysis),
		// we shouldn't skip it.
		// The "is_compiled" flag was likely intended for "playback of recorded sessions" where
		// the "ask" was the user input and the "command" was the result.
		// In that case, we only want to run the command.
		// But if we are running a "programmed" macro that mixes both, this logic is flawed.
		// Let's refine: We skip "ask" ONLY if it doesn't have an OutputVariable that is used later?
		// Or maybe we should trust the macro definition.
		// If the macro has BOTH "ask" and "command" steps, it's likely a recording.
		// In a recording, "ask" is the trigger, "command" is the action. We replay the action.
		// So skipping "ask" is correct for REPLAY.
		// But for "Hybrid" macros?
		// Let's assume for now that if it's a REST call, we might want the LLM to run if it's an "ask".
		// But if it's a recording, we don't.
		// How to distinguish?
		// Maybe we can check if the "ask" has a corresponding "command" immediately following it?
		// For now, I will leave it as is to preserve existing "Replay" behavior.
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

		// Prepare streamer if available
		var stepStreamer *StepStreamer
		if streamer, ok := ctx.Value(CtxKeyJSONStreamer).(*JSONStreamer); ok {
			stepStreamer = streamer.StartStreamingStep("command", step.Command, "")
			ctx = context.WithValue(ctx, ai.CtxKeyResultStreamer, stepStreamer)
		}

		resp, err := executor.Execute(ctx, step.Command, resolvedArgs)

		// Check if tool streamed the result
		if stepStreamer != nil && stepStreamer.used {
			stepStreamer.Close()
			if err != nil {
				return fmt.Errorf("command execution failed (streamed): %w", err)
			}
			// If streamed, we don't capture output to variable currently
			return nil
		}

		if err != nil {
			return fmt.Errorf("command execution failed: %w", err)
		}

		// Stream result if streamer is present (and wasn't used by tool)
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

		store, err := db.OpenBtreeCursor(ctx, resource, tx)
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
	if err := store.Load(ctx, "general", name, &macro); err != nil {
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
	Execute(ctx context.Context, toolName string, args map[string]any) (string, error)
}

// ServiceToolExecutor delegates tool execution to registered agents
type ServiceToolExecutor struct {
	s *Service
}

func (e *ServiceToolExecutor) Execute(ctx context.Context, toolName string, args map[string]any) (string, error) {
	for _, agent := range e.s.registry {
		if provider, ok := agent.(ToolProvider); ok {
			resp, err := provider.Execute(ctx, toolName, args)
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
	// Remove MacroRecorder from context during execution to ensure tools execute instead of recording
	ctx = context.WithValue(ctx, ai.CtxKeyMacroRecorder, nil)

	// Ensure we have a tool executor
	if ctx.Value(ai.CtxKeyExecutor) == nil {
		executor := &ServiceToolExecutor{s: s}
		ctx = context.WithValue(ctx, ai.CtxKeyExecutor, executor)
	}

	// Detect compiled mode (if any step is a command)
	// We treat a macro as "compiled" (replay mode) if it contains "command" steps.
	// This causes "ask" steps to be skipped during execution, assuming they were just the triggers for the commands.
	// TODO: Allow a flag to force execution of "ask" steps even in compiled macros (e.g. for hybrid agents).
	isCompiled := false
	for _, step := range macro.Steps {
		if step.Type == "command" {
			isCompiled = true
			break
		}
	}
	ctx = context.WithValue(ctx, "is_compiled", isCompiled)

	// Set Playback flag
	// wasPlayback := s.session.Playback
	s.session.Playback = true
	// defer func() { s.session.Playback = wasPlayback }()

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

	// Initialize currentDBName from payload
	var currentDBName string
	if p := ai.GetSessionPayload(ctx); p != nil {
		currentDBName = p.CurrentDB
	}

	for _, step := range steps {
		// Check if context is already cancelled (by errgroup or sync error)
		if groupCtx.Err() != nil {
			break
		}

		// Determine effective database for this step
		stepDBName := step.Database
		if stepDBName != "" {
			// Step specifies DB -> Update currentDBName (sticky)
			currentDBName = stepDBName
		} else {
			// Step does not specify -> Fallback to currentDBName
			stepDBName = currentDBName
		}

		log.Debug("Determined effective database for step", "step_type", step.Type, "db", stepDBName)

		// Prepare Context and DB for this step
		stepCtx := groupCtx
		stepDB := db

		// Check for dead transaction (committed or rolled back)
		// This prevents "running cmd on a dead transaction" errors if a previous step manually closed it.
		p := ai.GetSessionPayload(groupCtx)
		if p != nil && p.Transaction != nil {
			if t, ok := p.Transaction.(sop.Transaction); ok && !t.HasBegun() {
				// Transaction is dead. Clear it from the payload for this step.
				// This forces the step to start a new local transaction (or session one if we could, but local is safer fallback).
				newPayload := *p
				newPayload.Transaction = nil
				stepCtx = context.WithValue(stepCtx, "session_payload", &newPayload)
				// Update p to point to newPayload so subsequent logic uses the clean state
				p = &newPayload
			}
		}

		// Check if we need to update context/DB based on stepDBName
		if p != nil {
			// If the desired DB differs from the context, or if we just want to ensure we have the right DB object
			if p.CurrentDB != stepDBName {
				// Clone payload
				newPayload := *p
				newPayload.CurrentDB = stepDBName
				// Clear transaction if switching DB
				newPayload.Transaction = nil

				stepCtx = context.WithValue(groupCtx, "session_payload", &newPayload)
				// ...
			}
			// ...
		}

		// If async, run in errgroup
		if step.IsAsync {
			// Async Step Logic:
			// We capture the 'currentDBName' at the moment of dispatch (via stepCtx/stepDB).
			// The async step runs with a CLONED context containing that DB.
			// It does NOT affect the 'currentDBName' for subsequent steps in the loop,
			// nor does it affect other async steps. This ensures isolation.

			// Check if we are in a transaction.
			// If so, we generally force sync execution to maintain integrity for simple steps.
			// HOWEVER, if the step is a nested "macro", we allow it to run async (Swarm pattern),
			// assuming it will manage its own transaction (e.g. on a different DB).
			p := ai.GetSessionPayload(stepCtx) // Use stepCtx
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
				asyncCtx := stepCtx
				asyncDB := stepDB

				if p != nil {
					// Clone payload and remove transaction
					newPayload := *p
					newPayload.Transaction = nil
					asyncCtx = context.WithValue(stepCtx, "session_payload", &newPayload)
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

					if err := s.runStep(asyncCtx, st, scope, scopeMu, sb, asyncDB); err != nil {
						if st.ContinueOnError {
							// Log error but don't stop the group
							log.Error("Async step failed (continuing)", "step_type", st.Type, "error", err)
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
		if err := s.runStep(stepCtx, step, scope, scopeMu, sb, stepDB); err != nil {
			if step.ContinueOnError {
				// Log error and continue
				log.Error("Step failed (continuing)", "step_type", step.Type, "error", err)
				continue
			}
			// Stop everything
			syncErr = err
			cancel() // Cancel context for async tasks
			break
		}
		if p != nil && p.Transaction == nil {
			fmt.Println("DEBUG: runSteps - Transaction cleared after step execution!")
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
