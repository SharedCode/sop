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

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

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
	}
	return nil
}

func (s *Service) runStepAsk(ctx context.Context, step ai.MacroStep, scope map[string]any, scopeMu *sync.RWMutex, sb *strings.Builder, db *database.Database) error {
	w, _ := ctx.Value(ai.CtxKeyWriter).(io.Writer)

	prompt := step.Prompt
	prompt = s.resolveTemplate(prompt, scope, scopeMu)

	msg := fmt.Sprintf("> %s\n", prompt)
	sb.WriteString(msg)
	if w != nil {
		fmt.Fprint(w, msg)
	}

	var opts []ai.Option
	if db != nil {
		opts = append(opts, ai.WithDatabase(db))
	}

	resp, err := s.Ask(ctx, prompt, opts...)
	if err != nil {
		msg := fmt.Sprintf("Error: %v\n", err)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}
		// Don't fail the macro, just log error? Or should we fail?
		// For now, let's continue but maybe set variable to empty/error?
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

		msg := fmt.Sprintf("%s\n", resp)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}

		// Support legacy "Variable" field
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
	if executor, ok := ctx.Value(ai.CtxKeyExecutor).(ai.ToolExecutor); ok && executor != nil {
		msg := fmt.Sprintf("Executing command '%s'...\n", step.Command)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
		}

		resp, err := executor.Execute(ctx, step.Command, resolvedArgs)
		if err != nil {
			return fmt.Errorf("command execution failed: %w", err)
		}

		msg = fmt.Sprintf("%s\n", resp)
		sb.WriteString(msg)
		if w != nil {
			fmt.Fprint(w, msg)
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
				return s.executeMacro(ctx, thenSteps, scope, scopeMu, sb, db)
			} else {
				if len(step.Else) > 0 {
					return s.executeMacro(ctx, step.Else, scope, scopeMu, sb, db)
				}
			}
		} else {
			msg := fmt.Sprintf("Error evaluating condition '%s': %v\n", cond, err)
			sb.WriteString(msg)
			if w != nil {
				fmt.Fprint(w, msg)
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
				if err := s.executeMacro(ctx, body, scope, scopeMu, sb, db); err != nil {
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
				if err := s.executeMacro(ctx, body, scope, scopeMu, sb, db); err != nil {
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
			// Use SessionPayload for resolution
			if p := ai.GetSessionPayload(ctx); p != nil && p.Databases != nil {
				if val, ok := p.Databases[step.Database]; ok {
					if d, ok := val.(*database.Database); ok {
						db = d
					} else {
						return fmt.Errorf("resolved database '%s' is not of type *database.Database", step.Database)
					}
				} else {
					return fmt.Errorf("database '%s' not found in session payload", step.Database)
				}
			} else {
				return fmt.Errorf("session payload or databases map not configured")
			}
		} else {
			// Use current context database
			db = currentDB
		}

		if db == nil {
			msg := "Error: No database configured for fetch operation.\n"
			sb.WriteString(msg)
			if w != nil {
				fmt.Fprint(w, msg)
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

	msg := fmt.Sprintf("%s\n", msgText)
	sb.WriteString(msg)
	if w != nil {
		fmt.Fprint(w, msg)
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
	targetDB := db
	var macroTx sop.Transaction
	var macroCtx context.Context = ctx

	// Step-level override
	dbOverride := step.Database
	if dbOverride == "" {
		dbOverride = macro.Database
	}
	if dbOverride != "" {
		// Resolve Database from Payload
		if p := ai.GetSessionPayload(ctx); p != nil && p.Databases != nil {
			if val, ok := p.Databases[dbOverride]; ok {
				if d, ok := val.(*database.Database); ok {
					targetDB = d
					// Start Transaction for this macro scope
					var err error
					macroTx, err = targetDB.BeginTransaction(ctx, sop.ForWriting)
					if err != nil {
						return fmt.Errorf("failed to begin transaction for macro '%s' on db '%s': %w", name, dbOverride, err)
					}
					// Update Payload in Context
					newPayload := *p
					newPayload.CurrentDB = targetDB
					newPayload.Transaction = macroTx
					macroCtx = context.WithValue(ctx, "session_payload", &newPayload)
				} else {
					return fmt.Errorf("resolved database '%s' is not of type *database.Database", dbOverride)
				}
			} else {
				return fmt.Errorf("database '%s' not found in session payload", dbOverride)
			}
		} else {
			return fmt.Errorf("session payload or databases map not configured, cannot switch to db '%s'", dbOverride)
		}
	}

	err = s.executeMacro(macroCtx, macro.Steps, nestedScope, &nestedMu, sb, targetDB)

	if macroTx != nil {
		if err != nil {
			macroTx.Rollback(ctx)
		} else {
			if commitErr := macroTx.Commit(ctx); commitErr != nil {
				return fmt.Errorf("failed to commit transaction for macro '%s': %w", name, commitErr)
			}
		}
	}

	return err
}
