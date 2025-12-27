package agent

import (
	"context"
	"fmt"
	"strings"
	"sync"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

// RunnerSession holds the state for the current agent execution session,
// including macro recording and transaction management.
type RunnerSession struct {
	Recording            bool
	RecordingMode        string // "standard" or "compiled"
	StopOnError          bool
	CurrentMacro         *ai.Macro
	CurrentMacroCategory string // Category for the macro being recorded
	Transaction          sop.Transaction
	CurrentDB            string         // The database the transaction is bound to
	Variables            map[string]any // Session-scoped variables (e.g. cached stores)
	LastStep             *ai.MacroStep
	// LastInteractionSteps tracks the number of steps added/executed in the last user interaction.
	LastInteractionSteps int
	// LastInteractionToolCalls buffers the tool calls from the last interaction for refactoring.
	LastInteractionToolCalls []ai.MacroStep
}

// NewRunnerSession creates a new runner session.
func NewRunnerSession() *RunnerSession {
	return &RunnerSession{
		RecordingMode: "standard",
	}
}

// handleSessionCommand processes session-related commands like /record, /play, etc.
// Returns (response, handled, error)
func (s *Service) handleSessionCommand(ctx context.Context, query string, db *database.Database) (string, bool, error) {
	// Handle Macro Management Commands
	if strings.HasPrefix(query, "/macro ") {
		resp, err := s.handleMacroCommand(ctx, query)
		return resp, true, err
	}

	// Handle Macro Commands
	if strings.HasPrefix(query, "/record ") {
		args := strings.Fields(strings.TrimPrefix(query, "/record "))
		if len(args) == 0 {
			return "Error: Macro name required", true, nil
		}

		mode := "standard"
		stopOnError := false
		force := false
		category := "general"

		// Parse arguments
		// /record [compiled] <name> [--stop-on-error] [--force] [--category <cat>]
		var cleanArgs []string
		for i := 0; i < len(args); i++ {
			arg := args[i]
			if arg == "--stop-on-error" {
				stopOnError = true
			} else if arg == "--force" {
				force = true
			} else if arg == "--category" {
				if i+1 < len(args) {
					category = args[i+1]
					i++
				}
			} else {
				cleanArgs = append(cleanArgs, arg)
			}
		}
		args = cleanArgs

		if len(args) == 0 {
			return "Error: Macro name required", true, nil
		}

		name := args[0]

		if args[0] == "compiled" {
			if len(args) < 2 {
				return "Error: Macro name required for compiled mode", true, nil
			}
			mode = "compiled"
			name = args[1]
			if len(args) > 2 {
				return fmt.Sprintf("Error: Too many arguments. Usage: /record compiled <name> [flags]. Found extra: %v", args[2:]), true, nil
			}
		} else {
			if len(args) > 1 {
				return fmt.Sprintf("Error: Too many arguments. Usage: /record <name> [flags]. Found extra: %v", args[1:]), true, nil
			}
		}

		// Check if macro exists
		macroDB := s.getMacroDB()
		if macroDB != nil {
			tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
			if err == nil {
				store, err := macroDB.OpenModelStore(ctx, "macros", tx)
				if err == nil {
					var dummy ai.Macro
					if err := store.Load(ctx, category, name, &dummy); err == nil {
						// Found!
						if !force {
							tx.Rollback(ctx)
							return fmt.Sprintf("Error: Macro '%s' (Category: %s) already exists. Use '/record %s --force' to overwrite.", name, category, name), true, nil
						}
					}
				}
				tx.Commit(ctx)
			}
		}

		s.session.Recording = true
		s.session.RecordingMode = mode
		s.session.StopOnError = stopOnError
		// Set macro.Database to current DB if available, else leave empty for composability
		var dbName string
		if p := ai.GetSessionPayload(ctx); p != nil {
			dbName = p.CurrentDB
		}

		log.Debug(fmt.Sprintf("database: %s", dbName))

		s.session.CurrentMacro = &ai.Macro{
			Name:     name,
			Database: dbName,
			Steps:    []ai.MacroStep{},
		}
		s.session.CurrentMacroCategory = category

		// We do NOT start a transaction here.
		// Recording mode uses "Auto-Commit per Step".
		// Each step will start and commit its own transaction.

		msg := fmt.Sprintf("Recording macro '%s' (Mode: %s)", name, mode)
		if stopOnError {
			msg += " [Stop on Error]"
		}
		if dbName == "System DB" {
			msg += "\nWarning: You are recording in 'System DB'. This macro will switch to 'System DB' when played."
		}
		return msg + "...", true, nil
	}

	if query == "/pause" {
		if s.session.CurrentMacro == nil {
			return "Error: No active macro recording", true, nil
		}
		s.session.Recording = false
		return "Recording paused.", true, nil
	}

	if query == "/resume" {
		if s.session.CurrentMacro == nil {
			return "Error: No active macro recording", true, nil
		}
		s.session.Recording = true
		return "Recording resumed.", true, nil
	}

	if query == "/stop" {
		if s.session.CurrentMacro == nil {
			return "Error: Not recording", true, nil
		}
		s.session.Recording = false
		macroDB := s.getMacroDB()
		if macroDB != nil {
			tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
			if err != nil {
				return fmt.Sprintf("Error starting transaction: %v", err), true, nil
			}
			store, err := macroDB.OpenModelStore(ctx, "macros", tx)
			if err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error opening store: %v", err), true, nil
			}

			log.Debug(fmt.Sprintf("saving macro w/ db: %s", s.session.CurrentMacro.Database))

			if err := store.Save(ctx, s.session.CurrentMacroCategory, s.session.CurrentMacro.Name, s.session.CurrentMacro); err != nil {
				tx.Rollback(ctx)
				return fmt.Sprintf("Error saving macro: %v", err), true, nil
			}
			if err := tx.Commit(ctx); err != nil {
				return fmt.Sprintf("Error committing transaction: %v", err), true, nil
			}
			msg := fmt.Sprintf("Macro '%s' (Category: %s) saved with %d steps.", s.session.CurrentMacro.Name, s.session.CurrentMacroCategory, len(s.session.CurrentMacro.Steps))

			// We do NOT commit any recording transaction here because we are in "Auto-Commit per Step" mode.
			// Any data changes were already committed during the step execution.
			s.session.Transaction = nil
			s.session.Variables = nil

			s.session.CurrentMacro = nil
			return msg, true, nil
		}
		s.session.CurrentMacro = nil
		return "Warning: No database configured, macro lost.", true, nil
	}

	if strings.HasPrefix(query, "/play ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/play "))
		if len(parts) == 0 {
			return "Error: Macro name required", true, nil
		}
		name := parts[0]
		category := "general"
		args := make(map[string]string)

		for i := 1; i < len(parts); i++ {
			arg := parts[i]
			if arg == "--category" && i+1 < len(parts) {
				category = parts[i+1]
				i++
				continue
			}
			kv := strings.SplitN(arg, "=", 2)
			if len(kv) == 2 {
				args[kv[0]] = kv[1]
			}
		}

		macroDB := s.getMacroDB()
		if macroDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}

		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		var macro ai.Macro
		if err := store.Load(ctx, category, name, &macro); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error loading macro: %v", err), true, nil
		}
		tx.Commit(ctx)

		var sb strings.Builder
		// sb.WriteString(fmt.Sprintf("Playing macro '%s'...\n", name))

		// Convert args to map[string]any
		scope := make(map[string]any)
		for k, v := range args {
			scope[k] = v
		}
		var scopeMu sync.RWMutex

		// Handle Database Switching for Macro
		var macroCtx context.Context = ctx

		// Initialize streamer for structured output
		streamer := NewJSONStreamer(&sb)
		sb.WriteString("[\n") // Start JSON array
		macroCtx = context.WithValue(macroCtx, CtxKeyJSONStreamer, streamer)

		if macro.Database != "" && !macro.Portable {
			// Resolve Database from Service Options
			if opts, ok := s.databases[macro.Database]; ok {
				targetDB := database.NewDatabase(opts)

				// Update Payload in Context
				if p := ai.GetSessionPayload(ctx); p != nil {
					newPayload := *p
					newPayload.CurrentDB = macro.Database
					newPayload.Transaction = nil // Ensure Open starts a new one
					macroCtx = context.WithValue(macroCtx, "session_payload", &newPayload)

					// Update local db var for executeMacro
					db = targetDB
				}
			} else {
				return fmt.Sprintf("Error: Macro '%s' requires database '%s' which is not configured.", name, macro.Database), true, nil
			}
		}

		// Lifecycle Management for Macro Execution
		if err := s.Open(macroCtx); err != nil {
			return fmt.Sprintf("Error initializing session: %v", err), true, nil
		}
		// Ensure we close the session (commit transaction)
		defer func() {
			if err := s.Close(macroCtx); err != nil {
				sb.WriteString(fmt.Sprintf("\nError closing session: %v", err))
			}
		}()

		if err := s.executeMacro(macroCtx, &macro, scope, &scopeMu, &sb, db); err != nil {
			errMsg := fmt.Sprintf("Error executing macro: %v", err)

			// Also add to streamer
			streamer.Write(StepExecutionResult{
				Type:  "error",
				Error: errMsg,
			})

			// If error, we might want to rollback.
			// Currently Close() commits.
			// We should probably rollback here if we can access the transaction.
			if p := ai.GetSessionPayload(macroCtx); p != nil && p.Transaction != nil {
				if tx, ok := p.Transaction.(sop.Transaction); ok {
					tx.Rollback(macroCtx)
					p.Transaction = nil // Prevent Close from committing
				}
			}
		}

		sb.WriteString("\n]") // End JSON array
		return sb.String(), true, nil
	}

	if strings.HasPrefix(query, "/delete ") {
		parts := strings.Fields(strings.TrimPrefix(query, "/delete "))
		if len(parts) == 0 {
			return "Error: Macro name required", true, nil
		}
		name := parts[0]
		category := "general"

		for i := 1; i < len(parts); i++ {
			if parts[i] == "--category" && i+1 < len(parts) {
				category = parts[i+1]
				i++
			}
		}

		macroDB := s.getMacroDB()
		if macroDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}

		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		var dummy ai.Macro
		if err := store.Load(ctx, category, name, &dummy); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error: Macro '%s' (Category: %s) not found.", name, category), true, nil
		}

		if err := store.Delete(ctx, category, name); err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error deleting macro: %v", err), true, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Sprintf("Error committing transaction: %v", err), true, nil
		}
		return fmt.Sprintf("Macro '%s' (Category: %s) deleted.", name, category), true, nil
	}

	if strings.HasPrefix(query, "/list") {
		args := strings.Fields(query)
		category := "general"
		for i := 1; i < len(args); i++ {
			if args[i] == "--category" && i+1 < len(args) {
				category = args[i+1]
				i++
			}
		}

		macroDB := s.getMacroDB()
		if macroDB == nil {
			return "Error: No database configured", true, nil
		}

		tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
		if err != nil {
			return fmt.Sprintf("Error starting transaction: %v", err), true, nil
		}

		store, err := macroDB.OpenModelStore(ctx, "macros", tx)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error opening store: %v", err), true, nil
		}

		names, err := store.List(ctx, category)
		if err != nil {
			tx.Rollback(ctx)
			return fmt.Sprintf("Error listing macros: %v", err), true, nil
		}
		tx.Commit(ctx)

		var sb strings.Builder
		sb.WriteString(fmt.Sprintf("Available Macros (Category: %s):\n", category))
		for _, n := range names {
			sb.WriteString(fmt.Sprintf("- %s\n", n))
		}
		return sb.String(), true, nil
	}

	return "", false, nil
}

func (s *Service) handleMacroCommand(ctx context.Context, query string) (string, error) {
	args := strings.Fields(strings.TrimPrefix(query, "/macro "))
	if len(args) == 0 {
		return "Usage: /macro <list|show|delete|step> ...", nil
	}

	cmd := args[0]
	macroDB := s.getMacroDB()
	if macroDB == nil {
		return "Error: No database configured", nil
	}

	switch cmd {
	case "list":
		return s.macroList(ctx, macroDB, args)

	case "show":
		return s.macroShow(ctx, macroDB, args)

	case "delete":
		return s.macroDelete(ctx, macroDB, args)

	case "save_as":
		return s.macroSaveAs(ctx, macroDB, args)

	case "step":
		return s.macroStep(ctx, macroDB, args)

	default:
		return "Unknown macro command. Usage: /macro <list|show|delete|step> ...", nil
	}
}
