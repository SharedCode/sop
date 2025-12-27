package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

func (s *Service) macroList(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	category := "general"
	for i := 1; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		}
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	names, err := store.List(ctx, category)
	tx.Commit(ctx)
	if err != nil {
		return fmt.Sprintf("Error listing macros: %v", err), nil
	}
	if len(names) == 0 {
		return fmt.Sprintf("No macros found in category '%s'.", category), nil
	}
	return fmt.Sprintf("Macros (Category: %s):\n- %s", category, strings.Join(names, "\n- ")), nil
}

func (s *Service) macroShow(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 2 {
		return "Usage: /macro show <name> [--json] [--category <cat>]", nil
	}
	name := args[1]
	category := "general"
	showJSON := false

	for i := 2; i < len(args); i++ {
		if args[i] == "--json" {
			showJSON = true
		} else if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		}
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	err = store.Load(ctx, category, name, &macro)
	tx.Commit(ctx)
	if err != nil {
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	if showJSON {
		b, err := json.MarshalIndent(macro, "", "  ")
		if err != nil {
			return fmt.Sprintf("Error marshaling macro: %v", err), nil
		}
		return fmt.Sprintf("```json\n%s\n```", string(b)), nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Macro: %s\n", macro.Name))
	for i, step := range macro.Steps {
		desc := step.Message
		if step.Type == "ask" {
			desc = step.Prompt
		} else if step.Type == "macro" {
			desc = fmt.Sprintf("Run '%s'", step.MacroName)
		} else if step.Type == "command" {
			argsJSON, _ := json.Marshal(step.Args)
			desc = fmt.Sprintf("Execute '%s' %s", step.Command, string(argsJSON))
		}
		sb.WriteString(fmt.Sprintf("%d. [%s] %s", i+1, step.Type, desc))
		sb.WriteString("\n")
	}
	return sb.String(), nil
}

func (s *Service) macroDelete(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 2 {
		return "Usage: /macro delete <name> [--category <cat>]", nil
	}
	name := args[1]
	category := "general"
	if len(args) > 3 && args[2] == "--category" {
		category = args[3]
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}

	var dummy ai.Macro
	if err := store.Load(ctx, category, name, &dummy); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error: Macro '%s' (Category: %s) not found.", name, category), nil
	}

	err = store.Delete(ctx, category, name)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error deleting macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Macro '%s' (Category: %s) deleted.", name, category), nil
}

func (s *Service) macroSaveAs(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 2 {
		return "Usage: /macro save_as <name> [--category <cat>]", nil
	}
	name := args[1]
	category := "general"
	if len(args) > 3 && args[2] == "--category" {
		category = args[3]
	}

	if s.session.LastStep == nil {
		return "Error: No previous step available to save. Run a command first.", nil
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}

	// Check if macro exists
	var dummy ai.Macro
	if err := store.Load(ctx, category, name, &dummy); err == nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error: Macro '%s' (Category: %s) already exists. Use '/macro delete %s' first.", name, category, name), nil
	}

	newMacro := ai.Macro{
		Name:  name,
		Steps: []ai.MacroStep{*s.session.LastStep},
	}

	if err := store.Save(ctx, category, name, newMacro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Macro '%s' (Category: %s) created from last step.", name, category), nil
}

func (s *Service) macroStep(ctx context.Context, macroDB *database.Database, args []string) (string, error) {
	if len(args) < 3 {
		return "Usage: /macro step <add|delete|update> <macro_name> ... [--category <cat>]", nil
	}
	subCmd := args[1]
	name := args[2]

	category := "general"
	var cleanArgs []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--category" && i+1 < len(args) {
			category = args[i+1]
			i++
		} else {
			cleanArgs = append(cleanArgs, args[i])
		}
	}
	args = cleanArgs

	if subCmd == "add" {
		return s.macroStepAdd(ctx, macroDB, name, category, args)
	}

	if subCmd == "delete" {
		return s.macroStepDelete(ctx, macroDB, name, category, args)
	}

	if subCmd == "update" {
		return s.macroStepUpdate(ctx, macroDB, name, category, args)
	}

	return "Unknown step command. Usage: /macro step <delete|add|update> ...", nil
}

func (s *Service) macroStepAdd(ctx context.Context, macroDB *database.Database, name string, category string, args []string) (string, error) {
	// /macro step add <macro_name> <position> [target_index]
	if len(args) < 4 {
		return "Usage: /macro step add <macro_name> <position> [target_index]", nil
	}
	if s.session.LastStep == nil {
		return "Error: No previous step available to add. Run a command first.", nil
	}

	position := args[3]
	targetIdx := -1
	if position == "before" || position == "after" {
		if len(args) < 5 {
			return "Usage: /macro step add <macro_name> <before|after> <target_index>", nil
		}
		var err error
		targetIdx, err = strconv.Atoi(args[4])
		if err != nil || targetIdx < 1 {
			return "Error: Invalid target index", nil
		}
		targetIdx-- // 0-based
	}

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	newStep := *s.session.LastStep

	switch position {
	case "top":
		macro.Steps = append([]ai.MacroStep{newStep}, macro.Steps...)
	case "bottom":
		macro.Steps = append(macro.Steps, newStep)
	case "before":
		if targetIdx >= len(macro.Steps) {
			tx.Rollback(ctx)
			return "Error: Target index out of range", nil
		}
		macro.Steps = append(macro.Steps[:targetIdx], append([]ai.MacroStep{newStep}, macro.Steps[targetIdx:]...)...)
	case "after":
		if targetIdx >= len(macro.Steps) {
			tx.Rollback(ctx)
			return "Error: Target index out of range", nil
		}
		// Insert after targetIdx (so at targetIdx + 1)
		targetIdx++
		macro.Steps = append(macro.Steps[:targetIdx], append([]ai.MacroStep{newStep}, macro.Steps[targetIdx:]...)...)
	default:
		tx.Rollback(ctx)
		return "Error: Invalid position. Use top, bottom, before, or after.", nil
	}

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Step added to macro '%s' (Category: %s) at %s.", name, category, position), nil
}

func (s *Service) macroStepDelete(ctx context.Context, macroDB *database.Database, name string, category string, args []string) (string, error) {
	if len(args) < 4 {
		return "Usage: /macro step delete <macro_name> <step_index>", nil
	}
	idxStr := args[3]
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 1 {
		return "Error: Invalid step index", nil
	}
	// Adjust to 0-based
	idx--

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	if idx >= len(macro.Steps) {
		tx.Rollback(ctx)
		return "Error: Step index out of range", nil
	}

	// Remove step
	macro.Steps = append(macro.Steps[:idx], macro.Steps[idx+1:]...)

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Step %d deleted from macro '%s' (Category: %s).", idx+1, name, category), nil
}

func (s *Service) macroStepUpdate(ctx context.Context, macroDB *database.Database, name string, category string, args []string) (string, error) {
	// /macro step update <macro_name> <step_index>
	if len(args) < 4 {
		return "Usage: /macro step update <macro_name> <step_index>", nil
	}
	if s.session.LastStep == nil {
		return "Error: No previous step available to update with. Run a command first.", nil
	}

	idxStr := args[3]
	idx, err := strconv.Atoi(idxStr)
	if err != nil || idx < 1 {
		return "Error: Invalid step index", nil
	}
	idx-- // 0-based

	tx, err := macroDB.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		return fmt.Sprintf("Error starting transaction: %v", err), nil
	}
	store, err := macroDB.OpenModelStore(ctx, "macros", tx)
	if err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error opening store: %v", err), nil
	}
	var macro ai.Macro
	if err := store.Load(ctx, category, name, &macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error loading macro: %v", err), nil
	}

	if idx >= len(macro.Steps) {
		tx.Rollback(ctx)
		return "Error: Step index out of range", nil
	}

	// Update step
	macro.Steps[idx] = *s.session.LastStep

	if err := store.Save(ctx, category, name, macro); err != nil {
		tx.Rollback(ctx)
		return fmt.Sprintf("Error saving macro: %v", err), nil
	}
	tx.Commit(ctx)
	return fmt.Sprintf("Step %d updated in macro '%s' (Category: %s).", idx+1, name, category), nil
}
