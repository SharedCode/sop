package agent

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop/ai"
)

func (a *DataAdminAgent) executeMacroView(ctx context.Context, macro ai.Macro, args map[string]any) (string, error) {
	// For a "View" macro, we expect it to have exactly one step which is a "select" command.
	if len(macro.Steps) == 0 {
		return "", fmt.Errorf("macro '%s' is empty", macro.Name)
	}

	// Let's handle the simple case: Single Select Step
	step := macro.Steps[0]
	if step.Type != "command" || step.Command != "select" {
		return "", fmt.Errorf("macro '%s' is not a simple select view (first step is %s:%s)", macro.Name, step.Type, step.Command)
	}

	// Merge Args
	// Macro args are the base.
	mergedArgs := make(map[string]any)
	for k, v := range step.Args {
		mergedArgs[k] = v
	}

	// Override/Augment with runtime args
	for k, v := range args {
		if k == "store" {
			continue
		}
		mergedArgs[k] = v
	}

	// Ensure database is set correctly
	if step.Database != "" {
		mergedArgs["database"] = step.Database
	}

	// Execute
	return a.toolSelect(ctx, mergedArgs)
}
