package agent

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop/ai"
)

func (a *DataAdminAgent) executeScriptView(ctx context.Context, name string, script ai.Script, args map[string]any) (string, error) {
	// For a "View" script, we expect it to have exactly one step which is a "select" command.
	if len(script.Steps) == 0 {
		return "", fmt.Errorf("script '%s' is empty", name)
	}

	// Let's handle the simple case: Single Select Step
	step := script.Steps[0]
	if step.Type != "command" || step.Command != "select" {
		return "", fmt.Errorf("script '%s' is not a simple select view (first step is %s:%s)", name, step.Type, step.Command)
	}

	// Merge Args
	// Script args are the base.
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
