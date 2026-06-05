package ai

import (
	"fmt"
	"strings"
)

// ValidateScript checks a slice of ScriptSteps for logical errors,
// such as using a variable before it has been declared.
func ValidateScript(steps []ScriptStep) error {
	return validateScript(steps, make(map[string]bool))
}

func validateScript(steps []ScriptStep, knownVars map[string]bool) error {
	for i, step := range steps {
		if err := checkInputs(step, knownVars); err != nil {
			return fmt.Errorf("step %d (%s): %w", i, step.Type, err)
		}

		// Add output variables from this step to the known set for subsequent steps.
		if step.OutputVariable != "" {
			knownVars[step.OutputVariable] = true
		}

		if len(step.Steps) > 0 {
			nestedVars := copyMap(knownVars)
			if step.Iterator != "" {
				nestedVars[step.Iterator] = true
			}
			if err := validateScript(step.Steps, nestedVars); err != nil {
				return err
			}
		}
		if len(step.Then) > 0 {
			branchVars := copyMap(knownVars)
			if err := validateScript(step.Then, branchVars); err != nil {
				return err
			}
		}
		if len(step.Else) > 0 {
			branchVars := copyMap(knownVars)
			if err := validateScript(step.Else, branchVars); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkInputs(step ScriptStep, knownVars map[string]bool) error {
	// Check 'input_var' style usage.
	if step.InputVariable != "" && !isVariableReference(step.InputVariable) {
		if _, ok := knownVars[step.InputVariable]; !ok {
			return fmt.Errorf("input variable '%s' used before declaration", step.InputVariable)
		}
	}

	// Check args values that might be variables.
	if step.Args != nil {
		for key, val := range step.Args {
			if strVal, ok := val.(string); ok && isVariableReference(strVal) {
				varName := strings.TrimPrefix(strVal, "@")
				if _, ok := knownVars[varName]; !ok {
					return fmt.Errorf("argument '%s' uses variable '%s' before declaration", key, varName)
				}
			}
		}
	}

	return nil
}

func isVariableReference(s string) bool {
	return strings.HasPrefix(s, "@")
}

func copyMap(m map[string]bool) map[string]bool {
	newMap := make(map[string]bool, len(m))
	for k, v := range m {
		newMap[k] = v
	}
	return newMap
}
