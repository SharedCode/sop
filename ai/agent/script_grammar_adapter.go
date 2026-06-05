package agent

import "github.com/sharedcode/sop/ai"

// SanitizeScript exposes the execution-path sanitization pass for reuse.
func SanitizeScript(script []ScriptInstruction) []ScriptInstruction {
	return sanitizeScript(script)
}

// ValidateScriptGrammar exposes the execution-path grammar validation pass for reuse.
func ValidateScriptGrammar(script []ScriptInstruction) error {
	return validateScriptGrammar(script)
}

// ValidateScriptSteps converts []ai.ScriptStep to the internal execution form,
// applies the same sanitize-before-grammar path used by execute_script,
// and returns grammar validation errors.
func ValidateScriptSteps(steps []ai.ScriptStep) error {
	instructions := make([]ScriptInstruction, 0, len(steps))
	for _, step := range steps {
		op := step.Type
		if step.Command != "" {
			op = step.Command
		}

		instr := ScriptInstruction{
			Name:      step.Name,
			Op:        op,
			Args:      cloneScriptArgs(step.Args),
			InputVar:  step.InputVariable,
			ResultVar: step.OutputVariable,
		}
		if step.Type == "command" && step.Command != "" {
			instr.Op = step.Command
		}
		instructions = append(instructions, instr)
	}

	instructions = SanitizeScript(instructions)
	return ValidateScriptGrammar(instructions)
}

func validateScriptGrammarFromSteps(steps []ai.ScriptStep) error {
	return ValidateScriptSteps(steps)
}

func cloneScriptArgs(args map[string]any) map[string]any {
	if len(args) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(args))
	for k, v := range args {
		cloned[k] = v
	}
	return cloned
}
