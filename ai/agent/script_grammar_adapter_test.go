package agent

import (
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestValidateScriptGrammarFromSteps_UsesSanitizeBeforeGrammar(t *testing.T) {
	steps := []ai.ScriptStep{
		{Type: "command", Command: "return", Args: map[string]any{"value": "ok"}},
		{Type: "command", Command: "commit_tx", Args: map[string]any{"transaction": "tx"}},
	}

	if err := validateScriptGrammarFromSteps(steps); err != nil {
		t.Fatalf("expected sanitize + grammar validation to accept this script, got: %v", err)
	}
}
