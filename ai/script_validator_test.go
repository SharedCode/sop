package ai

import (
	"testing"
)

func TestValidateScript_Valid(t *testing.T) {
	steps := []ScriptStep{
		{
			Type:           "command",
			Command:        "begin_tx",
			OutputVariable: "tx",
		},
		{
			Type:    "command",
			Command: "open_store",
			Args:    map[string]any{"name": "users", "transaction": "@tx"},
		},
	}
	if err := ValidateScript(steps); err != nil {
		t.Errorf("ValidateScript() returned an unexpected error: %v", err)
	}
}

func TestValidateScript_InvalidVariable(t *testing.T) {
	steps := []ScriptStep{
		{
			Type:    "command",
			Command: "open_store",
			Args:    map[string]any{"name": "users", "transaction": "@tx"},
		},
		{
			Type:           "command",
			Command:        "begin_tx",
			OutputVariable: "tx",
		},
	}
	err := ValidateScript(steps)
	if err == nil {
		t.Errorf("ValidateScript() should have returned an error for using a variable before declaration")
	}
	expected := "step 0 (command): argument 'transaction' uses variable 'tx' before declaration"
	if err.Error() != expected {
		t.Errorf("ValidateScript() returned wrong error message. got %q, want %q", err.Error(), expected)
	}
}

func TestValidateScript_InvalidInputVariable(t *testing.T) {
	steps := []ScriptStep{
		{
			Type:          "command",
			Command:       "filter",
			InputVariable: "user_list",
		},
		{
			Type:           "command",
			Command:        "scan",
			OutputVariable: "user_list",
		},
	}
	err := ValidateScript(steps)
	if err == nil {
		t.Errorf("ValidateScript() should have returned an error for using an input variable before declaration")
	}
	expected := "step 0 (command): input variable 'user_list' used before declaration"
	if err.Error() != expected {
		t.Errorf("ValidateScript() returned wrong error message. got %q, want %q", err.Error(), expected)
	}
}

func TestValidateScript_LoopVariableScope(t *testing.T) {
	steps := []ScriptStep{
		{
			Type:           "command",
			Command:        "scan",
			OutputVariable: "user_list",
		},
		{
			Type:     "loop",
			List:     "@user_list",
			Iterator: "user",
			Steps: []ScriptStep{
				{
					Type:    "command",
					Command: "inspect",
					Args:    map[string]any{"item": "@user"},
				},
			},
		},
	}
	if err := ValidateScript(steps); err != nil {
		t.Errorf("ValidateScript() returned an unexpected error for valid loop: %v", err)
	}
}

func TestValidateScript_LoopVariableOutOfScope(t *testing.T) {
	steps := []ScriptStep{
		{
			Type:           "command",
			Command:        "scan",
			OutputVariable: "user_list",
		},
		{
			Type:     "loop",
			List:     "@user_list",
			Iterator: "user",
			Steps:    []ScriptStep{},
		},
		{
			Type:    "command",
			Command: "inspect",
			Args:    map[string]any{"item": "@user"},
		},
	}
	err := ValidateScript(steps)
	if err == nil {
		t.Errorf("ValidateScript() should have returned an error for using a loop variable out of scope")
	}
}
