package agent

import (
	"context"
	"strings"
	"testing"
)

func TestBuildAvatarPrompt(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)
	agent.registerTools(context.Background()) // Populate registry with default tools

	avatarName := "test_expert"
	taskContext := "Analyze the database schema"
	customPersona := "You are a database testing Avatar."
	allowedTools := []string{"list_databases"}

	prompt := agent.buildAvatarPrompt(context.Background(), avatarName, taskContext, customPersona, allowedTools)

	// Asserts persona injection
	if !strings.Contains(prompt, "You are a database testing Avatar.") {
		t.Errorf("Expected custom persona injection, but got: %s", prompt)
	}

	// Asserts Task Context injection
	if !strings.Contains(prompt, "Task Context from Omni Supervisor:\nAnalyze the database schema") {
		t.Errorf("Expected task context injection, but got: %s", prompt)
	}

	// Asserts Tool injection
	if !strings.Contains(prompt, "ONLY use these allowed tools:") {
		t.Errorf("Expected allowed tools section, but got: %s", prompt)
	}
	if !strings.Contains(prompt, "- list_databases") {
		t.Errorf("Expected list_databases tool definition, but got: %s", prompt)
	}

	// Make sure it doesn't contain forbidden tools
	if strings.Contains(prompt, "- execute_shell_command") {
		t.Errorf("Avatar prompt should not contain unapproved tool execution instructions")
	}
}

func TestBuildAvatarPrompt_DefaultPersona(t *testing.T) {
	agent := NewCopilotAgent(Config{}, nil, nil)

	avatarName := "blank_expert"
	taskContext := "Do something blank"

	prompt := agent.buildAvatarPrompt(context.Background(), avatarName, taskContext, "", nil)

	if !strings.Contains(prompt, "You are the blank_expert Avatar. Your task is strictly limited to your domain.") {
		t.Errorf("Expected default persona fallback, but got: %s", prompt)
	}

	// Should not have the tool section if no allowed tools are configured
	if strings.Contains(prompt, "ONLY use these allowed tools:") {
		t.Errorf("Expected no tool instruction for empty tools array, but got: %s", prompt)
	}
}
