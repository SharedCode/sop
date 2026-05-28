package agent

import (
	"context"
	"encoding/json"
	"fmt"
)

// toolExecuteLocalCommand equips the LLM to orchestrate shell commands on the user's local machine.
// Instead of running the command on the server, it returns an action payload that the UI interprets
// to communicate with the local SOP Desktop Companion daemon.

func (a *CopilotAgent) registerAutomationTools(ctx context.Context) {
	a.registry.Register("execute_local_command", "Executes a shell command on the user's local machine via the SOP Desktop Daemon. Use this for building code, git operations, local file inspection, or running local scripts. This will prompt the user for approval in the UI.", "(command: string, reasoning: string)", a.toolExecuteLocalCommand)

	a.registry.Register("send_email", "Sends an email.", "(to: string, subject: string, body: string)", a.toolSendEmail)
}
func (a *CopilotAgent) toolExecuteLocalCommand(ctx context.Context, args map[string]any) (string, error) {
	commandRaw, ok := args["command"]
	if !ok {
		return "", fmt.Errorf("missing required argument 'command'")
	}
	command, ok := commandRaw.(string)
	if !ok {
		return "", fmt.Errorf("'command' must be a string")
	}

	reasoningRaw, _ := args["reasoning"]
	reasoning, _ := reasoningRaw.(string)

	payload := map[string]any{
		"action_type": "local_shell_execution",
		"command":     command,
		"reasoning":   reasoning,
	}

	payloadBytes, _ := json.Marshal(payload)

	// Return a structured marker. The REST endpoint or frontend UI will catch this
	// specific format, pause the chat loop, and prompt the user to approve sending
	// it to their local machine's daemon. Once executed locally, the UI will send
	// the terminal output back as the next user chat message.
	return fmt.Sprintf("<<<ACTION_REQUIRED:%s>>>", string(payloadBytes)), nil
}
