package agent

import "strings"

func shouldShortCircuitScriptOnError(command string, args map[string]any, err error) bool {
	if err == nil {
		return false
	}
	if isTransactionControlCommand(command, args) {
		return true
	}
	return isTransactionLifecycleError(err)
}

func isTransactionControlCommand(command string, args map[string]any) bool {
	switch strings.TrimSpace(strings.ToLower(command)) {
	case "manage_transaction":
		action, _ := args["action"].(string)
		switch strings.TrimSpace(strings.ToLower(action)) {
		case "begin", "commit", "rollback":
			return true
		}
	}
	return false
}

func isTransactionLifecycleError(err error) bool {
	msg := strings.ToLower(strings.TrimSpace(err.Error()))
	if msg == "" {
		return false
	}

	markers := []string{
		"failed to begin transaction",
		"failed to begin auto-transaction",
		"failed to begin implicit script transaction",
		"failed to begin global transaction",
		"failed to begin transaction on",
		"failed to commit transaction",
		"transaction commit failed",
		"failed to commit implicit script transaction",
		"failed to commit global transaction",
		"rollback failed",
		"commit failed:",
		"failed to auto-start new one",
		"operation 'begin_tx' failed",
		"operation 'commit_tx' failed",
		"operation 'rollback_tx' failed",
		"deferred operation failed",
	}

	for _, marker := range markers {
		if strings.Contains(msg, marker) {
			return true
		}
	}

	return false
}
