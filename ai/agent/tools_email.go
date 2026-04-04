package agent

import (
	"context"
	"fmt"
)

// toolSendEmail simulates sending an email.
// In a production environment, this would integrate with an SMTP server or email API (e.g. SendGrid, AWS SES).
func (a *DataAdminAgent) toolSendEmail(ctx context.Context, args map[string]any) (string, error) {
	to, _ := args["to"].(string)
	subject, _ := args["subject"].(string)
	body, _ := args["body"].(string)

	if to == "" {
		return "", fmt.Errorf("argument 'to' is required")
	}
	// Subject and body can be optional or empty, but let's encourage them.
	if subject == "" {
		subject = "(No Subject)"
	}

	// Log to stdout to demonstrate the action in the CLI/Logs
	fmt.Printf("\n[System] Sending Email...\nTo: %s\nSubject: %s\nBody: %s\n", to, subject, body)

	return fmt.Sprintf("Email successfully sent to %s", to), nil
}
