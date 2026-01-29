package scripts

import (
	"context"
	"fmt"
	"log/slog"
)

func init() {
	Register("SendEmail", SendEmailWrapper)
}

// SendEmailWrapper adapts the typed SendEmail to the ScriptFunc interface.
func SendEmailWrapper(ctx context.Context, args map[string]any) (any, error) {
	recipient, _ := args["recipient"].(string)
	subject, _ := args["subject"].(string)
	body, _ := args["body"].(string)

	if recipient == "" {
		return nil, fmt.Errorf("recipient is required")
	}

	return nil, SendEmail(recipient, subject, body)
}

// SendEmail sends an email to the recipient.
// This is currently a stub for demonstration.
func SendEmail(recipient string, subject string, body string) error {
	slog.Info("Sending Email", "to", recipient, "subject", subject)
	fmt.Printf("MOCK EMAIL SENT To: %s | Subject: %s | Body: %s\n", recipient, subject, body)
	return nil
}
