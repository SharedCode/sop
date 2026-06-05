package agent

import (
	"context"
	"testing"

	"github.com/sharedcode/sop/ai"
)

func TestToolSetVerboseUpdatesSessionPayload(t *testing.T) {
	ag := NewCopilotAgent(Config{}, nil, nil)
	p := &ai.SessionPayload{Verbose: false}
	ctx := context.WithValue(context.Background(), "session_payload", p)

	msg, err := ag.toolSetVerbose(ctx, map[string]any{"verbose": true})
	if err != nil {
		t.Fatalf("toolSetVerbose() error = %v", err)
	}
	if !p.Verbose {
		t.Fatalf("expected session payload verbosity to be enabled, got %q", msg)
	}
	if !ag.Config.Verbose {
		t.Fatalf("expected agent config verbosity to be enabled, got %q", msg)
	}
}
