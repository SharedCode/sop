package agent

import (
	"context"
	"strings"
	"testing"
)

func TestToolSearchKB_RejectsMissingKBName(t *testing.T) {
	ag := &CopilotAgent{}

	_, err := ag.toolSearchKB(context.Background(), map[string]any{"query": "hello"})
	if err == nil {
		t.Fatal("toolSearchKB should require an explicit kb_name")
	}
	if !strings.Contains(err.Error(), "missing required argument 'kb_name'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestToolSearchKB_UsesExplicitKBName(t *testing.T) {
	ag := &CopilotAgent{}

	_, err := ag.toolSearchKB(context.Background(), map[string]any{"kb_name": "sop_docs", "query": "hello"})
	if err == nil {
		t.Fatal("toolSearchKB should fail when no DB is available for the explicit KB")
	}
	if !strings.Contains(err.Error(), "could not resolve DB for KB 'sop_docs'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestStripRoutingPrefix_RemovesMatchingKBPrefix(t *testing.T) {
	got := stripRoutingPrefix("  SOP: how do I create new b-tree  ", "SOP")
	want := "how do I create new b-tree"
	if got != want {
		t.Fatalf("stripRoutingPrefix() = %q, want %q", got, want)
	}
}

func TestStripRoutingPrefix_LeavesNonMatchingPrefixUntouched(t *testing.T) {
	got := stripRoutingPrefix("  SOP: how do I create new b-tree  ", "other_space")
	want := "SOP: how do I create new b-tree"
	if got != want {
		t.Fatalf("stripRoutingPrefix() = %q, want %q", got, want)
	}
}
