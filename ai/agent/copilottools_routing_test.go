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

func TestStripRoutingPrefix_DoesNotStripUnknownPrefix(t *testing.T) {
	got := stripRoutingPrefix("custom:language/c#/tutorial", "SOP")
	if got != "custom:language/c#/tutorial" {
		t.Fatalf("stripRoutingPrefix() = %q, want %q", got, "custom:language/c#/tutorial")
	}
}

func TestStripRoutingPrefix_StripsOnlyLeadingKBPrefixBeforeOMNI(t *testing.T) {
	got := stripRoutingPrefix("SOP:OMNI:language/c#/tutorial", "SOP")
	if got != "OMNI:language/c#/tutorial" {
		t.Fatalf("stripRoutingPrefix() = %q, want %q", got, "OMNI:language/c#/tutorial")
	}
}

func TestStripRoutingPrefix_RemovesRecognizedRoutingPatterns(t *testing.T) {
	tests := []struct {
		name  string
		query string
		kb    string
		want  string
	}{
		{name: "kb prefix", query: "SOP:mykb:language/c#/tutorial", kb: "mykb", want: "language/c#/tutorial"},
		{name: "omni prefix", query: "OMNI:language/c#/tutorial", kb: "SOP", want: "language/c#/tutorial"},
		{name: "omni sop kb prefix", query: "OMNI:SOP:mykb:language/c#/tutorial", kb: "mykb", want: "language/c#/tutorial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripRoutingPrefix(tt.query, tt.kb); got != tt.want {
				t.Fatalf("stripRoutingPrefix() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestStripRoutingPrefix_RemovesLeadingOmniSOPPrefixes(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "colon prefix", query: "omni:SOP:language bindings/c#", want: "language bindings/c#"},
		{name: "arrow prefix", query: "OMNI->SOP->language bindings/c#", want: "language bindings/c#"},
		{name: "slash prefix", query: "omni/SOP/language bindings/c#", want: "language bindings/c#"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripRoutingPrefix(tt.query, "other_space"); got != tt.want {
				t.Fatalf("stripRoutingPrefix() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLooksLikeCategoryPath_RecognizesSlashPaths(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{name: "simple path", query: "a/b", want: true},
		{name: "nested path", query: "a/b/c", want: true},
		{name: "dotdot path", query: "a/b/c/../n", want: true},
		{name: "kb prefix path", query: "SOP:language/c#/tutorial", want: true},
		{name: "plain text", query: "language bindings c#", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := looksLikeCategoryPath(tt.query); got != tt.want {
				t.Fatalf("looksLikeCategoryPath(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestExtractCategoryPathQuery_RecognizesHierarchicalPath(t *testing.T) {
	got := extractCategoryPathQuery("  SOP:language/c#/tutorial  ")
	if got != "language/c#/tutorial" {
		t.Fatalf("extractCategoryPathQuery() = %q, want %q", got, "language/c#/tutorial")
	}
}

func TestExtractCategoryPathQuery_PreservesLLMInstructionSuffix(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "plain path with llm suffix", query: "a/b/c:LLM extract Apple company", want: "a/b/c"},
		{name: "kb-prefixed path with llm suffix", query: "SOP:language/c#/tutorial:LLM summarize matches", want: "language/c#/tutorial"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractCategoryPathQuery(tt.query); got != tt.want {
				t.Fatalf("extractCategoryPathQuery(%q) = %q, want %q", tt.query, got, tt.want)
			}
		})
	}
}
