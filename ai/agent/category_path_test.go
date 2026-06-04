package agent

import (
	"testing"
)

// TestExtractCategoryPath validates category path extraction from queries
func TestExtractCategoryPath(t *testing.T) {
	tests := []struct {
		name        string
		query       string
		wantPath    string
		wantClean   string
		description string
	}{
		{
			name:        "bracketed_path",
			query:       "[Database/Architecture] explain B-tree structure",
			wantPath:    "Database/Architecture",
			wantClean:   "explain B-tree structure",
			description: "Bracketed category path at start of query",
		},
		{
			name:        "path_prefix",
			query:       "path:System/Tools list available commands",
			wantPath:    "System/Tools",
			wantClean:   "list available commands",
			description: "Category path with 'path:' prefix",
		},
		{
			name:        "in_prefix",
			query:       "in Engineering/API: how to create endpoints",
			wantPath:    "Engineering/API",
			wantClean:   "how to create endpoints",
			description: "Category path with 'in' prefix and colon",
		},
		{
			name:        "arrow_separator",
			query:       "[System > Tools > Advanced] configuration options",
			wantPath:    "System/Tools/Advanced",
			wantClean:   "configuration options",
			description: "Category path with arrow separator",
		},
		{
			name:        "no_path",
			query:       "what is the B-tree implementation?",
			wantPath:    "",
			wantClean:   "what is the B-tree implementation?",
			description: "Normal query without category path",
		},
		{
			name:        "bracketed_no_slash",
			query:       "[SingleWord] query here",
			wantPath:    "",
			wantClean:   "[SingleWord] query here",
			description: "Bracketed text without slash is not a path",
		},
		{
			name:        "empty_clean_query",
			query:       "[Database/Performance]",
			wantPath:    "Database/Performance",
			wantClean:   "",
			description: "Path only, no query text",
		},
		{
			name:        "nested_deep_path",
			query:       "path:SOP/Documentation/Architecture/Storage search for consistency guarantees",
			wantPath:    "SOP/Documentation/Architecture/Storage",
			wantClean:   "search for consistency guarantees",
			description: "Deep nested category path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPath, gotClean := extractCategoryPath(tt.query)

			if gotPath != tt.wantPath {
				t.Errorf("extractCategoryPath() path = %q, want %q", gotPath, tt.wantPath)
			}
			if gotClean != tt.wantClean {
				t.Errorf("extractCategoryPath() clean = %q, want %q", gotClean, tt.wantClean)
			}

			t.Logf("✓ %s: %q → path=%q, clean=%q", tt.description, tt.query, gotPath, gotClean)
		})
	}
}

// TestNormalizeCategoryPath validates category path normalization
func TestNormalizeCategoryPath(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		want        string
		description string
	}{
		{
			name:        "arrow_to_slash",
			input:       "Category > Subcategory",
			want:        "Category/Subcategory",
			description: "Convert arrow separator to slash",
		},
		{
			name:        "multiple_arrows",
			input:       "A > B > C > D",
			want:        "A/B/C/D",
			description: "Convert multiple arrows",
		},
		{
			name:        "double_slash",
			input:       "Category//Subcategory",
			want:        "Category/Subcategory",
			description: "Collapse double slashes",
		},
		{
			name:        "already_normalized",
			input:       "Database/Architecture/BTrees",
			want:        "Database/Architecture/BTrees",
			description: "Already normalized path unchanged",
		},
		{
			name:        "mixed_separators",
			input:       "Root > Sub/Category > Final",
			want:        "Root/Sub/Category/Final",
			description: "Mixed arrow and slash separators",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeCategoryPath(tt.input)
			if got != tt.want {
				t.Errorf("normalizeCategoryPath() = %q, want %q", got, tt.want)
			}
			t.Logf("✓ %s: %q → %q", tt.description, tt.input, got)
		})
	}
}
