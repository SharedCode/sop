package main

import (
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/memory"
)

func TestBuildExportItems_IncludesLeafCategoryForUI(t *testing.T) {
	catGraphMap = make(map[string]*memory.Category)
	catDescriptions = make(map[string]string)

	items := buildExportItems([]KnowledgeChunk{{
		ID:          "chunk-1",
		Category:    "Root / Child",
		Text:        "compiled text",
		Description: "compiled description",
		DocumentID:  sop.NewUUID(),
	}})

	if len(items) != 1 {
		t.Fatalf("expected 1 export item, got %d", len(items))
	}

	data := items[0].Data
	if got := data["category"]; got != "Child" {
		t.Fatalf("expected leaf category 'Child', got %v", got)
	}
	if got := data["category_path"]; got != "Root / Child" {
		t.Fatalf("expected category path 'Root / Child', got %v", got)
	}
	if items[0].CategoryPath == "" {
		t.Fatal("expected category path id to be populated")
	}
}
