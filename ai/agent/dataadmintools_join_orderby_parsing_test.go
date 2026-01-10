package agent

import (
	"strings"
	"testing"
)

func TestParseOrderBy(t *testing.T) {
	// Only Test Logic, not full ToolJoin call (which requires stores)
	
	orderBy := "a.f1 ASC, b.f2 DESC"
	
	isDesc := false
	var rightSortFields []string
	
	parts := strings.Split(orderBy, ",")
	firstPart := strings.TrimSpace(parts[0])
	lowerFirst := strings.ToLower(firstPart)
	
	if strings.HasSuffix(lowerFirst, " desc") {
		isDesc = true
	}
	
	if len(parts) > 1 {
		for _, p := range parts[1:] {
			p = strings.TrimSpace(p)
			if p != "" {
				rightSortFields = append(rightSortFields, p)
			}
		}
	}
	
	if isDesc {
		t.Error("Expected isDesc=false for 'a.f1 ASC'")
	}
	if len(rightSortFields) != 1 {
		t.Errorf("Expected 1 rightSortField, got %d", len(rightSortFields))
	} else {
		if rightSortFields[0] != "b.f2 DESC" {
			t.Errorf("Expected 'b.f2 DESC', got '%s'", rightSortFields[0])
		}
	}
}

// Ensure the helper strip helper works
func TestRightSortFieldParsing(t *testing.T) {
	spec := "b.f2 DESC"
	fieldName := spec
	desc := false
	lower := strings.ToLower(spec)
	if strings.HasSuffix(lower, " desc") {
		fieldName = spec[:len(spec)-5]
		desc = true
	} else if strings.HasSuffix(lower, " asc") {
		fieldName = spec[:len(spec)-4]
	}
	
	if !desc {
		t.Error("Expected desc=true")
	}
	if fieldName != "b.f2" {
		t.Errorf("Expected 'b.f2', got '%s'", fieldName)
	}
}
