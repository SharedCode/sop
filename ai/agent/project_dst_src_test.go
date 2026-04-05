package agent

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseProjectionFields_DstSrcFormat(t *testing.T) {
	// Test case 1: Single map with Dst and Src
	input1 := []any{
		map[string]any{
			"Dst": "alias_name",
			"Src": "source_field",
		},
	}
	result1 := parseProjectionFields(input1)
	assert.Len(t, result1, 1)
	assert.Equal(t, "alias_name", result1[0].Dst)
	assert.Equal(t, "source_field", result1[0].Src)

	// Test case 2: Mixed with existing format
	input2 := []any{
		map[string]any{
			"Dst": "col1",
			"Src": "table.col1",
		},
		map[string]any{
			"alias": "col2",
			"field": "table.col2",
		},
	}
	result2 := parseProjectionFields(input2)
	assert.Len(t, result2, 2)
	assert.Equal(t, "col1", result2[0].Dst)
	assert.Equal(t, "table.col1", result2[0].Src)
	assert.Equal(t, "col2", result2[1].Dst)
	assert.Equal(t, "table.col2", result2[1].Src)

	// Test case 3: Dst/Src mixed with implicit map style (Rule 2)
	input3 := []any{
		map[string]any{
			"Dst": "explicit_alias",
			"Src": "explicit_source",
		},
		map[string]any{
			"implicit_alias": "implicit_source",
		},
	}
	result3 := parseProjectionFields(input3)
	assert.Len(t, result3, 2)

	assert.Equal(t, "explicit_alias", result3[0].Dst)
	assert.Equal(t, "explicit_source", result3[0].Src)

	assert.Equal(t, "implicit_alias", result3[1].Dst)
	assert.Equal(t, "implicit_source", result3[1].Src)
}

func TestParseProjectionFields_DstSrc_Malformed(t *testing.T) {
	input := []any{
		map[string]any{
			"Dst": "only_dst",
		},
	}
	result := parseProjectionFields(input)

	// Expect Rule 2 validation
	assert.Len(t, result, 1)
	assert.Equal(t, "Dst", result[0].Dst)
	assert.Equal(t, "only_dst", result[0].Src)
}

// MockProjectCursor for testing ProjectCursor
type MockProjectCursor struct {
	Items []any
	Index int
}

func (m *MockProjectCursor) Next(ctx context.Context) (any, bool, error) {
	if m.Index >= len(m.Items) {
		return nil, false, nil
	}
	item := m.Items[m.Index]
	m.Index++
	return item, true, nil
}

func (m *MockProjectCursor) Close() error { return nil }

func TestProjectCursor_DstSrc_Execution(t *testing.T) {
	// 1. Setup Data
	mockData := []any{
		map[string]any{"users.first_name": "John", "users.last_name": "Doe", "age": 30},
	}
	cursor := &MockProjectCursor{Items: mockData}

	// 2. Setup ProjectCursor
	// Simulate parsing arguments: fields list with Dst/Src
	fields := parseProjectionFields([]any{
		map[string]any{"Dst": "First Name", "Src": "users.first_name"},
		map[string]any{"Dst": "Age", "Src": "age"},
	})

	pc := &ProjectCursor{
		source: cursor,
		fields: fields,
	}

	// 3. Execute Next
	res, ok, err := pc.Next(context.Background())
	assert.NoError(t, err)
	assert.True(t, ok)

	// 4. Verify Result
	// renderItem returns *OrderedMap
	om, ok := res.(*OrderedMap)
	assert.True(t, ok, "Result should be OrderedMap")

	val, exists := om.m["First Name"]
	assert.True(t, exists, "First Name should exist")
	assert.Equal(t, "John", val)

	valAge, existsAge := om.m["Age"]
	assert.True(t, existsAge, "Age should exist")
	assert.Equal(t, 30, valAge)
}
