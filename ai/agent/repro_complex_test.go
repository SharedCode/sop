package agent

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRepro_Complex_DuplicateFields(t *testing.T) {
	// 1. Simulate Data
	// Left (Department)
	leftData := map[string]any{
		"department": "HR",
		"region":     "APAC",
		"director":   "Joe Petit",
	}
	// Right (Employees) - "active" is unique to Right
	rightData := map[string]any{
		"name":       "Employee 14",
		"department": "HR",   // Collision
		"region":     "APAC", // Collision
		"active":     true,   // Unique
		"salary":     79115,  // Unique
	}

	// 2. Simulate JoinRight with alias "b"
	jc := &JoinRightCursor{
		rightStoreName: "b",
	}

	// NOTE: We are testing mergeResult behavior directly
	// mergeResult is internal, so we assume this test is in 'agent' package
	res := jc.mergeResult(leftData, rightData, "someKey")
	joinedMap := res.(*OrderedMap)

	// 3. Simulate Project: ["a.", "b.name as employee"]
	// This corresponds to "select a.*, b.name as employee"
	// (Assuming "a." is interpreted as "a.*" by renderItem/parseProjectionFields)

	fields := []string{"a.", "b.name as employee"}

	// We use renderItem to project the joined map
	output := renderItem(nil, joinedMap, fields)
	outMap := output.(*OrderedMap)

	// 4. Validate Output
	// Expected:
	// - department (from Left)
	// - region (from Left)
	// - director (from Left)
	// - employee ("Employee 14")

	// UNDESIRED / BUG:
	// - active (from Right, if leaked as naked)
	// - salary (from Right, if leaked as naked)
	// - b.active, b.salary (if not filtered by a.*)

	t.Logf("Output Keys: %v", outMap.keys)

	// Check for duplicates or pollution
	assert.Contains(t, outMap.m, "department")
	assert.Contains(t, outMap.m, "region")
	assert.Contains(t, outMap.m, "director")
	assert.Contains(t, outMap.m, "employee")

	// Check pollution from Right unique fields into 'a.*'
	// 'active' is unique to Right. 'a.*' should NOT pick it up.
	_, hasActive := outMap.m["active"]
	assert.False(t, hasActive, "Left wildcard 'a.*' should NOT include Right-unique field 'active'")

	_, hasSalary := outMap.m["salary"]
	assert.False(t, hasSalary, "Left wildcard 'a.*' should NOT include Right-unique field 'salary'")

	// Check for 'b.' prefixed fields
	// 'a.*' should exclude 'b.*'
	_, hasBActive := outMap.m["b.active"]
	assert.False(t, hasBActive, "Left wildcard 'a.*' should exclude 'b.active'")
}

func TestRepro_SelectNaked_RecursiveFallback(t *testing.T) {
	// Verify that if we remove naked keys from the map,
	// specific "select active" still works via fallback lookup.

	joinedMap := &OrderedMap{
		m: map[string]any{
			"department": "HR", // Left
			"b.active":   true, // Right (Aliased)
			// "active": true,     // MISSING (Intentionally not added)
		},
		keys: []string{"department", "b.active"},
	}

	// Project "active" (naked)
	fields := []string{"active"}

	output := renderItem(nil, joinedMap, fields)
	outMap := output.(*OrderedMap)

	val, ok := outMap.m["active"]
	assert.True(t, ok, "Should find 'active' via fallback resolution to 'b.active'")
	assert.Equal(t, true, val)
}
