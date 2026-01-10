package agent

import (
"fmt"
"testing"

"github.com/stretchr/testify/assert"
)

func TestReproSanitizeAndRender(t *testing.T) {
// 1. Sanitize Logic
rawFields := []string{"a.", "b.name AS employee"}
script := []ScriptInstruction{
{
Op: "project",
Args: map[string]interface{}{
"fields": rawFields,
},
},
}

sanitized := sanitizeScript(script)
pFields := sanitized[0].Args["fields"].([]ProjectionField)

fmt.Printf("Sanitized Fields: %+v\n", pFields)

assert.Equal(t, "a.*", pFields[0].Src)
assert.Equal(t, "a.*", pFields[0].Dst) // cleanName returns same for wildcard? Dst defaults to CleanName
	assert.Equal(t, "b.name", pFields[1].Src) // "b.name" -> "b.name" (preserved for alias lookup)
// 2. Render Logic
// Simulate Joined Data: 
// Left: {region: "US", department: "Sales"}
// Right: {region: "US", department: "Sales", name: "John"}
// Collisions handled (merged map)
merged := map[string]any{
"region": "US",
"department": "Sales",
"Right.region": "US",
"Right.department": "Sales",
"name": "John",
}

// Call renderItem
result := renderItem(nil, merged, pFields)
fmt.Printf("Render Result: %+v\n", result)

resMap := result.(*OrderedMap)
assert.NotNil(t, resMap)

// Access unexported field 'm' since we are in same package
val := resMap.m["region"]
assert.Equal(t, "US", val)

val = resMap.m["employee"]
assert.Equal(t, "John", val)
}
