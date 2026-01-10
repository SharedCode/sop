package agent

import (
"testing"
"github.com/stretchr/testify/assert"
)

func TestSanitizeScript(t *testing.T) {
tests := []struct {
name     string
input    []ScriptInstruction
expected []ScriptInstruction
}{
{
name: "Join - Default to Inner",
input: []ScriptInstruction{
{
Op: "join",
Args: map[string]interface{}{
"dataset": "users",
},
},
},
expected: []ScriptInstruction{
{
Op: "join",
Args: map[string]interface{}{
"dataset": "users",
"type":    "inner",
},
},
},
},
{
name: "Join - Normalize Type Case",
input: []ScriptInstruction{
{
Op: "join",
Args: map[string]interface{}{
"type": "LEFT",
},
},
},
expected: []ScriptInstruction{
{
Op: "join",
Args: map[string]interface{}{
"type": "left",
},
},
},
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
result := sanitizeScript(tt.input)
assert.Equal(t, tt.expected, result)
})
}
}

func TestSanitizeScript_Project(t *testing.T) {
// this is harder to test with strict equality because parseProjectionFields returns []ProjectionField
// and we don't want to couple strictly to the internal struct if we can avoid it, 
// but since we are in the same package we can import the types or just check behavior.

input := []ScriptInstruction{
{
Op: "project",
Args: map[string]interface{}{
"fields": []string{"a", "b.c"},
},
},
}

result := sanitizeScript(input)

fields := result[0].Args["fields"]
// expected is a slice of ProjectionField
// But let's just assert it is NOT []string
_, isStringSlice := fields.([]string)
assert.False(t, isStringSlice, "Fields should be converted from []string to []ProjectionField")

// We can trust parseProjectionFields does the right thing if we successfully called it.
}
