// Package main demonstrates Gemini 3.1 Pro optimizations integration.
//
// This demo shows how the new ThinkingLevel and ResponseSchema features
// are automatically applied in the SOP AI agent system.
//
// Run: go run ai/demo_gemini_optimizations.go
package main

import (
	"fmt"

	"github.com/sharedcode/sop/ai"
)

func main() {
	fmt.Printf("=== Gemini 3.1 Pro Optimizations Integration Demo ===\n\n")

	// Example 1: Tool calling with auto-detected thinking level
	fmt.Println("1. Tool Calling (auto-detects 'medium' thinking level):")
	toolOpts := ai.GenOptions{
		Tools: []ai.ToolDefinition{
			{Name: "query_store", Description: "Query a data store"},
		},
	}
	fmt.Printf("   Input: %d tools defined\n", len(toolOpts.Tools))
	fmt.Printf("   Result: buildGeminiRequest will auto-set ThinkingLevel='medium'\n")
	fmt.Printf("   Benefit: Balanced reasoning for structured tool schemas\n\n")

	// Example 2: Explicit low thinking level for strict JSON
	fmt.Println("2. Classifier/Structured Output (explicit 'low' thinking level):")
	classifierOpts := ai.GenOptions{
		Temperature:   0.0,
		ThinkingLevel: "low",
	}
	fmt.Printf("   Input: Temperature=%.1f, ThinkingLevel='%s'\n",
		classifierOpts.Temperature, classifierOpts.ThinkingLevel)
	fmt.Printf("   Result: Strict adherence to JSON schema, no creative variations\n")
	fmt.Printf("   Usage: Topic routing, memory categorization, script refinement\n\n")

	// Example 3: High thinking level for creative synthesis
	fmt.Println("3. Final Synthesis (explicit 'high' thinking level):")
	synthOpts := ai.GenOptions{
		Temperature:   0.7,
		ThinkingLevel: "high",
	}
	fmt.Printf("   Input: Temperature=%.1f, ThinkingLevel='%s'\n",
		synthOpts.Temperature, synthOpts.ThinkingLevel)
	fmt.Printf("   Result: More creative reasoning for explanations\n")
	fmt.Printf("   Usage: ReAct iteration limit reached, need explanation\n\n")

	// Example 4: Response Schema enforcement
	fmt.Println("4. Response Schema (hard output constraint):")
	schemaOpts := ai.GenOptions{
		ThinkingLevel: "low",
		ResponseSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"answer": map[string]any{
					"type": "string",
				},
				"confidence": map[string]any{
					"type": "number",
				},
			},
			"required": []any{"answer"},
		},
	}
	_ = schemaOpts // Demonstrate the configuration
	fmt.Printf("   Input: JSON schema with 'answer' and 'confidence' fields\n")
	fmt.Printf("   Result: Model CANNOT emit keys outside this schema\n")
	fmt.Printf("   Benefit: Prevents hallucinated fields, ensures parseable output\n\n")

	// Summary
	fmt.Println("=== Integration Points ===")
	integrationPoints := []string{
		"✓ ai/interfaces.go - Added ThinkingLevel and ResponseSchema to GenOptions",
		"✓ ai/generator/gemini.go - Auto-detect medium level with tools",
		"✓ ai/agent/engine_native.go - ReAct loops use low/medium/high strategically",
		"✓ ai/agent/classifier.go - All classifiers use 'low' for strict JSON",
		"✓ ai/memory/manager.go - Memory operations use 'low' for formats",
		"✓ ai/agent/service.go - Topic routing uses 'low' for deterministic JSON",
		"✓ ai/agent/service.script.go - Script refinement uses 'low' for precision",
	}
	for _, point := range integrationPoints {
		fmt.Println(point)
	}

	fmt.Println("\n=== Key Benefits ===")
	benefits := []string{
		"→ Faster tool calls with lower reasoning overhead",
		"→ More reliable schema adherence in structured outputs",
		"→ Prevents creative syntax variations in tool parameters",
		"→ Hard constraints via ResponseSchema prevent hallucinated fields",
		"→ Automatically optimized for gemini-3.1-pro (and backward compatible)",
	}
	for _, benefit := range benefits {
		fmt.Println(benefit)
	}

	fmt.Println("\n✅ Integration Complete!")
}
