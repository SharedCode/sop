package agent

import (
	"encoding/json"
	"fmt"
	"strings"
)

// PromptComponent represents the standard ingredients of our system prompt.
type PromptComponent string

const (
	ComponentPersona        PromptComponent = "persona"
	ComponentSemanticMemory PromptComponent = "semantic_memory"
	ComponentSystemTools    PromptComponent = "system_tools"
	ComponentRecipes        PromptComponent = "workflow_recipes"
	ComponentPlaybooks      PromptComponent = "playbooks"
	ComponentFocusedContext PromptComponent = "focused_execution_context"
	ComponentSchema         PromptComponent = "schema"
	ComponentHistory        PromptComponent = "conversation_history"
	ComponentUserQuery      PromptComponent = "user_query"
)

// PromptElement represents a single block of context for the LLM.
type PromptElement struct {
	Component PromptComponent `json:"component"`
	Content   string          `json:"content"`
}

// PromptBudgetProfile caps prompt growth per component and overall.
type PromptBudgetProfile struct {
	TotalChars            int
	ComponentCharBudgets  map[PromptComponent]int
	TrimPriorityLowToHigh []PromptComponent
}

// PromptComponentBudgetStat captures how a single component changed under budgeting.
type PromptComponentBudgetStat struct {
	Component     PromptComponent `json:"component"`
	OriginalChars int             `json:"original_chars"`
	FinalChars    int             `json:"final_chars"`
}

// Trimmed reports whether the component lost content under budgeting.
func (s PromptComponentBudgetStat) Trimmed() bool {
	return s.FinalChars < s.OriginalChars
}

// PromptBudgetReport captures the effect of prompt budgeting on the final prompt.
type PromptBudgetReport struct {
	OriginalTotalChars int                         `json:"original_total_chars"`
	FinalTotalChars    int                         `json:"final_total_chars"`
	ComponentStats     []PromptComponentBudgetStat `json:"component_stats"`
}

// TrimmedComponents returns only the components that were reduced under budget.
func (r PromptBudgetReport) TrimmedComponents() []PromptComponentBudgetStat {
	trimmed := make([]PromptComponentBudgetStat, 0)
	for _, stat := range r.ComponentStats {
		if stat.Trimmed() {
			trimmed = append(trimmed, stat)
		}
	}
	return trimmed
}

// SystemPromptBuilder helps construct robust, structured prompts declaratively.
type SystemPromptBuilder struct {
	elements []PromptElement
}

// NewSystemPromptBuilder initializes a new declarative builder.
func NewSystemPromptBuilder() *SystemPromptBuilder {
	return &SystemPromptBuilder{
		elements: make([]PromptElement, 0),
	}
}

// With adds a new ingredient to the prompt if the content is not empty.
func (b *SystemPromptBuilder) With(component PromptComponent, content string) *SystemPromptBuilder {
	content = strings.TrimSpace(content)
	if content != "" {
		b.elements = append(b.elements, PromptElement{
			Component: component,
			Content:   content,
		})
	}
	return b
}

// ToJSON serializes the structured prompt into a highly readable JSON array string.
// This is optimal for modern LLMs (Gemini, GPT-4) to parse distinct sections clearly.
func (b *SystemPromptBuilder) ToJSON() string {
	if len(b.elements) == 0 {
		return "[]"
	}
	bytes, err := json.MarshalIndent(b.elements, "", "  ")
	if err != nil {
		return ""
	}
	return string(bytes)
}

// ToJSONWithBudget serializes the prompt after applying component and total-size budgets.
func (b *SystemPromptBuilder) ToJSONWithBudget(profile PromptBudgetProfile) string {
	rendered, _ := b.ToJSONWithBudgetReport(profile)
	return rendered
}

// ToJSONWithBudgetReport serializes the prompt after applying budgets and returns diagnostics.
func (b *SystemPromptBuilder) ToJSONWithBudgetReport(profile PromptBudgetProfile) (string, PromptBudgetReport) {
	if len(b.elements) == 0 {
		return "[]", PromptBudgetReport{}
	}

	elements := make([]PromptElement, 0, len(b.elements))
	report := PromptBudgetReport{ComponentStats: make([]PromptComponentBudgetStat, 0, len(b.elements))}
	for _, el := range b.elements {
		originalContent := strings.TrimSpace(el.Content)
		content := el.Content
		if maxChars, ok := profile.ComponentCharBudgets[el.Component]; ok && maxChars > 0 {
			content = trimPromptComponentContent(el.Component, content, maxChars)
		}
		content = strings.TrimSpace(content)
		report.OriginalTotalChars += len(originalContent)
		if content != "" {
			report.FinalTotalChars += len(content)
			elements = append(elements, PromptElement{Component: el.Component, Content: content})
		}
		report.ComponentStats = append(report.ComponentStats, PromptComponentBudgetStat{
			Component:     el.Component,
			OriginalChars: len(originalContent),
			FinalChars:    len(content),
		})
	}

	if profile.TotalChars > 0 {
		elements, report = trimPromptElementsToTotalBudget(elements, profile, report)
	}

	if len(elements) == 0 {
		return "[]", report
	}
	bytes, err := json.MarshalIndent(elements, "", "  ")
	if err != nil {
		return "", report
	}
	return string(bytes), report
}

// ToXML serializes the structured prompt into XML-style tags.
// This is optimal for Claude and older Anthropic models.
func (b *SystemPromptBuilder) ToXML() string {
	var sb strings.Builder
	for _, el := range b.elements {
		sb.WriteString(fmt.Sprintf("<%s>\n%s\n</%s>\n\n", el.Component, el.Content, el.Component))
	}
	return strings.TrimSpace(sb.String())
}

func trimPromptElementsToTotalBudget(elements []PromptElement, profile PromptBudgetProfile, report PromptBudgetReport) ([]PromptElement, PromptBudgetReport) {
	totalChars := 0
	for _, el := range elements {
		totalChars += len(el.Content)
	}
	if totalChars <= profile.TotalChars {
		report.FinalTotalChars = totalChars
		return elements, report
	}

	working := append([]PromptElement(nil), elements...)
	excess := totalChars - profile.TotalChars
	for _, component := range profile.TrimPriorityLowToHigh {
		for i := range working {
			if working[i].Component != component || excess <= 0 {
				continue
			}

			floor := 0
			if budget, ok := profile.ComponentCharBudgets[component]; ok && budget > 0 {
				floor = minPromptChars(budget / 3)
			}
			if len(working[i].Content) <= floor {
				continue
			}

			target := len(working[i].Content) - excess
			if target < floor {
				target = floor
			}
			trimmed := trimPromptComponentContent(component, working[i].Content, target)
			reducedBy := len(working[i].Content) - len(trimmed)
			working[i].Content = trimmed
			for idx := range report.ComponentStats {
				if report.ComponentStats[idx].Component == component {
					report.ComponentStats[idx].FinalChars = len(trimmed)
					break
				}
			}
			excess -= reducedBy
		}
	}

	filtered := working[:0]
	for _, el := range working {
		if strings.TrimSpace(el.Content) != "" {
			filtered = append(filtered, el)
		}
	}
	report.FinalTotalChars = 0
	for _, el := range filtered {
		report.FinalTotalChars += len(el.Content)
	}
	return filtered, report
}

func trimPromptComponentContent(component PromptComponent, content string, maxChars int) string {
	content = strings.TrimSpace(content)
	if maxChars <= 0 || len(content) <= maxChars {
		return content
	}

	marker := "\n...[truncated]"
	if maxChars <= len(marker)+16 {
		return content[:maxChars]
	}

	switch component {
	case ComponentHistory:
		keep := maxChars - len(marker)
		if keep < 0 {
			keep = 0
		}
		if keep >= len(content) {
			return content
		}
		return marker + content[len(content)-keep:]
	default:
		keep := maxChars - len(marker)
		if keep < 0 {
			keep = 0
		}
		if keep >= len(content) {
			return content
		}
		return content[:keep] + marker
	}
}

func minPromptChars(v int) int {
	if v < 120 {
		return 120
	}
	return v
}
