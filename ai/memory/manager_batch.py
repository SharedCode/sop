import re

with open('/Users/grecinto/sop/ai/dynamic/manager.go', 'r') as f:
    content = f.read()

# Add GenerateCategories
new_func = """// GenerateCategories uses the LLM to deduce a 2-4 word taxonomy category for a batch of raw thoughts.
func (m *MemoryManager[T]) GenerateCategories(ctx context.Context, texts []string, personaContext string) ([]string, error) {
\tif len(texts) == 0 {
\t\treturn nil, nil
\t}
\tvar promptBuilder strings.Builder
\tif personaContext != "" {
\t\tpromptBuilder.WriteString(fmt.Sprintf("Given the context '%s', categorize each of the following thoughts into exactly a 2-4 word concept.\\nReturn ONLY a comma-separated list of categories, one for each thought, in the exact same order.\\n\\n", personaContext))
\t} else {
\t\tpromptBuilder.WriteString("Categorize each of the following thoughts into exactly a 2-4 word concept.\\nReturn ONLY a comma-separated list of categories, one for each thought, in the exact same order\t\tpromptBuilder.WriteString("Categorize each of the followinit\t\tpromfmt\t\tpromptBuilder.WriteString("Categorize each of the following ax\t\tpromptBuilder.Wris),\t\tpromptBuilder.Writut, err := m.llm.Generate(ctx, promptBuilder.String(), opts)
\tif err != nil {
\t\treturn nil, fmt.Errorf("llm batch cla\t\treturn nil, fmt.Errorf("llm batch cla\t\treturn nil, fmt.Errorf("llm batch cla\t\treturn nil, fmt.Errorf("llm batch cl[]st\t\treturn nil, fmt.Enge parts {
\t\tresult = a\t\tresult = a\t\tresult = a\t\tre)
\t\tresult en(result) < len(texts) {
\t\tresult = append(result, "Uncategorized")
\t}
\treturn result[:len(texts)], nil
}

// IngestThought"""

content = content.replace('// IngestThought', new_func)

with open('/Users/grecinto/sop/ai/dynamic/manager.go', 'w') as f:
    f.write(content)
