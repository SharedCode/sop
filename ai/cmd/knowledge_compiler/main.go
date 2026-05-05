package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type KnowledgeChunk struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Text        string `json:"text"`
	Description string `json:"description"`
}

func main() {
	searchPatterns := []string{"./*.md", "./ai/*.md", "../*.md", "../../*.md"}

	var allChunks []KnowledgeChunk

	for _, pattern := range searchPatterns {
		files, _ := filepath.Glob(pattern)

		for _, file := range files {
			filename := filepath.Base(file)
			upperName := strings.ToUpper(filename)
			if strings.Contains(upperName, "CODE_OF_CONDUCT") ||
				strings.Contains(upperName, "LICENSE") ||
				strings.Contains(upperName, "CHANGELOG") ||
				strings.Contains(upperName, "ARTICLE") ||
				strings.Contains(upperName, "POST") ||
				strings.Contains(upperName, "ANNOUNCEMENT") ||
				strings.Contains(upperName, "RELEASE") ||
				strings.Contains(upperName, "README2") ||
				strings.Contains(upperName, "PROPOSAL") ||
				strings.Contains(upperName, "CONTRIBUTING") ||
				strings.Contains(upperName, "LINKEDIN") ||
				strings.Contains(upperName, "WHITEPAPER") {
				continue
			}

			// Deduplication: If we find multiple READMEs or COOKBOOKs,
			// tag them so the LLM knows their domain.
			domainContext := ""
			absPath, _ := filepath.Abs(file)
			if strings.Contains(absPath, "/ai/") {
				if upperName == "README.MD" {
					domainContext = "[AI MODULE ROOT README] "
				} else if upperName == "COOKBOOK.MD" {
					domainContext = "[AI MODULE COOKBOOK] "
				} else if upperName == "OMNI_PERSONA.MD" {
					domainContext = "[SYSTEM DIRECTIVE] "
				} else {
					domainContext = "[AI MODULE DOC] "
				}
			} else {
				if upperName == "README.MD" {
					domainContext = "[SOP CORE README] "
				} else if upperName == "COOKBOOK.MD" {
					domainContext = "[SOP CORE COOKBOOK] "
				} else if upperName == "AI_COPILOT.MD" {
					domainContext = "[AI INTEGRATION OVERVIEW] "
				} else {
					domainContext = "[SOP CORE DOC] "
				}
			}

			fmt.Printf("Parsing: %s (Context: %s)\n", file, strings.TrimSpace(domainContext))
			chunks := processMarkdownFile(file, domainContext)
			allChunks = append(allChunks, chunks...)
		}
	}

	outData, _ := json.MarshalIndent(allChunks, "", "  ")
	outputPath := "sop_base_knowledge.json"
	os.WriteFile(outputPath, outData, 0644)

	fmt.Printf("Success! Compiled %d knowledge chunks into %s\n", len(allChunks), outputPath)
}

func processMarkdownFile(path string, domainContext string) []KnowledgeChunk {
	contentBytes, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	content := string(contentBytes)

	filename := filepath.Base(path)
	category := strings.TrimSuffix(filename, filepath.Ext(filename))

	var chunks []KnowledgeChunk
	parts := strings.Split(content, "\n## ")

	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		title := filename
		body := part

		if i > 0 {
			lines := strings.SplitN(part, "\n", 2)
			title = strings.TrimSpace(lines[0])
			if len(lines) > 1 {
				body = strings.TrimSpace(lines[1])
			} else {
				body = ""
			}
		} else {
			if strings.HasPrefix(part, "# ") {
				lines := strings.SplitN(part, "\n", 2)
				title = strings.TrimPrefix(strings.TrimSpace(lines[0]), "# ")
				if len(lines) > 1 {
					body = strings.TrimSpace(lines[1])
				}
			}
		}

		title = strings.TrimLeft(title, "# ")

		if len(strings.TrimSpace(body)) < 50 {
			continue
		}

		chunks = append(chunks, KnowledgeChunk{
			ID:          fmt.Sprintf("%s_section_%d", category, i),
			Category:    category,
			Text:        domainContext + title,
			Description: body,
		})
	}
	return chunks
}
