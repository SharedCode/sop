package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type KnowledgeChunk struct {
	Category string `json:"category"`
	Title    string `json:"title"`
	Content  string `json:"content"`
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
				strings.Contains(upperName, "CHANGELOG") {
				continue
			}

			chunks := processMarkdownFile(file)
			allChunks = append(allChunks, chunks...)
		}
	}

	outData, _ := json.MarshalIndent(allChunks, "", "  ")
	outputPath := "sop_base_knowledge.json"
	os.WriteFile(outputPath, outData, 0644)

	fmt.Printf("Success! Compiled %d knowledge chunks into %s\n", len(allChunks), outputPath)
}

func processMarkdownFile(path string) []KnowledgeChunk {
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
			Category: category,
			Title:    title,
			Content:  body,
		})
	}
	return chunks
}
