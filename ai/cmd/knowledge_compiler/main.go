package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/memory"
)

type KnowledgeChunk struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Text        string `json:"text"`
	Description string `json:"description"`
}

var linkRegex = regexp.MustCompile(`\[.*?\]\(([^)]+\.md)\)`)

func findRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		if _, err := os.Stat(filepath.Join(dir, ".git")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "."
}

var parsedFiles = make(map[string]bool)
var allChunks []KnowledgeChunk
var catGraphMap = make(map[string]*memory.Category)

func getCat(catPath string) *memory.Category {
	if c, ok := catGraphMap[catPath]; ok {
		return c
	}
	parts := strings.Split(catPath, " / ")
	id := sop.UUID(uuid.New())
	c := &memory.Category{
		ID:   id,
		Name: parts[len(parts)-1],
		Path: catPath,
	}
	if len(parts) > 1 {
		parentPath := strings.Join(parts[:len(parts)-1], " / ")
		parentCat := getCat(parentPath)
		c.ParentIDs = append(c.ParentIDs, memory.CategoryParent{ParentID: parentCat.ID})
	}
	catGraphMap[catPath] = c
	return c
}

func parseMarkdownRecursive(filePath string, categoryContext []string) {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return
	}

	if parsedFiles[absPath] {
		return
	}
	parsedFiles[absPath] = true

	// Ignore KB compiler's own files or AI Copilot usage instructions
	if strings.Contains(absPath, "knowledge_compiler") || strings.Contains(absPath, "AI_COPILOT_USAGE.md") {
		return
	}

	fmt.Printf("Crawling: %s\n", absPath)

	contentBytes, err := os.ReadFile(absPath)
	if err != nil {
		return
	}
	content := string(contentBytes)
	lines := strings.Split(content, "\n")

	currentContext := append([]string{}, categoryContext...)

	var currentChunkText []string
	var currentChunkTitle string

	flushChunk := func() {
		if len(currentChunkText) > 0 {
			body := strings.TrimSpace(strings.Join(currentChunkText, "\n"))
			if len(body) >= 50 {
				catPath := ""
				if len(currentContext) > 0 {
					catPath = strings.Join(currentContext, " / ")
				} else {
					catPath = strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath))
				}

				idPrefix := strings.ReplaceAll(strings.TrimSuffix(filepath.Base(filePath), filepath.Ext(filePath)), " ", "_")

				if currentChunkTitle == "" {
					currentChunkTitle = filepath.Base(filePath)
				}

				allChunks = append(allChunks, KnowledgeChunk{
					ID:          fmt.Sprintf("%s_section_%d", idPrefix, len(allChunks)),
					Category:    catPath,
					Text:        currentChunkTitle,
					Description: body,
				})
			}
			currentChunkText = nil
		}
	}

	var inCodeBlock bool

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		if strings.HasPrefix(trimmedLine, "```") {
			inCodeBlock = !inCodeBlock
			// Continue processing the line if needed, but don't treat it as a header
		}

		isHeader := false
		var headerText string
		var headerLevel int

		if !inCodeBlock {
			if strings.HasPrefix(trimmedLine, "# ") {
				isHeader = true
				headerLevel = 1
				headerText = strings.TrimPrefix(trimmedLine, "# ")
			} else if strings.HasPrefix(trimmedLine, "## ") {
				isHeader = true
				headerLevel = 2
				headerText = strings.TrimPrefix(trimmedLine, "## ")
			} else if strings.HasPrefix(trimmedLine, "### ") {
				isHeader = true
				headerLevel = 3
				headerText = strings.TrimPrefix(trimmedLine, "### ")
			}
		}

		if isHeader {
			flushChunk()

			headerText = strings.TrimSpace(strings.ReplaceAll(headerText, "*", ""))
			currentChunkTitle = headerText

			startDepth := len(categoryContext)

			expectedDepth := startDepth + headerLevel - 1
			if expectedDepth < len(currentContext) {
				currentContext = currentContext[:expectedDepth]
			}

			if len(currentContext) > expectedDepth {
				currentContext[expectedDepth] = headerText
			} else {
				for len(currentContext) < expectedDepth {
					currentContext = append(currentContext, "Misc Context")
				}
				currentContext = append(currentContext, headerText)
			}
		}

		currentChunkText = append(currentChunkText, line)

		matches := linkRegex.FindAllStringSubmatch(trimmedLine, -1)
		for _, match := range matches {
			if len(match) > 1 {
				target := match[1]
				targetPath := filepath.Clean(filepath.Join(filepath.Dir(filePath), target))

				parseMarkdownRecursive(targetPath, currentContext)
			}
		}
	}
	flushChunk()
}

func parseUnlinkedFiles(repoRoot string) {
	filepath.WalkDir(repoRoot, func(file string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}

		if d.IsDir() {
			name := d.Name()
			if strings.HasPrefix(name, ".") && name != "." && name != ".." {
				return filepath.SkipDir
			}
			if name == "node_modules" || name == "vendor" || name == "server" || name == "server_bin" {
				return filepath.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(strings.ToLower(file), ".md") {
			return nil
		}

		absPath, _ := filepath.Abs(file)

		// Ignore KB compiler itself or AI copilot usage file
		if strings.Contains(absPath, "knowledge_compiler") || strings.Contains(absPath, "AI_COPILOT_USAGE.md") {
			return nil
		}

		// Also skip files we explicitly excluded previously in the code like License etc
		filename := d.Name()
		upperName := strings.ToUpper(filename)
		if strings.Contains(upperName, "CODE_OF_CONDUCT") ||
			strings.Contains(upperName, "LICENSE") ||
			strings.Contains(upperName, "CHANGELOG") ||
			strings.Contains(upperName, "POST") ||
			strings.Contains(upperName, "ANNOUNCEMENT") ||
			strings.Contains(upperName, "RELEASE") ||
			strings.Contains(upperName, "README2") ||
			strings.Contains(upperName, "PROPOSAL") ||
			strings.Contains(upperName, "CONTRIBUTING") ||
			strings.Contains(upperName, "LINKEDIN") {
			return nil
		}

		if !parsedFiles[absPath] {
			fmt.Printf("Sweeping up unlinked file: %s\n", absPath)

			contentBytes, err := os.ReadFile(absPath)
			if err != nil {
				return nil
			}
			title := strings.TrimSuffix(filepath.Base(absPath), filepath.Ext(absPath))
			inCodeBlock := false
			for _, line := range strings.Split(string(contentBytes), "\n") {
				trimmed := strings.TrimSpace(line)
				if strings.HasPrefix(trimmed, "```") {
					inCodeBlock = !inCodeBlock
				}
				if !inCodeBlock && strings.HasPrefix(trimmed, "# ") {
					title = strings.TrimPrefix(trimmed, "# ")
					break
				}
			}

			// We launch recursion on this unlinked file with its generic top root title
			parseMarkdownRecursive(absPath, []string{title})
		}

		return nil
	})
}

func main() {
	repoRoot := findRepoRoot()
	rootReadme := filepath.Join(repoRoot, "README.md")

	fmt.Println("Starting Recursive AST Crawl from Root README.md...")
	parseMarkdownRecursive(rootReadme, []string{})

	fmt.Println("Sweeping unlinked Markdown files...")
	parseUnlinkedFiles(repoRoot)

	var exportItems []memory.ExportItem[map[string]any]
	for _, chunk := range allChunks {
		cat := getCat(chunk.Category)
		exportItems = append(exportItems, memory.ExportItem[map[string]any]{
			Category:  cat.ID.String(),
			Data:      map[string]any{"description": chunk.Description, "original_id": chunk.ID},
			Summaries: []string{chunk.Text},
		})
	}

	var categories []*memory.Category
	for _, c := range catGraphMap {
		categories = append(categories, c)
	}

	var systemPrompt string
	personaBytes, err := os.ReadFile(filepath.Join(repoRoot, "ai", "OMNI_PERSONA.md"))
	if err == nil {
		systemPrompt = string(personaBytes)
	}

	outObj := memory.ExportData[map[string]any]{
		Config: &memory.KnowledgeBaseConfig{
			IsPersona:    true,
			SystemPrompt: systemPrompt,
		},
		Categories: categories,
		Items:      exportItems,
	}

	outData, _ := json.MarshalIndent(outObj, "", "  ")
	outputPath := filepath.Join(repoRoot, "ai", "sop_base_knowledge.json")
	os.WriteFile(outputPath, outData, 0644)

	fmt.Printf("Success! Compiled %d knowledge items and %d categories into %s\n", len(exportItems), len(categories), outputPath)
}
