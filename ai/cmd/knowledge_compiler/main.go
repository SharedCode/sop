package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/memory"

	"github.com/google/uuid"
)

type KnowledgeChunk struct {
	ID          string `json:"id"`
	Category    string `json:"category"`
	Text        string `json:"text"`
	Description string `json:"description"`
}

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

func parseREADME(readmePath string, fileCategoryMap map[string]string) {
	bytes, err := os.ReadFile(readmePath)
	if err != nil {
		return
	}

	lines := strings.Split(string(bytes), "\n")
	currentCategory := "General Overview"
	if strings.Contains(readmePath, "/ai/") {
		currentCategory = "AI & Scripts Overview"
	}

	linkRegex := regexp.MustCompile(`\[.*?\]\(([^)]+\.md)\)`)

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Update category on Headers (##)
		if strings.HasPrefix(line, "## ") {
			currentCategory = strings.TrimPrefix(line, "## ")
			currentCategory = strings.TrimSpace(strings.ReplaceAll(currentCategory, "*", ""))
		} else if strings.HasPrefix(line, "### ") {
			sub := strings.TrimPrefix(line, "### ")
			// Keep top level and append child
			currentCatParts := strings.Split(currentCategory, " / ")
			if len(currentCatParts) > 0 {
				currentCategory = currentCatParts[0] + " / " + strings.TrimSpace(strings.ReplaceAll(sub, "*", ""))
			}
		}

		// Map any links found
		matches := linkRegex.FindAllStringSubmatch(line, -1)
		for _, match := range matches {
			if len(match) > 1 {
				target := match[1]
				// Resolve path relative to the readme
				targetPath := filepath.Clean(filepath.Join(filepath.Dir(readmePath), target))
				if _, ok := fileCategoryMap[targetPath]; !ok {
					fileCategoryMap[targetPath] = currentCategory
				}
			}
		}
	}
}

func main() {
	var allChunks []KnowledgeChunk
	repoRoot := findRepoRoot()

	// Map to track the absolute path of files to their detected hierarchical category
	fileCategoryMap := make(map[string]string)

	// Parse Root README
	rootReadme := filepath.Join(repoRoot, "README.md")
	parseREADME(rootReadme, fileCategoryMap)

	// Parse AI README
	aiReadme := filepath.Join(repoRoot, "ai", "README.md")
	parseREADME(aiReadme, fileCategoryMap)

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

		domainContext := ""
		absPath, _ := filepath.Abs(file)
		if strings.Contains(absPath, "/ai/") {
			domainContext = "[AI MODULE DOC] "
		} else {
			domainContext = "[SOP CORE DOC] "
		}

		// Determine Category
		category := "Uncategorized"

		// Special override for root readmes
		if upperName == "README.MD" {
			if strings.Contains(absPath, "/ai/") {
				category = "AI / Documentation Root"
			} else {
				category = "Core / Documentation Root"
			}
		} else {
			if mappedCat, ok := fileCategoryMap[file]; ok {
				category = mappedCat
			} else if mappedCatAbs, ok := fileCategoryMap[absPath]; ok {
				category = mappedCatAbs
			} else {
				// Fallback: directory name
				category = filepath.Base(filepath.Dir(file))
			}
		}

		if category == "Table of contents" {
			category = filepath.Base(filepath.Dir(file))
		} else if strings.HasPrefix(category, "Table of contents / ") {
			category = strings.TrimPrefix(category, "Table of contents / ")
		}

		if strings.HasPrefix(category, "Unmapped / ") {
			category = strings.TrimPrefix(category, "Unmapped / ")
		}

		if category == filepath.Base(repoRoot) {
			name := strings.TrimSuffix(filepath.Base(file), filepath.Ext(file))
			name = strings.ReplaceAll(name, "_", " ")
			category = strings.Title(strings.ToLower(name))
		}

		fmt.Printf("Parsing: %s (Category: %s)\n", file, category)
		chunks := processMarkdownFile(file, domainContext, category)
		allChunks = append(allChunks, chunks...)

		return nil
	})

	catGraphMap := make(map[string]*memory.Category)
	var getCat func(string) *memory.Category
	getCat = func(catPath string) *memory.Category {
		if c, ok := catGraphMap[catPath]; ok {
			return c
		}
		parts := strings.Split(catPath, " / ")
		id := sop.UUID(uuid.New())
		c := &memory.Category{
			ID:   id,
			Name: catPath, // Keeping the full path so the Vector Embedder generates highly contextualized mathematical boundaries!
		}
		if len(parts) > 1 {
			parentPath := strings.Join(parts[:len(parts)-1], " / ")
			parentCat := getCat(parentPath)
			c.ParentIDs = append(c.ParentIDs, memory.CategoryParent{ParentID: parentCat.ID})
		}
		catGraphMap[catPath] = c
		return c
	}

	var exportItems []memory.ExportItem[map[string]any]
	for _, chunk := range allChunks {
		cat := getCat(chunk.Category)
		exportItems = append(exportItems, memory.ExportItem[map[string]any]{
			Category:  cat.Name,
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

func processMarkdownFile(path string, domainContext string, baseCategory string) []KnowledgeChunk {
	contentBytes, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	content := string(contentBytes)

	filename := filepath.Base(path)

	// Create safe ID prefix
	idPrefix := strings.ReplaceAll(strings.TrimSuffix(filename, filepath.Ext(filename)), " ", "_")

	var chunks []KnowledgeChunk
	parts := strings.Split(content, "\n## ")

	fileTitle := filename
	if len(parts) > 0 {
		firstLines := strings.Split(parts[0], "\n")
		for _, line := range firstLines {
			if strings.HasPrefix(line, "# ") {
				fileTitle = strings.TrimPrefix(line, "# ")
				fileTitle = strings.TrimSpace(strings.ReplaceAll(fileTitle, "*", ""))
				break
			}
		}
	}

	fileCategory := baseCategory
	cleanFilename := strings.TrimSuffix(filename, filepath.Ext(filename))

	if fileTitle != filename {
		if fileCategory == "Uncategorized" {
			fileCategory = fileTitle
		} else {
			fileCategory = fileCategory + " / " + fileTitle
		}
	} else {
		if fileCategory == "Uncategorized" {
			fileCategory = cleanFilename
		} else {
			fileCategory = fileCategory + " / " + cleanFilename
		}
	}

	for i, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		title := fileTitle
		body := part
		chunkCategory := fileCategory

		if i > 0 {
			lines := strings.SplitN(part, "\n", 2)
			title = strings.TrimSpace(lines[0])

			// We do NOT append the H2 title to the category hierarchy.
			// The H2 block is the Item itself, so it should belong to the file's category.
			chunkCategory = fileCategory

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
			ID:          fmt.Sprintf("%s_section_%d", idPrefix, i),
			Category:    chunkCategory,
			Text:        domainContext + title,
			Description: body,
		})
	}
	return chunks
}
