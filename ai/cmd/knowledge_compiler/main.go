package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/text"

	"github.com/google/uuid"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/memory"
)

type Section struct {
	Level       int
	Title       string
	Paragraphs  []string
	Children    []*Section
	LinkedFiles []string
	FilePath    string
}

func extractTextAndLinks(n ast.Node, source []byte) (string, []string) {
	var links []string
	var buf strings.Builder

	ast.Walk(n, func(c ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering {
			if link, ok := c.(*ast.Link); ok {
				dest := string(link.Destination)
				if strings.HasSuffix(strings.ToLower(dest), ".md") && !strings.HasPrefix(dest, "http") {
					links = append(links, dest)
				}
			}

			// Natively extract all text from all children regardless of what they are (Lists, Code, P)
			if c.Kind() == ast.KindText {
				buf.Write(c.(*ast.Text).Text(source))
			} else if c.Kind() == ast.KindHTMLBlock {
				lines := c.Lines()
				for i := 0; i < lines.Len(); i++ {
					seg := lines.At(i)
					buf.Write(seg.Value(source))
				}
			} else if c.Kind() == ast.KindRawHTML {
				rawHtml := c.(*ast.RawHTML)
				if rawHtml.Segments != nil {
					for i := 0; i < rawHtml.Segments.Len(); i++ {
						seg := rawHtml.Segments.At(i)
						buf.Write(seg.Value(source))
					}
				}
			} else if c.Kind() == ast.KindCodeSpan {
				buf.Write(c.Text(source))
				buf.WriteString(" ")
			}
		} else {
			// Formatting spacings on exit
			if c.Kind() == ast.KindParagraph || c.Kind() == ast.KindListItem || c.Kind() == ast.KindFencedCodeBlock {
				buf.WriteString("\n")
			}
		}

		// If it's a FencedCodeBlock, ast.Walk doesn't traverse the internal text nodes safely out of the box in some configs.
		// Instead we must grab its lines directly.
		if entering && c.Kind() == ast.KindFencedCodeBlock {
			lines := c.Lines()
			buf.WriteString("```\n")
			for i := 0; i < lines.Len(); i++ {
				seg := lines.At(i)
				buf.Write(seg.Value(source))
			}
			buf.WriteString("```\n")
			return ast.WalkSkipChildren, nil
		}

		return ast.WalkContinue, nil
	})

	return strings.TrimSpace(buf.String()), links
}

func getHeadingText(n *ast.Heading, source []byte) string {
	var buf strings.Builder
	ast.Walk(n, func(c ast.Node, entering bool) (ast.WalkStatus, error) {
		if entering && c.Kind() == ast.KindText {
			buf.Write(c.(*ast.Text).Text(source))
		}
		return ast.WalkContinue, nil
	})
	return strings.TrimSpace(buf.String())
}

var parsedFiles = make(map[string]*Section)
var baseURL string

func parseMarkdownToTree(filePath string, defaultTitle string) *Section {
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return nil
	}

	if existing, found := parsedFiles[absPath]; found {
		return existing
	}

	if strings.Contains(absPath, "knowledge_compiler") || strings.Contains(absPath, "AI_COPILOT_USAGE.md") {
		return nil
	}

	fmt.Printf("Crawling: %s\n", absPath)

	source, err := os.ReadFile(absPath)
	if err != nil {
		return nil
	}

	md := goldmark.New()
	doc := md.Parser().Parse(text.NewReader(source))

	root := &Section{Level: 0, Title: defaultTitle, FilePath: filePath}
	stack := []*Section{root}

	ignoring := false
	ignoredLevel := 0

	for n := doc.FirstChild(); n != nil; n = n.NextSibling() {
		switch node := n.(type) {
		case *ast.Heading:
			level := node.Level

			if ignoring && level <= ignoredLevel {
				ignoring = false
			}

			title := getHeadingText(node, source)
			if strings.EqualFold(title, "table of contents") {
				ignoring = true
				ignoredLevel = level
				continue
			}

			if ignoring {
				continue
			}

			for len(stack) > 1 && stack[len(stack)-1].Level >= level {
				stack = stack[:len(stack)-1]
			}

			if level >= 4 {
				// Flatten any heading deeper than L3 directly into the L3 body
				prefix := strings.Repeat("#", level)
				textStr := prefix + " " + title
				stack[len(stack)-1].Paragraphs = append(stack[len(stack)-1].Paragraphs, textStr)
			} else {
				sec := &Section{
					Level:    level,
					Title:    title,
					FilePath: filePath,
				}
				stack[len(stack)-1].Children = append(stack[len(stack)-1].Children, sec)
				stack = append(stack, sec)
			}
		default:
			if ignoring {
				continue
			}
			textStr, links := extractTextAndLinks(node, source)
			if len(textStr) > 0 {
				stack[len(stack)-1].Paragraphs = append(stack[len(stack)-1].Paragraphs, textStr)
			}
			stack[len(stack)-1].LinkedFiles = append(stack[len(stack)-1].LinkedFiles, links...)
		}
	}

	// Cache tree for this file to prevent infinite recursions early
	parsedFiles[absPath] = root

	return root
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

var allChunks []KnowledgeChunk
var allDocuments []*memory.Document
var catGraphMap = make(map[string]*memory.Category)
var catDescriptions = make(map[string]string)

type KnowledgeChunk struct {
	ID          string   `json:"id"`
	Category    string   `json:"category"`
	Text        string   `json:"text"`
	Description string   `json:"description"`
	DocumentID  sop.UUID `json:"document_id"`
}

func getCat(catPath string) *memory.Category {
	if c, ok := catGraphMap[catPath]; ok {
		return c
	}
	parts := strings.Split(catPath, " / ")
	id := sop.UUID(uuid.New())
	c := &memory.Category{
		ID:   id,
		Name: removePrefix(parts[len(parts)-1], prefix),
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

func buildExportItems(chunks []KnowledgeChunk) []memory.ExportItem[map[string]any] {
	exportItems := make([]memory.ExportItem[map[string]any], 0, len(chunks))
	for _, chunk := range chunks {
		cat := getCat(chunk.Category)
		exportItems = append(exportItems, memory.ExportItem[map[string]any]{
			CategoryPath: cat.ID.String(),
			DocID:        chunk.DocumentID.String(),
			Data: map[string]any{
				"category":      cat.Name,
				"category_path": cat.Path,
				"description":   chunk.Description,
				"original_id":   chunk.ID,
			},
			Summaries: []string{chunk.Text},
		})
	}
	return exportItems
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
		if strings.Contains(absPath, "knowledge_compiler") {
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
			strings.Contains(upperName, "PROPOSAL") ||
			strings.Contains(upperName, "CONTRIBUTING") ||
			strings.Contains(upperName, "LINKEDIN") ||
			strings.Contains(upperName, "DEV_TO_POST") ||
			strings.Contains(upperName, "AI_COPILOT") ||
			strings.Contains(upperName, "SYSTEM_KNOWLEDGE") ||
			strings.HasPrefix(upperName, "CLASSIFY_") ||
			strings.Contains(upperName, "CURRENT_DESIGN_PLAN") {
			return nil
		}

		fmt.Printf("Sweeping file as L1 Category: %s\n", absPath)
		docTitle := strings.TrimSuffix(filepath.Base(absPath), filepath.Ext(absPath))
		title := docTitle
		tree := parseMarkdownToTree(absPath, title)
		if tree != nil {
			// --- Build Document metadata for Import ---
			// Since we intend to KEEP these docs out of the blob limits
			// and just process them dynamically on queries, we generate URLs
			// representing their local existence in repo, without pushing huge text chunks.
			relPath, _ := filepath.Rel(repoRoot, absPath)
			docID := sop.NewUUID()

			urlVal := "file://" + relPath
			if baseURL != "" {
				if strings.HasSuffix(baseURL, "/") {
					urlVal = baseURL + relPath
				} else {
					urlVal = baseURL + "/" + relPath
				}
			}

			doc := &memory.Document{
				ID:          docID,
				Title:       title,
				URL:         urlVal,
				ContentType: "text/markdown",
				Content:     "", // Omitted from physical DB storage because we referenced its filesystem URL
			}
			allDocuments = append(allDocuments, doc)

			// Extract title from top heading (H1, H2, H3, etc.) and description from its first paragraph
			if len(tree.Children) > 0 {
				title = tree.Children[0].Title
				if len(tree.Children[0].Paragraphs) > 0 {
					catDescriptions[normalizeCategoryName(title)] = cleanText(title + "\n\n" + tree.Children[0].Paragraphs[0])
				}
			} else if len(tree.Paragraphs) > 0 {
				catDescriptions[normalizeCategoryName(title)] = cleanText(title + "\n\n" + tree.Paragraphs[0])
			}

			if len(tree.Children) > 0 {
				if len(tree.Paragraphs) > 0 {
					tree.Children[0].Paragraphs = append(tree.Paragraphs, tree.Children[0].Paragraphs...)
				}
				for _, child := range tree.Children {
					if strings.EqualFold(title, child.Title) {
						if len(child.Children) == 0 && len(child.Paragraphs) == 0 {
							continue
						}
						processFlattenedTreeIntoChunks(child, []string{title}, docID)
					} else {
						if len(child.Children) == 0 && len(child.Paragraphs) == 0 {
							continue
						}
						processFlattenedTreeIntoChunks(child, []string{title, child.Title}, docID)
					}
				}
			} else {
				processFlattenedTreeIntoChunks(tree, []string{title}, docID)
			}
		}

		return nil
	})
}

func processFlattenedTreeIntoChunks(node *Section, currentPathContext []string, docID sop.UUID) {
	var normalizedPaths []string
	for _, p := range currentPathContext {
		normalizedPaths = append(normalizedPaths, normalizeCategoryName(p))
	}

	var catPath string
	if len(normalizedPaths) > 0 {
		catPath = strings.Join(normalizedPaths, " / ")
	} else {
		catPath = normalizeCategoryName(node.Title)
	}

	desc := node.Title
	if len(node.Paragraphs) > 0 {
		desc = node.Paragraphs[0]
	}
	cleanedDesc := cleanText(desc)
	if len(cleanedDesc) > 500 {
		runes := []rune(cleanedDesc)
		if len(runes) > 500 {
			cleanedDesc = string(runes[:500]) + "..."
		}
	}
	catDescriptions[catPath] = cleanedDesc

	body := strings.TrimSpace(strings.Join(node.Paragraphs, "\n"))

	if strings.Contains(node.Title, "Execute Script Tool") {
		fmt.Printf("Execute Script Tool -> bodyLen: %d, children: %d\n", len(body), len(node.Children))
	}
	// Create an Item if we have substantial content AND (length > 500 OR it's a leaf node)
	if len(body) >= 10 && (len(body) > 500 || len(node.Children) == 0) {
		idPrefix := strings.ReplaceAll(node.Title, " ", "_")
		allChunks = append(allChunks, KnowledgeChunk{
			ID:          fmt.Sprintf("%s_section_%d", idPrefix, len(allChunks)),
			Category:    catPath,
			Text:        removePrefix(cleanText(node.Title), prefix),
			Description: cleanText(body),
			DocumentID:  docID,
		})
	}

	for _, child := range node.Children {
		childPathContext := append([]string{}, currentPathContext...)
		childPathContext = append(childPathContext, child.Title)
		processFlattenedTreeIntoChunks(child, childPathContext, docID)
	}
}

var prefix string

func main() {
	flag.StringVar(&baseURL, "base-url", "", "Base URL for documents")
	flag.StringVar(&prefix, "prefix", "", "Prefix to remove from category names")
	flag.Parse()

	repoRoot := findRepoRoot()

	fmt.Println("Sweeping unlinked Markdown files...")
	parseUnlinkedFiles(repoRoot)

	fmt.Println("Rolling up single-item sub-categories...")
	changed := true
	for changed {
		changed = false
		itemsPerCat := make(map[string][]int)
		for i, chunk := range allChunks {
			itemsPerCat[chunk.Category] = append(itemsPerCat[chunk.Category], i)
		}

		for catPath, indices := range itemsPerCat {
			if len(indices) == 1 && strings.Contains(catPath, " / ") {
				hasChildren := false
				for otherCat := range itemsPerCat {
					if strings.HasPrefix(otherCat, catPath+" / ") {
						hasChildren = true
						break
					}
				}

				if !hasChildren {
					parts := strings.Split(catPath, " / ")
					parentPath := strings.Join(parts[:len(parts)-1], " / ")

					idx := indices[0]

					// Ensure the original ## header name is not lost when flattening
					headerName := parts[len(parts)-1]
					if !strings.HasPrefix(allChunks[idx].Text, headerName) {
						if allChunks[idx].Text != headerName {
							allChunks[idx].Text = headerName + " - " + allChunks[idx].Text
						}
					}

					allChunks[idx].Category = parentPath
					changed = true
				}
			}
		}
	}

	exportItems := buildExportItems(allChunks)

	var categories []*memory.Category
	for _, c := range catGraphMap {
		if desc, ok := catDescriptions[c.Path]; ok {
			c.Description = desc
		} else if desc, ok := catDescriptions[c.Name]; ok {
			c.Description = desc
		}
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
			DocumentMode: baseURL != "",
		},
		Categories: categories,
		Documents:  allDocuments,
		Items:      exportItems,
	}

	outData, _ := json.MarshalIndent(outObj, "", "  ")
	outputPath := filepath.Join(repoRoot, "ai", "sop_base_knowledge.json")
	os.WriteFile(outputPath, outData, 0644)

	fmt.Printf("Success! Compiled %d knowledge items and %d categories into %s\n", len(exportItems), len(categories), outputPath)
}
