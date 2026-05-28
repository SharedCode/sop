package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/ai/memory"
)

func handlePreloadSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req PreloadSpaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.TemplateID == "" || req.DatabaseName == "" {
		http.Error(w, "template_id and database_name are required", http.StatusBadRequest)
		return
	}

	var actualPath string
	pathsToTry := []string{
		req.TemplateID + ".json",
		"../" + req.TemplateID + ".json",
		"../../" + req.TemplateID + ".json",
		"ai/" + req.TemplateID + ".json",
	}
	if strings.EqualFold(req.TemplateID, ai.DefaultKBName) || req.TemplateID == "SOP" {
		pathsToTry = append(pathsToTry, "sop_base_knowledge.json", "ai/sop_base_knowledge.json", "../ai/sop_base_knowledge.json")
	}

	for _, p := range pathsToTry {
		if _, err := os.Stat(p); err == nil {
			actualPath = p
			break
		}
	}

	if actualPath == "" && (strings.EqualFold(req.TemplateID, ai.DefaultKBName) || req.TemplateID == "SOP") {
		// Auto-generate if running in source development (go run)
		isGoRun := strings.Contains(os.Args[0], "go-build") || strings.HasPrefix(os.Args[0], os.TempDir())
		if isGoRun {
			fmt.Printf("SOP Knowledge Base JSON not found. Auto-compiling since running in dev mode...\n")
			compilerPath := "./ai/cmd/knowledge_compiler"
			if _, err := os.Stat("../../ai/cmd/knowledge_compiler"); err == nil {
				compilerPath = "../../ai/cmd/knowledge_compiler"
			} else if _, err := os.Stat("../ai/cmd/knowledge_compiler"); err == nil {
				compilerPath = "../ai/cmd/knowledge_compiler"
			}
			cmd := exec.Command("go", "run", compilerPath)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err == nil {
				// Search again
				for _, p := range pathsToTry {
					if _, err := os.Stat(p); err == nil {
						actualPath = p
						break
					}
				}
			} else {
				fmt.Printf("Warning: Failed to auto-compile SOP KB: %v\n", err)
			}
		}
	}

	if actualPath == "" {
		http.Error(w, fmt.Sprintf("Failed to find Space file for template '%s'", req.TemplateID), http.StatusBadRequest)
		return
	}

	// Remap to Ingest request
	ingestReq := IngestSpaceRequest{
		DatabaseName:    req.DatabaseName,
		SpaceName:       req.TemplateID, // Use template ID as the Space name
		PreloadFilePath: actualPath,
	}

	bodyBytes, _ := json.Marshal(ingestReq)
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if strings.EqualFold(req.TemplateID, ai.DefaultKBName) || strings.EqualFold(req.TemplateID, "SOP") {
		// Forward directly to ingest import for SOP KB
		handleIngestImportSpace(w, r)
	} else {
		// Forward directly to ingest for other templates
		handleIngestSpace(w, r)
	}
}

func extractSummaries(chunk SpaceIngestChunk) []string {
	summaries := chunk.Summaries
	var finalParagraphs []string
	seen := make(map[string]bool)

	addParagraph := func(toAdd []string) {
		for _, s := range toAdd {
			s = strings.TrimSpace(s)
			if s == "" || seen[s] {
				continue
			}
			seen[s] = true
			if len(finalParagraphs) < MAX_ITEM_SUMMARIES {
				finalParagraphs = append(finalParagraphs, s)
			}
		}
	}

	if sArr, ok := summaries.([]interface{}); ok && len(sArr) > 0 {
		var res []string
		for _, s := range sArr {
			if str, ok := s.(string); ok && str != "" {
				res = append(res, str)
			}
		}
		addParagraph(res)
	} else if sArr2, ok := summaries.([]string); ok && len(sArr2) > 0 {
		addParagraph(sArr2)
	} else if sStr, ok := summaries.(string); ok && sStr != "" {
		sStr = strings.ReplaceAll(sStr, "\r\n", "\n")
		parts := strings.Split(sStr, "\n\n")
		var res []string
		for _, p := range parts {
			p = strings.TrimSpace(p)
			if p != "" {
				if len(p) > MAX_PARAGRAPH_LENGTH {
					p = p[:MAX_PARAGRAPH_LENGTH]
				}
				res = append(res, p)
			}
		}
		addParagraph(res)
	}

	if len(finalParagraphs) >= MAX_ITEM_SUMMARIES {
		return finalParagraphs
	}

	// Check if we can apply some heuristics and come up with decent Summaries.
	s := determineSummaries(chunk.Text, chunk.Description, MAX_ITEM_SUMMARIES)
	addParagraph(s)

	if len(finalParagraphs) >= MAX_ITEM_SUMMARIES {
		return finalParagraphs
	}

	// Check if there is Text, Description in Data.
	if chunk.Data != nil {
		var dataText, dataDesc string
		if t, ok := chunk.Data["text"].(string); ok {
			dataText = t
		}
		if d, ok := chunk.Data["description"].(string); ok {
			dataDesc = d
		}
		if dataText != "" || dataDesc != "" {
			s = determineSummaries(dataText, dataDesc, MAX_ITEM_SUMMARIES)
			addParagraph(s)
		}
	}

	return finalParagraphs
}

func handleCreateSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateSpaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.SpaceName == "" || req.DatabaseName == "" {
		http.Error(w, "space_name and database_name are required", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	opts, err := getDBOptions(ctx, req.DatabaseName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get DB config: %v", err), http.StatusInternalServerError)
		return
	}

	db := database.NewDatabase(opts)
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	kb, err := db.OpenKnowledgeBase(ctx, req.SpaceName, trans, dbLLM, dbEmbedder, false)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open Space '%s': %v", req.SpaceName, err), http.StatusInternalServerError)
		return
	}

	if req.Attributes != nil {
		if err := kb.SetConfig(ctx, req.Attributes); err != nil {
			http.Error(w, fmt.Sprintf("Failed to set Space config: %v", err), http.StatusInternalServerError)
			return
		}
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, fmt.Sprintf("Failed to commit Space creation: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"message": fmt.Sprintf("Space '%s' created successfully", req.SpaceName),
	})
}

func handleIngestSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req IngestSpaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.DatabaseName == "" {
		http.Error(w, "database_name is required", http.StatusBadRequest)
		return
	}

	if req.SpaceName == "" {
		http.Error(w, "space_name is required", http.StatusBadRequest)
		return
	}

	if len(req.CustomData) == 0 && req.URL == "" && req.PreloadFilePath == "" {
		http.Error(w, "Must provide custom_data, url, or preload filepath to ingest", http.StatusBadRequest)
		return
	}

	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	task := RegisterTask("SpaceIngest", 100)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"task_id": task.TaskID,
		"message": fmt.Sprintf("Preloading and Ingesting %s started in background", req.SpaceName),
	})

	go func(taskId string, request IngestSpaceRequest, emb ai.Embeddings, llm ai.Generator) {
		defer func() {
			if rec := recover(); rec != nil {
				UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Panic during preload: %v", rec))
			}
		}()

		ctx := context.Background()
		UpdateTask(taskId, "in_progress", 10, 100, "Initializing database connection...", "")
		opts, err := getDBOptions(ctx, request.DatabaseName)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to get DB config: %v", err))
			return
		}

		db := database.NewDatabase(opts)

		trans, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to begin transaction: %v", err))
			return
		}

		kb, err := db.OpenKnowledgeBase(ctx, request.SpaceName, trans, llm, emb, false)
		if err != nil {
			trans.Rollback(ctx)
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to open KnowledgeBase '%s': %v", request.SpaceName, err))
			return
		}

		if request.Attributes != nil {
			err := kb.SetConfig(ctx, request.Attributes)
			if err != nil {
				fmt.Printf("Failed to insert Space Attributes: %v\n", err)
			}
		}

		var reader io.Reader
		var closer io.Closer

		if len(request.CustomData) > 0 {
			reader = bytes.NewReader(request.CustomData)
		} else if request.URL != "" {
			reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, request.URL, nil)
			if err == nil {
				resp, err := http.DefaultClient.Do(reqHTTP)
				if err == nil {
					reader = resp.Body
					closer = resp.Body
					defer closer.Close()
				}
			}
		} else if request.PreloadFilePath != "" {
			f, err := os.Open(request.PreloadFilePath)
			if err == nil {
				reader = f
				closer = f
				defer closer.Close()
			}
		}

		if reader != nil {
			UpdateTask(taskId, "in_progress", 30, 100, "Reading Space data stream...", "")

			decoder := json.NewDecoder(reader)
			var batch []memory.Thought[map[string]any]
			const batchSize = 500
			totalProcessed := 0

			// Track current config, fetch existing KB config or default to what's defined in the stream
			currentKBConfig, _ := kb.GetConfig(ctx)
			documentMode := false
			if currentKBConfig != nil {
				documentMode = currentKBConfig.DocumentMode
			}

			t, err := decoder.Token()
			if err == nil {
				if delim, ok := t.(json.Delim); ok {
					if delim == '{' {
						// It's an object, look for 'items' array
						for decoder.More() {
							keyToken, err := decoder.Token()
							if err != nil {
								break
							}
							keyStr, ok := keyToken.(string)
							if !ok {
								break
							}

							if keyStr == "config" {
								var cfg memory.KnowledgeBaseConfig
								if err := decoder.Decode(&cfg); err == nil {
									kb.SetConfig(ctx, &cfg)
									documentMode = cfg.DocumentMode
								}
							} else if keyStr == "items" {
								// read opening '['
								t, err := decoder.Token()
								if err != nil {
									break
								}
								if delim, ok := t.(json.Delim); !ok || delim != '[' {
									break
								}

								// read items
								for decoder.More() {
									var chunk SpaceIngestChunk
									if err := decoder.Decode(&chunk); err != nil {
										break
									}

									cid := chunk.ID
									if cid == "" {
										cid = fmt.Sprintf("custom_%d", totalProcessed)
									}

									batch = append(batch, memory.Thought[map[string]any]{
										DocID:     chunk.DocID,
										Summaries: extractSummaries(chunk), Vectors: chunk.Vectors, CategoryPath: chunk.Category, Data: buildChunkData(cid, chunk, documentMode),
									})
									totalProcessed++

									if len(batch) >= batchSize {
										UpdateTask(taskId, "in_progress", 50, totalProcessed+100, fmt.Sprintf("Embedding and ingesting batch (processed %d)...", totalProcessed), "")
										err := kb.IngestThoughts(ctx, batch, "expert")
										if err != nil {
											fmt.Printf("Failed to ingest batch: %v\n", err)
										}
										batch = batch[:0]
									}
								}
							} else {
								// skip unknown properties
								var skip any
								decoder.Decode(&skip)
							}
						}
					} else if delim == '[' {
						// It's an array directly
						for decoder.More() {
							var chunk SpaceIngestChunk
							if err := decoder.Decode(&chunk); err != nil {
								break
							}

							cid := chunk.ID
							if cid == "" {
								cid = fmt.Sprintf("custom_%d", totalProcessed)
							}

							batch = append(batch, memory.Thought[map[string]any]{
								DocID:     chunk.DocID,
								Summaries: extractSummaries(chunk), Vectors: chunk.Vectors, CategoryPath: chunk.Category, Data: buildChunkData(cid, chunk, documentMode),
							})
							totalProcessed++

							if len(batch) >= batchSize {
								UpdateTask(taskId, "in_progress", 50, totalProcessed+100, fmt.Sprintf("Embedding and ingesting batch (processed %d)...", totalProcessed), "")
								err := kb.IngestThoughts(ctx, batch, "expert")
								if err != nil {
									fmt.Printf("Failed to ingest batch: %v\n", err)
								}
								batch = batch[:0]
							}
						}
					}
				}
			}

			// Process remaining batch
			if len(batch) > 0 {
				UpdateTask(taskId, "in_progress", 50, totalProcessed, fmt.Sprintf("Embedding and ingesting final batch (processed %d)...", totalProcessed), "")
				err := kb.IngestThoughts(ctx, batch, "expert")
				if err != nil {
					fmt.Printf("Failed to ingest final batch: %v\n", err)
				}
			}

			if dbEmbedder != nil {
				cfg, cfgErr := kb.GetConfig(ctx)
				if cfgErr == nil && cfg != nil {
					needsUpdate := false
					if cfg.EmbedderDimension != dbEmbedder.Dim() {
						cfg.EmbedderDimension = dbEmbedder.Dim()
						needsUpdate = true
					}
					if cfg.Embedder != dbEmbedder.Name() {
						cfg.Embedder = dbEmbedder.Name()
						needsUpdate = true
					}
					if needsUpdate {
						kb.SetConfig(ctx, cfg)
					}
				}
			}
		}

		UpdateTask(taskId, "in_progress", 90, 100, "Committing changes...", "")
		if err := trans.Commit(ctx); err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to commit vector store initialization: %v", err))
			return
		}

		UpdateTask(taskId, "completed", 100, 100, fmt.Sprintf("Successfully pre-loaded %s", request.SpaceName), "")
	}(task.TaskID, req, dbEmbedder, dbLLM)
}

func handleIngestImportSpace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req IngestSpaceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.DatabaseName == "" || req.SpaceName == "" {
		http.Error(w, "database_name and space_name are required", http.StatusBadRequest)
		return
	}

	if len(req.CustomData) == 0 && req.URL == "" && req.PreloadFilePath == "" {
		http.Error(w, "Must provide custom_data, url, or preload filepath to ingest/import", http.StatusBadRequest)
		return
	}

	dbEmbedder := GetConfiguredEmbedder(r)
	dbLLM := GetConfiguredLLM(r)

	task := RegisterTask("SpaceIngestImport", 100)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": true,
		"task_id": task.TaskID,
		"message": fmt.Sprintf("Ingest/Importing %s started in background", req.SpaceName),
	})

	go func(taskId string, request IngestSpaceRequest, emb ai.Embeddings, llm ai.Generator) {
		defer func() {
			if rec := recover(); rec != nil {
				UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Panic during ingest import: %v", rec))
			}
		}()

		ctx := context.Background()
		UpdateTask(taskId, "in_progress", 10, 100, "Initializing database connection...", "")
		opts, err := getDBOptions(ctx, request.DatabaseName)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to get DB config: %v", err))
			return
		}

		db := database.NewDatabase(opts)
		trans, err := db.BeginTransaction(ctx, sop.ForWriting)
		if err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to begin transaction: %v", err))
			return
		}

		kb, err := db.OpenKnowledgeBase(ctx, request.SpaceName, trans, llm, emb, false)
		if err != nil {
			trans.Rollback(ctx)
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to open KnowledgeBase '%s': %v", request.SpaceName, err))
			return
		}

		var reader io.Reader
		var closer io.Closer

		if request.PreloadFilePath != "" {
			f, err := os.Open(request.PreloadFilePath)
			if err == nil {
				reader = f
				closer = f
				defer closer.Close()
			}
		} else if request.URL != "" {
			reqHTTP, err := http.NewRequestWithContext(ctx, http.MethodGet, request.URL, nil)
			if err == nil {
				resp, err := http.DefaultClient.Do(reqHTTP)
				if err == nil {
					reader = resp.Body
					closer = resp.Body
					defer closer.Close()
				}
			}
		} else if len(request.CustomData) > 0 {
			reader = bytes.NewReader(request.CustomData)
		}

		if reader != nil {
			UpdateTask(taskId, "in_progress", 30, 100, "Reading Space data stream...", "")

			enrichCb := func(it *memory.ExportItem[map[string]any]) {
				var textStr, descStr string
				if txt, ok := it.Data["text"].(string); ok {
					textStr = txt
				} else if txt, ok := it.Data["title"].(string); ok {
					textStr = txt
				} // In compiler, text is exported in Summaries, but other systems might put it in Data

				if desc, ok := it.Data["description"].(string); ok {
					descStr = desc
				} else if cnt, ok := it.Data["content"].(string); ok {
					descStr = cnt
				}

				it.Summaries = extractSummaries(SpaceIngestChunk{
					Summaries:   it.Summaries,
					Text:        textStr,
					Description: descStr,
					Data:        it.Data,
				})
			}

			err = kb.ImportJSON(ctx, reader, "expert", enrichCb)
			if err != nil {
				trans.Rollback(ctx)
				UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to ImportJSON: %v", err))
				return
			}
		}

		if dbEmbedder != nil {
			cfg, cfgErr := kb.GetConfig(ctx)
			if cfgErr == nil && cfg != nil {
				needsUpdate := false

				if cfg.EmbedderDimension != dbEmbedder.Dim() {
					cfg.EmbedderDimension = dbEmbedder.Dim()
					needsUpdate = true
				}
				if cfg.Embedder != dbEmbedder.Name() {
					cfg.Embedder = dbEmbedder.Name()
					needsUpdate = true
				}
				if needsUpdate {
					kb.SetConfig(ctx, cfg)
				}
			}
		}

		UpdateTask(taskId, "in_progress", 90, 100, "Committing changes...", "")
		if err := trans.Commit(ctx); err != nil {
			UpdateTask(taskId, "error", 0, 0, "", fmt.Sprintf("Failed to commit ingest import: %v", err))
			return
		}

		// Vectorize if "SOP" KB ingest (preloading).
		if request.SpaceName == "SOP" && emb != nil {
			UpdateTask(taskId, "in_progress", 95, 100, "Calculating Embeddings (Auto Vectorize)...", "")
			if err := db.Vectorize(ctx, kb.Name(), llm, emb, 100); err != nil {
				UpdateTask(taskId, "error", 100, 100, "", fmt.Sprintf("Import successful, but vectorization failed: %v", err))
				return
			}
		}

		UpdateTask(taskId, "completed", 100, 100, fmt.Sprintf("Successfully imported and ingested %s", request.SpaceName), "")
	}(task.TaskID, req, dbEmbedder, dbLLM)
}
