package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type ModelCatalog struct {
	LLM      []ModelCatalogGroup `json:"llm,omitempty"`
	Embedder []ModelCatalogGroup `json:"embedder,omitempty"`
}

type ModelCatalogGroup struct {
	Label   string               `json:"label"`
	Options []ModelCatalogOption `json:"options,omitempty"`
}

type ModelCatalogOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

func defaultModelCatalog() ModelCatalog {
	return ModelCatalog{
		LLM: []ModelCatalogGroup{
			{
				Label: "Google",
				Options: []ModelCatalogOption{
					{Value: "gemini:gemini-3.1-pro-preview", Label: "Gemini 3.1 Pro Preview"},
				},
			},
			{
				Label: "Anthropic",
				Options: []ModelCatalogOption{
					{Value: "anthropic:claude-4.6-sonnet", Label: "Claude Sonnet 4.6"},
				},
			},
			{
				Label: "OpenAI",
				Options: []ModelCatalogOption{
					{Value: "openai:gpt-5.4", Label: "GPT-5.4"},
				},
			},
			{
				Label: "Local",
				Options: []ModelCatalogOption{
					{Value: "ollama", Label: "Ollama (Local Default)"},
					{Value: "ollama:gemma-4", Label: "Ollama (Gemma 4)"},
				},
			},
		},
		Embedder: []ModelCatalogGroup{
			{
				Label: "Google",
				Options: []ModelCatalogOption{
					{Value: "gemini:gemini-embedding-2", Label: "Gemini (gemini-embedding-2)"},
				},
			},
			{
				Label: "OpenAI",
				Options: []ModelCatalogOption{
					{Value: "openai:text-embedding-3-small", Label: "OpenAI (text-embedding-3-small)"},
				},
			},
			{
				Label: "Local",
				Options: []ModelCatalogOption{
					{Value: "ollama:nomic-embed-text", Label: "Ollama (nomic-embed-text)"},
				},
			},
		},
	}
}

const modelCatalogFilename = "model_catalog.json"

func ensureModelCatalogDefaults(catalog *ModelCatalog) bool {
	defaults := defaultModelCatalog()
	changed := false

	if len(catalog.LLM) == 0 {
		catalog.LLM = defaults.LLM
		changed = true
	}
	if len(catalog.Embedder) == 0 {
		catalog.Embedder = defaults.Embedder
		changed = true
	}

	return changed
}

func resolveModelCatalogPath(configPath string) string {
	if configPath != "" {
		return filepath.Join(filepath.Dir(configPath), modelCatalogFilename)
	}

	if cwd, err := os.Getwd(); err == nil {
		return filepath.Join(cwd, modelCatalogFilename)
	}

	return modelCatalogFilename
}

func modelCatalogCandidatePaths(configPath string) []string {
	seen := map[string]struct{}{}
	var paths []string

	add := func(path string) {
		if path == "" {
			return
		}
		cleaned := filepath.Clean(path)
		if _, ok := seen[cleaned]; ok {
			return
		}
		seen[cleaned] = struct{}{}
		paths = append(paths, cleaned)
	}

	if configPath != "" {
		add(filepath.Join(filepath.Dir(configPath), modelCatalogFilename))
	}

	if exePath, err := os.Executable(); err == nil {
		add(filepath.Join(filepath.Dir(exePath), modelCatalogFilename))
	}

	if cwd, err := os.Getwd(); err == nil {
		add(filepath.Join(cwd, modelCatalogFilename))
		add(filepath.Join(filepath.Dir(cwd), modelCatalogFilename))
	}

	add(modelCatalogFilename)

	return paths
}

func saveModelCatalog(path string, catalog ModelCatalog) error {
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "    ")
	if err := encoder.Encode(catalog); err != nil {
		return err
	}

	return os.Rename(path+".tmp", path)
}

func loadModelCatalog(configPath string) (bool, error) {
	modelCatalog = defaultModelCatalog()

	for _, path := range modelCatalogCandidatePaths(configPath) {
		f, err := os.Open(path)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return false, err
		}

		var loaded ModelCatalog
		decodeErr := json.NewDecoder(f).Decode(&loaded)
		f.Close()
		if decodeErr != nil {
			return false, decodeErr
		}

		seededDefaults := ensureModelCatalogDefaults(&loaded)
		modelCatalog = loaded

		if seededDefaults {
			if err := saveModelCatalog(path, modelCatalog); err != nil {
				return false, err
			}
		}

		return seededDefaults, nil
	}

	if configPath == "" {
		return false, nil
	}

	path := resolveModelCatalogPath(configPath)
	if err := saveModelCatalog(path, modelCatalog); err != nil {
		return false, err
	}
	return true, nil
}

func ensureModelCatalogFile(configPath string) error {
	if configPath == "" {
		return nil
	}

	path := resolveModelCatalogPath(configPath)
	if _, err := os.Stat(path); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}

	catalog := modelCatalog
	if ensureModelCatalogDefaults(&catalog) {
		modelCatalog = catalog
	}

	return saveModelCatalog(path, catalog)
}
