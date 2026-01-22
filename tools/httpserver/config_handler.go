package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	log "log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/model"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/fs"
)

// handleListEnvironments returns a list of JSON config files in the current directory.
func handleListEnvironments(w http.ResponseWriter, r *http.Request) {
	files, err := os.ReadDir(".")
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to read directory: %v", err), http.StatusInternalServerError)
		return
	}

	var sets []string
	for _, f := range files {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".json" {
			// Basic heuristic: check if it has "system_db" or "databases" field?
			// For now, simpler: just list all single-level JSONs or assume naming convention?
			// Let's just list all JSONs for now, user can pick.
			sets = append(sets, f.Name())
		}
	}
	json.NewEncoder(w).Encode(sets)
}

// handleCreateEnvironment creates a new empty JSON config file and switches to it.
func handleCreateEnvironment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name string `json:"name"`
	}
	// Debug: Dump raw body
	bodyBytes, _ := io.ReadAll(r.Body)
	log.Debug(fmt.Sprintf("RAW INIT DB PAYLOAD: %s", string(bodyBytes)))

	// Refill body for decoder
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Debug hex dump to find source of corruption
	if req.Name != "" {
		log.Info(fmt.Sprintf("DEBUG-NAME-HEX (CreateEnv): Name='%s', Hex=%x", req.Name, []byte(req.Name)))
	}

	// Sanitize Name
	req.Name = sanitizePath(req.Name)

	if req.Name == "" {
		http.Error(w, "Name is required", http.StatusBadRequest)
		return
	}

	// Ensure .json extension
	filename := req.Name
	if filepath.Ext(filename) != ".json" {
		filename += ".json"
	}

	// Check conflict
	if _, err := os.Stat(filename); err == nil {
		http.Error(w, "Environment already exists. Please choose a different name.", http.StatusConflict)
		return
	}

	// Initialize config in MEMORY ONLY.
	// We do NOT write to disk yet to avoid creating empty "abandoned" files if the user cancels the wizard.
	config = Config{
		Port:       8080,
		Databases:  []DatabaseConfig{},
		ConfigFile: filename,
	}

	// Reload agents (will be empty)
	initAgents()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "ok",
		"message": "Environment prepared (in-memory) and active",
		"file":    filename,
	})
}

// handleSwitchEnvironment loads a specific JSON config file into memory.
func handleSwitchEnvironment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Filename string `json:"filename"`
	}
	// Debug: Dump raw body
	bodyBytes, _ := io.ReadAll(r.Body)
	log.Debug(fmt.Sprintf("RAW INIT DB PAYLOAD: %s", string(bodyBytes)))

	// Refill body for decoder
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Filename == "" {
		http.Error(w, "Filename required", http.StatusBadRequest)
		return
	}

	// Verify it exists
	if _, err := os.Stat(req.Filename); os.IsNotExist(err) {
		http.Error(w, "File not found", http.StatusNotFound)
		return
	}

	if err := loadConfig(req.Filename); err != nil {
		http.Error(w, fmt.Sprintf("Failed to load config: %v", err), http.StatusInternalServerError)
		return
	}

	// Re-initialize agents to pick up new database configuration
	initAgents()

	// Force update ConfigFile tracker
	config.ConfigFile = req.Filename

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Switched to " + req.Filename})
}

// handleDeleteEnvironment deletes a configuration file and optionally its data.
func handleDeleteEnvironment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Filename       string `json:"filename"`
		DeleteData     bool   `json:"delete_data"` // Legacy
		DeleteSystemDB bool   `json:"delete_system_db"`
		DeleteUserDBs  bool   `json:"delete_user_dbs"`
	}
	// Debug: Dump raw body
	bodyBytes, _ := io.ReadAll(r.Body)
	log.Debug(fmt.Sprintf("RAW DELETE ENV PAYLOAD: %s", string(bodyBytes)))

	// Refill body for decoder
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Filename == "" {
		http.Error(w, "Filename is required", http.StatusBadRequest)
		return
	}

	// 1. Read config to find paths
	// If any delete flag is true
	shouldDeleteSystem := req.DeleteSystemDB || req.DeleteData
	shouldDeleteUsers := req.DeleteUserDBs || req.DeleteData

	if shouldDeleteSystem || shouldDeleteUsers {
		f, err := os.Open(req.Filename)
		if err == nil {
			var targetConfig Config
			if err := json.NewDecoder(f).Decode(&targetConfig); err == nil {
				// System DB
				if shouldDeleteSystem && targetConfig.SystemDB != nil && targetConfig.SystemDB.Path != "" {
					if err := database.Remove(r.Context(), targetConfig.SystemDB.Path); err != nil {
						log.Warn(fmt.Sprintf("Failed to remove SystemDB path %s: %v", targetConfig.SystemDB.Path, err))
					}
				}
				// User DBs
				if shouldDeleteUsers {
					for _, db := range targetConfig.Databases {
						if db.Path != "" {
							if err := database.Remove(r.Context(), db.Path); err != nil {
								log.Warn(fmt.Sprintf("Failed to remove UserDB path %s: %v", db.Path, err))
							}
						}
					}
				}
			}
			f.Close()
		}
	}

	// 2. Delete Config File
	if err := os.Remove(req.Filename); err != nil && !os.IsNotExist(err) {
		http.Error(w, fmt.Sprintf("Failed to remove config file: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Environment deleted successfully"})
}

// handleGetSystemEnv returns environment variables related to System DB configuration
func handleGetSystemEnv(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	envPath := os.Getenv("SYSTEM_DB_PATH")

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"system_db_path": envPath,
	})
}

// handleSaveConfig writes the provided configuration to the specified file path.
func handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		RegistryPath    string   `json:"registry_path"`
		StoresFolders   []string `json:"stores_folders"`
		Port            int      `json:"port"`
		Type            string   `json:"type"`     // standalone|clustered
		ConnectionURL   string   `json:"conn_url"` // Redis
		RegistryHashMod int      `json:"registry_hash_mod"`
		LLMApiKey       string   `json:"llm_api_key"`
		UseSharedBrain  bool     `json:"use_shared_brain"`
		ErasureConfig   *struct {
			DataChunks   int      `json:"data_chunks"`
			ParityChunks int      `json:"parity_chunks"`
			BasePaths    []string `json:"base_paths"`
		} `json:"erasure_config"`
		ErasureConfigs []struct {
			Key          string   `json:"key"`
			DataChunks   int      `json:"data_chunks"`
			ParityChunks int      `json:"parity_chunks"`
			BasePaths    []string `json:"base_paths"`
		} `json:"erasure_configs"`
	}

	// Debug: Dump raw body
	bodyBytes, _ := io.ReadAll(r.Body)
	log.Debug(fmt.Sprintf("RAW INIT DB PAYLOAD: %s", string(bodyBytes)))

	// Refill body for decoder
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Debug hex dump to find source of corruption
	if req.RegistryPath != "" {
		log.Info(fmt.Sprintf("DEBUG-PATH-HEX (SaveConfig): RegistryPath='%s', Hex=%x", req.RegistryPath, []byte(req.RegistryPath)))
	}

	// Sanitize paths
	req.RegistryPath = sanitizePath(req.RegistryPath)
	for i, sf := range req.StoresFolders {
		req.StoresFolders[i] = sanitizePath(sf)
	}
	if req.ErasureConfig != nil {
		for i, bp := range req.ErasureConfig.BasePaths {
			req.ErasureConfig.BasePaths[i] = sanitizePath(bp)
		}
	}
	for i := range req.ErasureConfigs {
		for j, bp := range req.ErasureConfigs[i].BasePaths {
			req.ErasureConfigs[i].BasePaths[j] = sanitizePath(bp)
		}
	}

	// VALIDATION START
	{
		newPaths := []string{}
		// Deduplication Map (Key: Absolute Path)
		// This prevents "Internal Path Conflict" if RegistryPath matches a StoresFolder,
		// or if multiple identical paths are provided (e.g. EC partitions).
		seenPaths := make(map[string]struct{})

		addPath := func(p string) {
			if p == "" {
				return
			}
			// Use Absolute Path for deduplication to catch "/tmp/db" vs "/tmp/db/"
			abs, err := filepath.Abs(p)
			if err != nil {
				// Fallback to raw string if Abs fails, though validatePathSafety will likely catch it later
				abs = p
			}
			if _, exists := seenPaths[abs]; !exists {
				seenPaths[abs] = struct{}{}
				newPaths = append(newPaths, p)
			}
		}

		if req.RegistryPath != "" {
			addPath(req.RegistryPath)
		}
		for _, sf := range req.StoresFolders {
			addPath(sf)
		}

		if req.ErasureConfig != nil {
			for _, bp := range req.ErasureConfig.BasePaths {
				addPath(bp)
			}
		}
		for _, ec := range req.ErasureConfigs {
			for _, bp := range ec.BasePaths {
				addPath(bp)
			}
		}

		alreadyConfigured := collectAllConfiguredPaths(SystemDBName)
		if err := validatePathSafety(newPaths, alreadyConfigured); err != nil {
			http.Error(w, fmt.Sprintf("Path Safety Error: %v", err), http.StatusBadRequest)
			return
		}
	}
	// VALIDATION END

	// Update global config
	if req.Port > 0 {
		config.Port = req.Port
	}
	if req.LLMApiKey != "" {
		config.LLMApiKey = req.LLMApiKey
	}

	// Initialize SystemDB if RegistryPath provided (Setup Wizard)
	if req.RegistryPath != "" {
		// 1. Setup SystemDB on disk
		// Deduplicate paths: RegistryPath + StoresFolders
		storeFolders := []string{req.RegistryPath}
		folderSet := map[string]struct{}{req.RegistryPath: {}}

		if len(req.StoresFolders) > 0 {
			for _, sf := range req.StoresFolders {
				if _, exists := folderSet[sf]; !exists {
					storeFolders = append(storeFolders, sf)
					folderSet[sf] = struct{}{}
				}
			}
		}

		if req.RegistryHashMod == 0 {
			req.RegistryHashMod = fs.MinimumModValue
		}

		sysOpts := sop.DatabaseOptions{
			StoresFolders:        storeFolders,
			RegistryHashModValue: req.RegistryHashMod,
		}

		if req.Type == "clustered" {
			sysOpts.Type = sop.Clustered
			sysOpts.CacheType = sop.Redis
			redisUrl := req.ConnectionURL
			if redisUrl == "" {
				redisUrl = "localhost:6379"
			}
			sysOpts.RedisConfig = &sop.RedisCacheConfig{
				Address: redisUrl,
			}
		} else {
			sysOpts.Type = sop.Standalone
		}

		if req.ErasureConfig != nil && req.ErasureConfig.DataChunks > 0 {
			sysOpts.ErasureConfig = map[string]sop.ErasureCodingConfig{
				"default": {
					DataShardsCount:             req.ErasureConfig.DataChunks,
					ParityShardsCount:           req.ErasureConfig.ParityChunks,
					BaseFolderPathsAcrossDrives: req.ErasureConfig.BasePaths,
				},
			}
		}

		// Support multiple partitions (max 4 usually)
		if len(req.ErasureConfigs) > 0 {
			if sysOpts.ErasureConfig == nil {
				sysOpts.ErasureConfig = make(map[string]sop.ErasureCodingConfig)
			}
			for _, ec := range req.ErasureConfigs {
				// Handle explicit empty string set by user as "" (two double quotes)
				key := ec.Key
				if key == "\"\"" {
					key = ""
				}
				sysOpts.ErasureConfig[key] = sop.ErasureCodingConfig{
					DataShardsCount:             ec.DataChunks,
					ParityShardsCount:           ec.ParityChunks,
					BaseFolderPathsAcrossDrives: ec.BasePaths,
				}
			}
		}

		// Check for existing System DB files to determine if this is a "Shared Brain" / Reuse scenario
		hasDBOptions, hasRegHashMod := database.IsDatabasePath(req.RegistryPath)

		// Relaxation: Check 'system_db' subfolder if not found in root
		if !hasDBOptions {
			subPath := filepath.Join(req.RegistryPath, "system_db")
			if exists, mod := database.IsDatabasePath(subPath); exists {
				req.RegistryPath = subPath
				hasDBOptions = true
				hasRegHashMod = mod
			}
		}

		shouldSetup := true

		if req.UseSharedBrain {
			// User wants to explicitly reuse an existing DB
			if hasDBOptions && hasRegHashMod {
				shouldSetup = false
				log.Info(fmt.Sprintf("Shared Brain detected at '%s'. Reusing...", req.RegistryPath))

				// 1. Check Write Permissions
				testFile := filepath.Join(req.RegistryPath, ".sop_write_test")
				if f, err := os.Create(testFile); err != nil {
					http.Error(w, fmt.Sprintf("System DB path exists but is not writable: %v", err), http.StatusBadRequest)
					return
				} else {
					f.Close()
					os.Remove(testFile)
				}

				// 2. Load existing options to ensure sysOpts matches the on-disk DB
				// This prevents using default HashMod (1) if the DB was created with something else.
				dbOptionsPath := filepath.Join(req.RegistryPath, "dboptions.json")
				existingOptsBytes, err := os.ReadFile(dbOptionsPath)
				if err == nil {
					var existingOpts sop.DatabaseOptions
					if err := json.Unmarshal(existingOptsBytes, &existingOpts); err == nil {
						sysOpts = existingOpts
						log.Info("Loaded existing Database Options from disk.")
					} else {
						log.Warn(fmt.Sprintf("Failed to parse existing dboptions.json: %v", err))
					}
				}
			} else {
				// Missing files
				http.Error(w, fmt.Sprintf("Shared Brain selected but System DB files (dboptions.json, reghashmod.txt) are missing in '%s'.", req.RegistryPath), http.StatusBadRequest)
				return
			}
		} else {
			// User wants to create a NEW DB
			if hasDBOptions || hasRegHashMod {
				// Files exist -> Conflict
				http.Error(w, fmt.Sprintf("Destination path '%s' already contains System DB files. Enable 'Use Shared Brain' to reuse it or choose a clean path.", req.RegistryPath), http.StatusBadRequest)
				return
			}
			// intended behavior: shouldSetup remains true
		}

		if shouldSetup {
			if _, err := database.Setup(r.Context(), sysOpts); err != nil {
				http.Error(w, fmt.Sprintf("Failed to setup system registry: %v", err), http.StatusInternalServerError)
				return
			}
		}

		// Auto-Create "Scripts" Store for AI Agents
		func() {
			ctx := r.Context()
			// Open transaction to create the store
			trans, err := database.BeginTransaction(ctx, sysOpts, sop.ForWriting)
			if err != nil {
				log.Error(fmt.Sprintf("Failed to begin transaction for scripts store creation: %v", err))
				return
			}
			// Use model.New(...) which uses the standard AI Model Store logic.
			// Calling List will trigger openStore() which calls newBtree() to create it if missing.
			ms := model.New("scripts", trans)
			if _, err := ms.List(ctx, ""); err != nil {
				// Log but don't fail, it might just be empty or initial setup nuance
				log.Info(fmt.Sprintf("Initialized 'scripts' store check: %v", err))
			}

			// Seed "demo_loop" script
			demoLoop := ai.Script{
				Name:        "demo_loop",
				Description: "Demonstrates loops and variables",
				Steps: []ai.ScriptStep{
					{
						Type:     "set",
						Variable: "items",
						Value:    "apple\nbanana\ncherry",
					},
					{
						Type:     "loop",
						List:     "items",
						Iterator: "fruit",
						Steps: []ai.ScriptStep{
							{
								Type:    "say",
								Message: "Processing {{.fruit}}...",
							},
							{
								Type:   "ask",
								Prompt: "What color is {{.fruit}}? (Answer in 1 word)",
							},
						},
					},
				},
			}
			if err := ms.Save(ctx, "general", "demo_loop", demoLoop); err != nil {
				log.Error(fmt.Sprintf("Failed to seed demo_loop script: %v", err))
			} else {
				log.Info("Seeded 'demo_loop' script into System DB.")
			}

			if err := trans.Commit(ctx); err != nil {
				log.Error(fmt.Sprintf("Failed to commit scripts store creation: %v", err))
			}
		}()

		// 2. Update Config
		config.SystemDB = &DatabaseConfig{
			Name:     SystemDBName,
			Path:     req.RegistryPath,
			IsSystem: true,
			Mode:     req.Type,
		}

		// Persist Erasure Coding settings
		var ecs []ErasureConfigEntry
		if req.ErasureConfig != nil && req.ErasureConfig.DataChunks > 0 {
			ecs = append(ecs, ErasureConfigEntry{
				Key:          "",
				DataChunks:   req.ErasureConfig.DataChunks,
				ParityChunks: req.ErasureConfig.ParityChunks,
				BasePaths:    req.ErasureConfig.BasePaths,
			})
		}
		for _, val := range req.ErasureConfigs {
			ecs = append(ecs, ErasureConfigEntry{
				Key:          val.Key,
				DataChunks:   val.DataChunks,
				ParityChunks: val.ParityChunks,
				BasePaths:    val.BasePaths,
			})
		}
		config.SystemDB.ErasureConfigs = ecs

		if config.SystemDB.Mode == "" {
			config.SystemDB.Mode = "standalone"
		}
		if req.Type == "clustered" {
			if req.ConnectionURL != "" {
				config.SystemDB.RedisURL = req.ConnectionURL
			} else {
				config.SystemDB.RedisURL = "localhost:6379"
			}
		}
	}

	// Ensure PageSize is set
	if config.PageSize == 0 {
		config.PageSize = 40
	}

	if config.ConfigFile == "" {
		config.ConfigFile = "config.json"
	}

	// Ensure directory exists
	dir := filepath.Dir(config.ConfigFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		http.Error(w, fmt.Sprintf("Failed to create directory: %v", err), http.StatusInternalServerError)
		return
	}

	// Write config file
	f, err := os.Create(config.ConfigFile)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create config file: %v", err), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "    ")
	if err := encoder.Encode(config); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write config file: %v", err), http.StatusInternalServerError)
		return
	}

	// Reload agents to reflect configuration changes
	initAgents()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Configuration saved successfully"})
}

// handleInitDatabase initializes a database folder structure and writes configuration.
func handleInitDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name            string   `json:"name"`
		Path            string   `json:"path"`
		Type            string   `json:"type"`
		Connection      string   `json:"connection"`
		PopulateDemo    bool     `json:"populate_demo"`
		RegistryHashMod int      `json:"registry_hash_mod"`
		StoresFolders   []string `json:"stores_folders"`
		UseSharedDB     bool     `json:"use_shared_db"`
		ErasureConfigs  []struct {
			Key          string   `json:"key"`
			DataChunks   int      `json:"data_chunks"`
			ParityChunks int      `json:"parity_chunks"`
			BasePaths    []string `json:"base_paths"`
		} `json:"erasure_configs"`
	}

	// Debug: Dump raw body
	bodyBytes, _ := io.ReadAll(r.Body)
	log.Debug(fmt.Sprintf("RAW INIT DB PAYLOAD: %s", string(bodyBytes)))

	// Refill body for decoder
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Debug hex dump to find source of corruption
	if req.Path != "" {
		log.Info(fmt.Sprintf("DEBUG-PATH-HEX (InitDatabase): Path='%s', Hex=%x", req.Path, []byte(req.Path)))
	}

	// Sanitize paths
	req.Path = sanitizePath(req.Path)
	for i, sf := range req.StoresFolders {
		req.StoresFolders[i] = sanitizePath(sf)
	}
	for i := range req.ErasureConfigs {
		for j, bp := range req.ErasureConfigs[i].BasePaths {
			req.ErasureConfigs[i].BasePaths[j] = sanitizePath(bp)
		}
	}

	// VALIDATION START
	{
		newPaths := []string{}
		// Deduplication logic for User DB (same as System DB fix)
		seenPaths := make(map[string]struct{})

		addPath := func(p string) {
			if p == "" {
				return
			}
			abs, err := filepath.Abs(p)
			if err != nil {
				abs = p
			}
			if _, exists := seenPaths[abs]; !exists {
				seenPaths[abs] = struct{}{}
				newPaths = append(newPaths, p)
			}
		}

		if req.Path != "" {
			addPath(req.Path)
		}
		for _, sf := range req.StoresFolders {
			addPath(sf)
		}
		for _, ec := range req.ErasureConfigs {
			for _, bp := range ec.BasePaths {
				addPath(bp)
			}
		}

		alreadyConfigured := collectAllConfiguredPaths(req.Name)
		if err := validatePathSafety(newPaths, alreadyConfigured); err != nil {
			http.Error(w, fmt.Sprintf("Path Safety Error: %v", err), http.StatusBadRequest)
			return
		}
	}
	// VALIDATION END

	if req.Path == "" || req.Name == "" {
		http.Error(w, "Database path and name are required", http.StatusBadRequest)
		return
	}

	// Construct Options
	storeFolders := []string{req.Path}
	seen := make(map[string]bool)
	seen[req.Path] = true
	if len(req.StoresFolders) > 0 {
		for _, folder := range req.StoresFolders {
			if !seen[folder] {
				storeFolders = append(storeFolders, folder)
				seen[folder] = true
			}
		}
	}

	if req.RegistryHashMod == 0 {
		req.RegistryHashMod = fs.MinimumModValue
	}

	log.Info(fmt.Sprintf("InitUserDB: Path='%s', Name='%s', StoresFolders=%v", req.Path, req.Name, storeFolders))

	options := sop.DatabaseOptions{
		StoresFolders:        storeFolders,
		RegistryHashModValue: req.RegistryHashMod,
	}

	if len(req.ErasureConfigs) > 0 {
		options.ErasureConfig = make(map[string]sop.ErasureCodingConfig)
		for _, ec := range req.ErasureConfigs {
			// Log the key properly, marking empty string clearly
			keyLog := ec.Key
			if keyLog == "" {
				keyLog = "<EMPTY_STRING>"
			}
			log.Info(fmt.Sprintf("Processing Erasure Config: Key='%s', Data=%d, Parity=%d, Paths=%v", keyLog, ec.DataChunks, ec.ParityChunks, ec.BasePaths))

			// Sanitize key if it comes in as explicitly quoted empty string
			finalKey := ec.Key
			if finalKey == `""` {
				finalKey = ""
			}

			options.ErasureConfig[finalKey] = sop.ErasureCodingConfig{
				DataShardsCount:             ec.DataChunks,
				ParityShardsCount:           ec.ParityChunks,
				BaseFolderPathsAcrossDrives: ec.BasePaths,
			}
		}
	}

	if req.Type == "clustered" {
		options.Type = sop.Clustered
		options.CacheType = sop.Redis
		redisUrl := req.Connection
		if redisUrl == "" {
			redisUrl = "localhost:6379"
		}
		options.RedisConfig = &sop.RedisCacheConfig{
			Address: redisUrl,
		}
	} else {
		options.Type = sop.Standalone
	}

	// ErasureConfig is already populated above before this block

	// Check for existing DB files to determine state (Shared vs New)
	dbOptionsPath := filepath.Join(req.Path, "dboptions.json")
	// Note: User DBs don't strictly have reghashmod.txt at root necessarily if it's purely a store,
	// but standard init creates it. We'll check dboptions.json as the primary indicator.
	_, errOpts := os.Stat(dbOptionsPath)
	hasDBOptions := !os.IsNotExist(errOpts)

	// Relaxation for User DB Reuse: Check "db" subfolder if root is missing options and user requested reuse
	if req.UseSharedDB && !hasDBOptions {
		subDBPath := filepath.Join(req.Path, "db")
		if _, err := os.Stat(filepath.Join(subDBPath, "dboptions.json")); err == nil {
			req.Path = subDBPath
			hasDBOptions = true
			log.Info(fmt.Sprintf("Relaxed User DB Path: '%s' -> '%s'", filepath.Dir(req.Path), req.Path))

			// Also update the storeFolders if they were just set to default root
			// Logic above was: storeFolders = []string{req.Path}
			// But req.Path changed.
			// Re-synch only if it was indeed just the path.
			// However, storeFolders might contain other drives.
			// For simplicity in this common case (single folder user DB), we assume the user meant the subfolder for the main store.
			// But strictly speaking, if we shift the path, we might need to shift the first element of StoreFolders.
			if len(storeFolders) > 0 && storeFolders[0] == filepath.Dir(req.Path) {
				storeFolders[0] = req.Path
				options.StoresFolders = storeFolders
			}
		}
	}

	shouldSetup := true

	if req.UseSharedDB {
		// User wants to reuse existing DB
		if hasDBOptions {
			shouldSetup = false
			log.Info(fmt.Sprintf("Shared User DB detected at '%s'. Reusing...", req.Path))

			// Check Write Permissions
			testFile := filepath.Join(req.Path, ".sop_write_test")
			if f, err := os.Create(testFile); err != nil {
				http.Error(w, fmt.Sprintf("User DB path exists but is not writable: %v", err), http.StatusBadRequest)
				return
			} else {
				f.Close()
				os.Remove(testFile)
			}
			// We don't necessarily load options here because we are about to just AddDatabase to the runtime,
			// which happens via config. However, database.Setup checks are skipped.
		} else {
			http.Error(w, fmt.Sprintf("Shared User DB selected but 'dboptions.json' is missing in '%s'.", req.Path), http.StatusBadRequest)
			return
		}
	} else {
		// New DB requested
		if hasDBOptions {
			http.Error(w, fmt.Sprintf("Destination path '%s' already contains a 'dboptions.json'. Enable 'Shared Mode' to reuse it or choose a clean path.", req.Path), http.StatusBadRequest)
			return
		}
	}

	// 1. Setup Database (Creates folders, writes dboptions.json)
	// This uses the official SOP setup routine.
	ctx := r.Context()
	if shouldSetup {
		if _, err := database.Setup(ctx, options); err != nil {
			// If the database is already setup (e.g. valid retry), legitimate warning but we can proceed
			// This branch might not be reached given the checks above, but kept for robustness against race/parallel
			if !strings.Contains(err.Error(), "already setup") {
				http.Error(w, fmt.Sprintf("Failed to setup database: %v", err), http.StatusInternalServerError)
				return
			}
			log.Warn(fmt.Sprintf("Database setup check: %v. Proceeding to populate/init...", err))
		}
	}

	// 2. Initialize Registry (Trigger reghashmod.txt creation)
	// We use the provided options to initialize the database instance.

	if req.PopulateDemo && shouldSetup {
		if err := PopulateDemoData(ctx, options); err != nil {
			http.Error(w, fmt.Sprintf("Failed to populate demo data: %v", err), http.StatusInternalServerError)
			return
		}
	} else {
		// Start a transaction to force initialization of the registry
		tx, err := database.BeginTransaction(ctx, options, sop.ForWriting)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to begin transaction: %v", err), http.StatusInternalServerError)
			return
		}

		comparer := func(a, b string) int {
			return cmp.Compare(a, b)
		}

		// Open a dummy store to force repository initialization
		_, err = database.NewBtree[string, string](ctx, options, "system_check", tx, comparer)
		if err != nil {
			tx.Rollback(ctx)
			http.Error(w, fmt.Sprintf("Failed to initialize database registry: %v", err), http.StatusInternalServerError)
			return
		}

		// We don't add anything.
		if err := tx.Commit(ctx); err != nil {
			http.Error(w, fmt.Sprintf("Failed to commit initialization transaction: %v", err), http.StatusInternalServerError)
			return
		}
	}

	// Add to global config and save
	newDB := DatabaseConfig{
		Name:          req.Name,
		Path:          req.Path,
		StoresFolders: req.StoresFolders,
		Mode:          "standalone", // Default to standalone for now
	}

	// Persist Erasure Coding settings
	var ecs []ErasureConfigEntry
	for _, val := range req.ErasureConfigs {
		ecs = append(ecs, ErasureConfigEntry{
			Key:          val.Key,
			DataChunks:   val.DataChunks,
			ParityChunks: val.ParityChunks,
			BasePaths:    val.BasePaths,
		})
	}
	newDB.ErasureConfigs = ecs

	if req.Type == "clustered" {
		newDB.Mode = "clustered"
		newDB.RedisURL = req.Connection
		if newDB.RedisURL == "" {
			newDB.RedisURL = "localhost:6379"
		}
	}

	// Check if exists
	exists := false
	for i, db := range config.Databases {
		if db.Name == newDB.Name {
			config.Databases[i] = newDB
			exists = true
			break
		}
	}
	if !exists {
		config.Databases = append(config.Databases, newDB)
	}

	// Save Config
	if config.ConfigFile != "" {
		f, err := os.Create(config.ConfigFile)
		if err == nil {
			defer f.Close()
			encoder := json.NewEncoder(f)
			encoder.SetIndent("", "    ")
			encoder.Encode(config)
		} else {
			log.Error(fmt.Sprintf("Failed to save config after db init: %v", err))
		}
	}

	// Reload agents to reflect configuration changes
	initAgents()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Database initialized successfully"})
}

// handleValidatePath checks if a path is valid/writable
func handleValidatePath(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	path := r.URL.Query().Get("path")
	if path == "" {
		http.Error(w, "Path is required", http.StatusBadRequest)
		return
	}

	// Check if exists
	info, err := os.Stat(path)
	exists := err == nil
	isDir := exists && info.IsDir()

	// Check writability (try to create a temp file)
	writable := false
	if exists && isDir {
		tmpFile := filepath.Join(path, ".write_test")
		if f, err := os.Create(tmpFile); err == nil {
			f.Close()
			os.Remove(tmpFile)
			writable = true
		}
	} else if !exists {
		// Check if parent is writable
		parent := filepath.Dir(path)
		if _, err := os.Stat(parent); err == nil {
			tmpFile := filepath.Join(parent, ".write_test")
			if f, err := os.Create(tmpFile); err == nil {
				f.Close()
				os.Remove(tmpFile)
				writable = true
			}
		}
	}

	// Check for System DB artifacts
	hasDBOptions := false
	hasRegHashMod := false
	if isDir {
		hasDBOptions, hasRegHashMod = database.IsDatabasePath(path)

		// Relaxation: Check 'system_db' subfolder
		if !hasDBOptions {
			if exists, mod := database.IsDatabasePath(filepath.Join(path, "system_db")); exists {
				hasDBOptions = true
				hasRegHashMod = mod
			}
		}

		// Debug Validation
		log.Info(fmt.Sprintf("ValidatePath: Path='%s', DBOptions=%v, RegHashMod=%v", path, hasDBOptions, hasRegHashMod))
	}

	json.NewEncoder(w).Encode(map[string]any{
		"exists":        exists,
		"isDir":         isDir,
		"writable":      writable,
		"hasDBOptions":  hasDBOptions,
		"hasRegHashMod": hasRegHashMod,
	})
}

// handleUninstallSystem removes configuration and optionally data files
func handleUninstallSystem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		DeleteData     bool `json:"delete_data"` // Legacy
		DeleteSystemDB bool `json:"delete_system_db"`
		DeleteUserDBs  bool `json:"delete_user_dbs"`
	}
	// Debug: Dump raw body
	bodyBytes, _ := io.ReadAll(r.Body)
	log.Debug(fmt.Sprintf("RAW UNINSTALL PAYLOAD: %s", string(bodyBytes)))

	// Refill body for decoder
	r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	shouldDeleteSystem := req.DeleteSystemDB || req.DeleteData
	shouldDeleteUsers := req.DeleteUserDBs || req.DeleteData

	// 1. Delete actual data folders if requested
	if shouldDeleteSystem || shouldDeleteUsers {
		// System DB
		if shouldDeleteSystem && config.SystemDB != nil && config.SystemDB.Path != "" {
			if err := os.RemoveAll(config.SystemDB.Path); err != nil {
				log.Error(fmt.Sprintf("Failed to remove system db path %s: %v", config.SystemDB.Path, err))
				// Continue anyway to try cleaning up others
			}
		}
		// User DBs
		if shouldDeleteUsers {
			for _, db := range config.Databases {
				if db.Path != "" {
					if err := os.RemoveAll(db.Path); err != nil {
						log.Error(fmt.Sprintf("Failed to remove db path %s: %v", db.Path, err))
					}
				}
			}
		}
	}

	// 2. Clear Config Object (in memory)
	config = Config{}

	// 3. Remove Config File (on disk)
	// We check the default locations (config.json) or whatever was passed flag.
	// But in handleSaveConfig we used config.ConfigFile
	configFile := config.ConfigFile
	if configFile == "" {
		configFile = "config.json"
	}
	if err := os.Remove(configFile); err != nil && !os.IsNotExist(err) {
		http.Error(w, fmt.Sprintf("Failed to remove config file: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "System uninstalled successfully"})
}

// sanitizePath removes leading/trailing whitespace and non-graphic characters
// to prevent "invisible" or garbage characters from creating bad folder names.
func sanitizePath(p string) string {
	// 1. Remove non-graphic characters (e.g. control codes, zero-width spaces)
	p = strings.Map(func(r rune) rune {
		if unicode.IsGraphic(r) {
			return r
		}
		return -1
	}, p)

	// 2. Trim whitespace
	p = strings.TrimSpace(p)

	// 3. Trim specific garbage characters user might inadvertently paste (e.g. cursor, pipe)
	// Example: "db2/|"
	p = strings.TrimRight(p, "|")

	// 4. Final trim in case stripping left whitespace
	return strings.TrimSpace(p)
}

// collectAllConfiguredPaths gathers all paths currently in use by the system and other databases,
// excluding the database currently being edited/added (by name).
func collectAllConfiguredPaths(excludeDBName string) []string {
	var paths []string

	// System DB
	if config.SystemDB != nil && config.SystemDB.Path != "" && config.SystemDB.Name != excludeDBName && excludeDBName != SystemDBName {
		paths = append(paths, config.SystemDB.Path)
		paths = append(paths, config.SystemDB.StoresFolders...)
		for _, ec := range config.SystemDB.ErasureConfigs {
			paths = append(paths, ec.BasePaths...)
		}
	}

	// User DBs
	for _, db := range config.Databases {
		if db.Name == excludeDBName {
			continue
		}
		if db.Path != "" {
			paths = append(paths, db.Path)
		}
		paths = append(paths, db.StoresFolders...)
		for _, ec := range db.ErasureConfigs {
			paths = append(paths, ec.BasePaths...)
		}
	}
	return paths
}

// validatePathSafety checks for conflicts between a set of new paths and existing system paths.
// Policy: Strict Isolation.
// We do NOT allow different databases to share the same physical paths (e.g. EC drives).
// Sharing drives creates a Single Point of Failure (SPOF) where one drive failure
// could panic/degrade multiple databases simultaneously.
// This function detects:
// 1. Internal conflicts (e.g. store folder inside db folder in the new set)
// 2. External conflicts (e.g. new db folder inside existing system db folder)
func validatePathSafety(newPaths []string, existingPaths []string) error {
	// Clean and filter new paths
	cleanNew := make([]string, 0, len(newPaths))
	for _, np := range newPaths {
		if np == "" {
			continue
		}
		abs, err := filepath.Abs(np)
		if err != nil {
			return fmt.Errorf("invalid path '%s': %v", np, err)
		}
		cleanNew = append(cleanNew, abs)
	}

	// Clean and filter existing paths
	cleanExisting := make([]string, 0, len(existingPaths))
	for _, ep := range existingPaths {
		if ep == "" {
			continue
		}
		abs, err := filepath.Abs(ep)
		if err == nil {
			cleanExisting = append(cleanExisting, abs)
		}
	}

	// 1. Check Internal Conflicts (within the new set)
	for i := 0; i < len(cleanNew); i++ {
		for j := i + 1; j < len(cleanNew); j++ {
			if conflict, msg := isPathConflict(cleanNew[i], cleanNew[j]); conflict {
				return fmt.Errorf("internal path conflict: %s", msg)
			}
		}
	}

	// 2. Check External Conflicts (new vs existing)
	for _, np := range cleanNew {
		for _, ep := range cleanExisting {
			if conflict, msg := isPathConflict(np, ep); conflict {
				return fmt.Errorf("conflict with existing path: %s", msg)
			}
		}
	}

	return nil
}

func isPathConflict(pathA, pathB string) (bool, string) {
	if pathA == pathB {
		return true, fmt.Sprintf("paths are identical (Strict Isolation Policy): '%s'", pathA)
	}

	// Ensure trailing separator for prefix check to avoid /tmp matching /tmp2
	pathASep := pathA
	if !strings.HasSuffix(pathASep, string(os.PathSeparator)) {
		pathASep += string(os.PathSeparator)
	}
	pathBSep := pathB
	if !strings.HasSuffix(pathBSep, string(os.PathSeparator)) {
		pathBSep += string(os.PathSeparator)
	}

	// Check if A is inside B
	if strings.HasPrefix(pathASep, pathBSep) {
		return true, fmt.Sprintf("'%s' is inside '%s'", pathA, pathB)
	}
	// Check if B is inside A
	if strings.HasPrefix(pathBSep, pathASep) {
		return true, fmt.Sprintf("'%s' contains '%s'", pathA, pathB)
	}

	return false, ""
}
