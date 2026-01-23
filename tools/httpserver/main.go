package main

import (
	"cmp" // Added
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	log "log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	aidb "github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/fs"
	"github.com/sharedcode/sop/jsondb"
)

// ErasureConfigEntry defines a single EC zone configuration
type ErasureConfigEntry struct {
	Key          string   `json:"key"`
	DataChunks   int      `json:"data_chunks"`
	ParityChunks int      `json:"parity_chunks"`
	BasePaths    []string `json:"base_paths"`
}

// DatabaseConfig holds configuration for a single SOP database
type DatabaseConfig struct {
	Name          string   `json:"name"`
	Path          string   `json:"path"`
	StoresFolders []string `json:"stores_folders,omitempty"`
	Mode          string   `json:"mode"`  // "standalone" or "clustered"
	RedisURL      string   `json:"redis"` // Optional, for clustered
	IsSystem      bool     `json:"is_system,omitempty"`

	// ErasureConfigs stores a list of EC configurations.
	ErasureConfigs []ErasureConfigEntry `json:"erasure_configs,omitempty"`

	// EnableObfuscation specifies if this database should be obfuscated when accessed by AI tools.
	// This allows per-database granular control.
	EnableObfuscation bool `json:"enable_obfuscation,omitempty"`

	// // DetectedRoot is the inferred root directory for relative paths in this database.
	// // It is calculated at startup and not read from JSON.
	// DetectedRoot string `json:"-"`
}

// Config holds the server configuration
type Config struct {
	Port           int              `json:"port"`
	Databases      []DatabaseConfig `json:"databases"`
	PageSize       int              `json:"pageSize"`
	SystemDB       *DatabaseConfig  `json:"system_db,omitempty"`
	RootPassword   string           `json:"root_password,omitempty"`
	EnableRestAuth bool             `json:"enable_rest_auth,omitempty"`
	StubMode       bool             `json:"stub_mode,omitempty"`

	// ObfuscationMode defines the global obfuscation policy (disabled, per_database, all_databases).
	// This overrides any setting in the agent's own configuration.
	ObfuscationMode string `json:"obfuscation_mode,omitempty"`

	// LLMApiKey is the default API key for AI Agents (e.g. Gemini).
	LLMApiKey string `json:"llm_api_key,omitempty"`

	// Legacy/CLI fields - Ignored in JSON to keep config clean
	DatabasePath string `json:"-"`
	Mode         string `json:"-"`
	ConfigFile   string `json:"-"`
	RedisURL     string `json:"-"`
}

//go:embed templates/*
var content embed.FS

var config Config
var loadedAgents = make(map[string]ai.Agent[map[string]any])

const SystemDBName = "system"

func main() {

	l := log.New(log.NewJSONHandler(os.Stdout, &log.HandlerOptions{
		Level: log.LevelDebug,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo

	var showVersion bool
	var openBrowserFlag bool
	flag.BoolVar(&showVersion, "version", false, "Show version and exit")
	flag.BoolVar(&openBrowserFlag, "open-browser", true, "Open browser on startup")
	flag.IntVar(&config.Port, "port", 8080, "Port to run the server on")
	flag.StringVar(&config.DatabasePath, "database", "/tmp/sop_data", "Path to the SOP database/data directory")
	flag.StringVar(&config.Mode, "mode", "standalone", "SOP mode: 'standalone' or 'clustered'")
	flag.StringVar(&config.ConfigFile, "config", "", "Path to configuration file (optional)")
	flag.StringVar(&config.RedisURL, "redis", "localhost:6379", "Redis URL for clustered mode (e.g. localhost:6379)")
	flag.IntVar(&config.PageSize, "pageSize", 40, "Number of items to display per page")
	flag.BoolVar(&config.EnableRestAuth, "enable-rest-auth", false, "Enable Bearer token authentication for REST endpoints")
	flag.BoolVar(&config.StubMode, "stub", false, "Enable stub mode for AI agents")
	flag.Parse()

	if showVersion {
		fmt.Printf("SOP Data Manager v%s\n", sop.Version)
		os.Exit(0)
	}

	// Load config from file if provided
	if config.ConfigFile != "" {
		if err := loadConfig(config.ConfigFile); err != nil {
			log.Error(fmt.Sprintf("Failed to load config file: %v", err))
		}
	} else {
		// Try default config.json
		if _, err := os.Stat("config.json"); err == nil {
			config.ConfigFile = "config.json"
			loadConfig("config.json")
		}
	}

	// Override RootPassword from environment variable if set (Security best practice)
	if envPass := os.Getenv("SOP_ROOT_PASSWORD"); envPass != "" {
		config.RootPassword = envPass
	}
	if os.Getenv("SOP_ENABLE_REST_AUTH") == "true" {
		config.EnableRestAuth = true
	}

	// If no databases loaded (e.g. no config file or empty), use CLI flags as default
	// BUT only if config file was NOT loaded. If config file was loaded but empty, that's a valid state (Setup Mode).
	// Actually, if config file is missing, we are in Setup Mode.
	// We only fallback to CLI flags if the user explicitly provided them?
	// Or maybe we just start empty and let the UI handle it.
	// Let's say: If config file is missing, we start with NO databases, which triggers Setup Mode in UI.
	// UNLESS the user provided a specific database path via CLI that is NOT the default.
	// The default is "/tmp/sop_data".
	isDefaultPath := config.DatabasePath == "/tmp/sop_data"
	if len(config.Databases) == 0 && config.ConfigFile == "" && !isDefaultPath {
		config.Databases = []DatabaseConfig{
			{
				Name:     "Default",
				Path:     config.DatabasePath,
				Mode:     config.Mode,
				RedisURL: config.RedisURL,
			},
		}
	}

	// Resolve Database Paths to absolute
	for i := range config.Databases {
		if abs, err := filepath.Abs(config.Databases[i].Path); err == nil {
			config.Databases[i].Path = abs
		}
		// Ensure database path exists (basic check)
		if _, err := os.Stat(config.Databases[i].Path); os.IsNotExist(err) {
			log.Warn(fmt.Sprintf("Warning: Database path '%s' for '%s' does not exist.", config.Databases[i].Path, config.Databases[i].Name))
		}
	}

	// Setup Routes
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/api/databases", handleDatabases)
	http.HandleFunc("/api/databases/update", handleUpdateDatabase)
	http.HandleFunc("/api/stores", handleListStores)
	http.HandleFunc("/api/db/options", handleGetDBOptions)
	http.HandleFunc("/api/store/info", handleGetStoreInfo)
	http.HandleFunc("/api/store/update", handleUpdateStoreInfo)
	http.HandleFunc("/api/store/items", handleListItems)
	http.HandleFunc("/api/store/item/update", handleUpdateItem)
	http.HandleFunc("/api/store/item/add", handleAddItem)
	http.HandleFunc("/api/store/add", handleAddStore)
	http.HandleFunc("/api/store/delete", handleDeleteStore)
	http.HandleFunc("/api/store/item/delete", handleDeleteItem)
	http.HandleFunc("/api/admin/validate", handleValidateAdminToken)
	http.HandleFunc("/api/ai/chat", handleAIChat)
	http.HandleFunc("/api/scripts/execute", withAuth(handleExecuteScript))

	// Configuration Endpoints
	http.HandleFunc("/api/config/save", handleSaveConfig)
	http.HandleFunc("/api/db/init", handleInitDatabase)
	http.HandleFunc("/api/config/validate-path", handleValidatePath)
	http.HandleFunc("/api/system/uninstall", handleUninstallSystem)
	http.HandleFunc("/api/config/environments", handleListEnvironments)
	http.HandleFunc("/api/config/environments/create", handleCreateEnvironment)
	http.HandleFunc("/api/config/environments/switch", handleSwitchEnvironment)
	http.HandleFunc("/api/config/environments/delete", handleDeleteEnvironment)
	http.HandleFunc("/api/system/env", handleGetSystemEnv)

	// Initialize Agents
	initAgents()

	// Start Server
	addr := fmt.Sprintf(":%d", config.Port)
	log.Info(fmt.Sprintf("SOP Data Manager v%s running at http://localhost%s", sop.Version, addr))
	for _, db := range config.Databases {
		log.Debug(fmt.Sprintf("Database '%s': %s (%s)", db.Name, db.Path, db.Mode))
	}

	// Open Browser
	if openBrowserFlag {
		go func() {
			// Wait a bit for server to start
			time.Sleep(500 * time.Millisecond)
			openBrowser(fmt.Sprintf("http://localhost:%d", config.Port))
		}()
	}

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Error(err.Error())
	}
}

func openBrowser(url string) {
	var err error
	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	if err != nil {
		log.Error(fmt.Sprintf("Failed to open browser: %v", err))
	}
}

func loadConfig(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Reset data pointers to prevent pollution from previous active configuration
	config.Databases = nil
	config.SystemDB = nil

	if err := json.NewDecoder(f).Decode(&config); err != nil {
		return err
	}

	// Default System DB logic
	// We ONLY populate SystemDB if it is explicitly defined in the config.
	// If it is nil, we leave it as nil. This allows us to distinguish between
	// "Legacy/Default" (handled by getters) and "New Empty Environment" (handled by UI Wizard).

	if config.SystemDB != nil {
		configDir := filepath.Dir(path)
		if config.SystemDB.Path == "" {
			config.SystemDB.Path = configDir
		}
		if config.SystemDB.Name == "" {
			config.SystemDB.Name = SystemDBName
		}
		if config.SystemDB.Mode == "" {
			config.SystemDB.Mode = config.Mode
		}
		if config.SystemDB.RedisURL == "" {
			config.SystemDB.RedisURL = config.RedisURL
		}
	}
	return nil
}

func getSystemDBOptions() (sop.DatabaseOptions, error) {
	// 1. If explicitly configured in config.json, use it.
	if config.SystemDB != nil {
		return getDBOptionsFromConfig(config.SystemDB)
	}

	// 2. Fallback to default (cwd)
	cwd, _ := os.Getwd()
	opts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{cwd},
	}
	if config.Mode == "clustered" {
		opts.Type = sop.Clustered
		opts.CacheType = sop.Redis
		opts.RedisConfig = &sop.RedisCacheConfig{
			Address: config.RedisURL,
		}
	}
	return opts, nil
}

// IsSystemDB checks if the given database name corresponds to the System Database.
func IsSystemDB(name string) bool {
	// Check against configured SystemDB name if available
	if config.SystemDB != nil && config.SystemDB.Name == name {
		return true
	}
	// Check known system name.
	return name == SystemDBName
}

func getDBOptionsFromConfig(db *DatabaseConfig) (sop.DatabaseOptions, error) {
	// Try to load from disk first
	if loadedOpts, err := database.GetOptions(context.Background(), db.Path); err == nil {
		// Override with runtime config if necessary (e.g. Redis address from flags)
		if db.Mode == "clustered" {
			loadedOpts.Type = sop.Clustered
			loadedOpts.CacheType = sop.Redis
			loadedOpts.RedisConfig = &sop.RedisCacheConfig{
				Address: db.RedisURL,
			}
		}
		// Ensure we use the loaded options' StoresFolders if available, as they might contain the full list of folders
		// including those for erasure coding, which are critical for finding the data.
		return loadedOpts, nil
	}

	opts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{db.Path},
	}
	// We rely on SOP to handle path resolution.
	// If the store uses Erasure Coding, the config in StoreInfo should guide SOP to the correct files.
	// If we override StoresFolders here, we risk breaking Database discovery.

	if db.Mode == "clustered" {
		opts.Type = sop.Clustered
		opts.CacheType = sop.Redis
		opts.RedisConfig = &sop.RedisCacheConfig{
			Address: db.RedisURL,
		}
	}
	return opts, nil
}

func getDBOptions(dbName string) (sop.DatabaseOptions, error) {
	if dbName == "" {
		if len(config.Databases) > 0 {
			// Return options for the first database
			return getDBOptionsFromConfig(&config.Databases[0])
		}
		// If no databases, falls through to error or should we return SystemDB?
		// Stick to current behavior: likely error if no default found here.
	} else {
		// 1. Check if it explicitly matches the Configured System DB Name
		if config.SystemDB != nil && config.SystemDB.Name == dbName {
			return getDBOptionsFromConfig(config.SystemDB)
		}

		// 2. Check for System Database
		// Delegated to getSystemDBOptions to handle default/env configuration
		if dbName == SystemDBName {
			return getSystemDBOptions()
		}

		// 3. Check User Databases
		for i := range config.Databases {
			if config.Databases[i].Name == dbName {
				return getDBOptionsFromConfig(&config.Databases[i])
			}
		}
	}

	return sop.DatabaseOptions{}, fmt.Errorf("database '%s' not found", dbName)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(content, "templates/*.html")
	if err != nil {
		http.Error(w, "Could not load template: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Check if "users" store exists in any of the configured databases
	hasDemo := false
	ctx := r.Context()
	for _, dbCfg := range config.Databases {
		if dbOpts, err := getDBOptionsFromConfig(&dbCfg); err == nil {
			// Use a lightweight check if possible, or just try to open a transaction
			// Since we just added StoreExists to Database, we can use it if we had a Database object.
			// But here we have DatabaseOptions.
			// Let's create a temporary Database object to check.
			db := aidb.NewDatabase(dbOpts)
			if exists, err := db.StoreExists(ctx, "users"); err == nil && exists {
				hasDemo = true
				break
			}
		}
	}

	// Check if SystemDB path matches SYSTEM_DB_PATH environment variable (Enterprise Mode)
	isEnterprise := false
	if sysDBPath := os.Getenv("SYSTEM_DB_PATH"); sysDBPath != "" && config.SystemDB != nil {
		// Compare absolute paths to be safe
		absSysDB, err1 := filepath.Abs(config.SystemDB.Path)
		absEnv, err2 := filepath.Abs(sysDBPath)
		if err1 == nil && err2 == nil && absSysDB == absEnv {
			isEnterprise = true
		}
	}

	data := map[string]any{
		"Version": sop.Version,
		"Mode":    config.Mode,
		// AllowInvalidMapKey is a flag to bypass the validation that requires Map Key types
		// to have an Index Specification or CEL Expression. This is useful for testing.
		"AllowInvalidMapKey": os.Getenv("SOP_ALLOW_INVALID_MAP_KEY") == "true",
		"HasDemo":            hasDemo,
		"IsEnterprise":       isEnterprise,
		"SystemDBName":       SystemDBName,
		"ConfigFile":         config.ConfigFile,
		"MinHashMod":         fs.MinimumModValue,
		"MaxHashMod":         fs.MaximumModValue,
		"Env": map[string]bool{
			"SOP_ROOT_PASSWORD": os.Getenv("SOP_ROOT_PASSWORD") != "",
			"GEMINI_API_KEY":    os.Getenv("GEMINI_API_KEY") != "",
			"SYSTEM_DB_PATH":    os.Getenv("SYSTEM_DB_PATH") != "",
		},
	}
	if err := tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		log.Error("Failed to execute template", "error", err)
		http.Error(w, "Template execution failed", http.StatusInternalServerError)
	}
}

func handleDatabases(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.Header().Set("Content-Type", "application/json")

	// GET: List Databases
	if r.Method == http.MethodGet {
		dbs := make([]DatabaseConfig, len(config.Databases))
		copy(dbs, config.Databases)

		if config.SystemDB != nil {
			sysDB := *config.SystemDB
			if sysDB.Name == "" {
				sysDB.Name = SystemDBName
			}
			sysDB.IsSystem = true
			dbs = append(dbs, sysDB)
		} else if len(config.Databases) > 0 {
			// Backward Compatibility:
			// If we have user databases but no explicit SystemDB, we include the implicit one.
			// This prevents existing setups from "losing" their System DB in the UI.
			// But if Databases is EMPTY, we assume it's a fresh env and return nothing, triggering the Wizard.

			// We can reconstruct the implicit path logic or just ignore it for the list?
			// Generally, if it's implicit, it's in the CWD (or config dir).
			// Let's add it to key users happy.

			// Determine implicit path
			implicitPath := "."
			if config.ConfigFile != "" {
				implicitPath = filepath.Dir(config.ConfigFile)
			}

			dbs = append(dbs, DatabaseConfig{
				Name:     SystemDBName,
				Path:     implicitPath,
				Mode:     config.Mode,
				IsSystem: true,
			})
		}

		json.NewEncoder(w).Encode(dbs)
		return
	}

	// DELETE: Remove Database
	if r.Method == http.MethodDelete {
		name := r.URL.Query().Get("name")
		deleteDataStr := r.URL.Query().Get("delete_data")
		deleteData := deleteDataStr != "false" // Default to true if not specified or "true"

		if name == "" {
			http.Error(w, "Database name is required", http.StatusBadRequest)
			return
		}

		found := -1
		var dbPath string
		isSystem := false

		for i, db := range config.Databases {
			if db.Name == name {
				found = i
				dbPath = db.Path
				break
			}
		}

		if found == -1 && config.SystemDB != nil {
			sysName := config.SystemDB.Name
			if sysName == "" {
				sysName = SystemDBName
			}
			if sysName == name {
				isSystem = true
				dbPath = config.SystemDB.Path
			}
		}

		if found == -1 && !isSystem {
			http.Error(w, "Database not found", http.StatusNotFound)
			return
		}

		// Delete the database.
		if deleteData {
			if err := database.Remove(r.Context(), dbPath); err != nil {
				log.Warn(fmt.Sprintf("Cleanup: Failed to remove database (some metadata may persist): %v", err))
			}
		} else {
			log.Info(fmt.Sprintf("Database '%s' removed from config only. Data at '%s' preserved.", name, dbPath))
		}

		// Remove from config
		if found != -1 {
			config.Databases = append(config.Databases[:found], config.Databases[found+1:]...)
		} else if isSystem {
			// Clearing SystemDB means setting it to nil
			config.SystemDB = nil
		}

		// Save Config
		if config.ConfigFile != "" {
			saveConfigFile()
		}

		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}

	// POST: Create Database
	if r.Method == http.MethodPost {
		var req struct {
			Name            string   `json:"name"`
			Path            string   `json:"path"`
			Type            string   `json:"type"`
			Connection      string   `json:"conn_url"`
			StoresFolders   []string `json:"stores_folders"`
			RegistryHashMod int      `json:"registry_hash_mod"`
			PopulateDemo    bool     `json:"populate_demo"`
			UseSharedDB     bool     `json:"use_shared_db"`
			ErasureConfigs  []struct {
				Key          string   `json:"key"`
				DataChunks   int      `json:"data_chunks"`
				ParityChunks int      `json:"parity_chunks"`
				BasePaths    []string `json:"base_paths"`
			} `json:"erasure_configs"`
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if req.Name == "" || req.Path == "" {
			http.Error(w, "Name and Path are required", http.StatusBadRequest)
			return
		}

		if req.RegistryHashMod == 0 {
			req.RegistryHashMod = fs.MinimumModValue
		}

		// Sanitize paths
		req.Path = sanitizePath(req.Path)
		for i, sf := range req.StoresFolders {
			req.StoresFolders[i] = sanitizePath(sf)
		}

		// Check for existing DB files to determine state (Shared vs New)
		dbOptionsPath := filepath.Join(req.Path, "dboptions.json")
		_, errOpts := os.Stat(dbOptionsPath)
		hasDBOptions := !os.IsNotExist(errOpts)

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

		// Prepare Options
		// Prepare Options
		options := sop.DatabaseOptions{
			RegistryHashModValue: req.RegistryHashMod,
		}
		// If explicit Stores Folders provided (Advanced Mode), use them.
		// Otherwise use the Data Path (Simple Mode).
		if len(req.StoresFolders) > 0 {
			options.StoresFolders = req.StoresFolders
		} else {
			options.StoresFolders = []string{req.Path}
		}

		// Set Database Type and Cache Config
		if req.Type == "clustered" {
			options.Type = sop.Clustered
			options.CacheType = sop.Redis
			if req.Connection != "" {
				options.RedisConfig = &sop.RedisCacheConfig{
					URL: req.Connection,
				}
			} else {
				http.Error(w, "Redis Connection URL is required for Clustered mode", http.StatusBadRequest)
				return
			}
		} else {
			options.Type = sop.Standalone
			options.CacheType = sop.InMemory
		}

		// Set Erasure Config if provided
		if len(req.ErasureConfigs) > 0 {
			options.ErasureConfig = make(map[string]sop.ErasureCodingConfig)
			for _, ec := range req.ErasureConfigs {
				options.ErasureConfig[ec.Key] = sop.ErasureCodingConfig{
					DataShardsCount:             ec.DataChunks,
					ParityShardsCount:           ec.ParityChunks,
					BaseFolderPathsAcrossDrives: ec.BasePaths,
				}
			}
		}

		ctx := r.Context()

		if shouldSetup {
			// 1. Setup Database
			if _, err := database.Setup(ctx, options); err != nil {
				http.Error(w, "Failed to setup database: "+err.Error(), http.StatusInternalServerError)
				return
			}

			// 2. Initialize Registry (Create dummy store)
			tx, err := database.BeginTransaction(ctx, options, sop.ForWriting)
			if err != nil {
				http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
				return
			}

			comparer := func(a, b string) int {
				return cmp.Compare(a, b)
			}

			if _, err = database.NewBtree[string, string](ctx, options, "system_check", tx, comparer); err != nil {
				tx.Rollback(ctx)
				http.Error(w, "Failed to initialize registry: "+err.Error(), http.StatusInternalServerError)
				return
			}

			tx.Commit(ctx)

			// Clean up the dummy store (best effort)
			if err := database.RemoveBtree(ctx, options, "system_check"); err != nil {
				// It's okay if this fails, it's just a clean up.
				log.Warn(fmt.Sprintf("Cleanup: Failed to remove init store 'system_check': %v", err))
			}
		}

		if req.PopulateDemo && shouldSetup {
			if err := PopulateDemoData(ctx, options); err != nil {
				log.Error("Failed to populate demo data: " + err.Error())
			}
		}

		// 3. Update Config
		newDB := DatabaseConfig{
			Name:     req.Name,
			Path:     req.Path,
			Mode:     "standalone",
			RedisURL: req.Connection,
		}
		if req.Type == "clustered" {
			newDB.Mode = "clustered"
		}

		// Copy Erasure Configs
		for _, ec := range req.ErasureConfigs {
			newDB.ErasureConfigs = append(newDB.ErasureConfigs, struct {
				Key          string   `json:"key"`
				DataChunks   int      `json:"data_chunks"`
				ParityChunks int      `json:"parity_chunks"`
				BasePaths    []string `json:"base_paths"`
			}{
				Key:          ec.Key,
				DataChunks:   ec.DataChunks,
				ParityChunks: ec.ParityChunks,
				BasePaths:    ec.BasePaths,
			})
		}

		// Check duplication
		for _, db := range config.Databases {
			if db.Name == newDB.Name {
				http.Error(w, "Database with this name already exists", http.StatusConflict)
				return
			}
		}

		config.Databases = append(config.Databases, newDB)

		if config.ConfigFile != "" {
			saveConfigFile()
		}

		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		return
	}
}

func saveConfigFile() {
	if config.ConfigFile == "" {
		return
	}
	f, err := os.Create(config.ConfigFile)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to save config: %v", err))
		return
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "    ")
	encoder.Encode(config)
}

func handleUpdateDatabase(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Name           string `json:"name"`
		ErasureConfigs []struct {
			Key          string   `json:"key"`
			DataChunks   int      `json:"data_chunks"`
			ParityChunks int      `json:"parity_chunks"`
			BasePaths    []string `json:"base_paths"`
		} `json:"erasure_configs"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	dbIdx := -1
	for i, db := range config.Databases {
		if db.Name == req.Name {
			dbIdx = i
			break
		}
	}

	if dbIdx == -1 {
		http.Error(w, "Database not found", http.StatusNotFound)
		return
	}

	// Validate (No Deletions allowed for now)
	currentKeys := make(map[string]bool)
	for _, ec := range config.Databases[dbIdx].ErasureConfigs {
		currentKeys[ec.Key] = true
	}

	newKeys := make(map[string]bool)
	for _, ec := range req.ErasureConfigs {
		newKeys[ec.Key] = true
	}

	for k := range currentKeys {
		if !newKeys[k] {
			http.Error(w, fmt.Sprintf("Deletion of Data File entry '%s' is not allowed.", k), http.StatusForbidden)
			return
		}
	}

	// Apply Update
	var newConfigs []ErasureConfigEntry

	for _, ec := range req.ErasureConfigs {
		newConfigs = append(newConfigs, ErasureConfigEntry{
			Key:          ec.Key,
			DataChunks:   ec.DataChunks,
			ParityChunks: ec.ParityChunks,
			BasePaths:    ec.BasePaths,
		})
	}

	// VALIDATION START
	{
		targetDB := config.Databases[dbIdx]
		proposedPaths := []string{}
		if targetDB.Path != "" {
			proposedPaths = append(proposedPaths, targetDB.Path)
		}
		proposedPaths = append(proposedPaths, targetDB.StoresFolders...)
		for _, ec := range newConfigs {
			proposedPaths = append(proposedPaths, ec.BasePaths...)
		}

		if err := validatePathSafety(proposedPaths, collectAllConfiguredPaths(req.Name)); err != nil {
			http.Error(w, fmt.Sprintf("Path Safety Error: %v", err), http.StatusBadRequest)
			return
		}
	}
	// VALIDATION END

	config.Databases[dbIdx].ErasureConfigs = newConfigs

	if config.ConfigFile != "" {
		saveConfigFile()
	}

	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleListStores(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	dbName := r.URL.Query().Get("database")
	ctx := r.Context()
	dbOpts, err := getDBOptions(dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Open a read-only transaction to fetch stores
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	stores, err := trans.GetStores(ctx)
	if err != nil {
		http.Error(w, "Failed to list stores: "+err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(stores)
}

func handleGetDBOptions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	dbName := r.URL.Query().Get("database")
	dbOpts, err := getDBOptions(dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dbOpts)
}

func inferType(v any) (string, bool) {
	if v == nil {
		return "string", false
	}

	// Handle JSON number (float64) which might be int
	if f, ok := v.(float64); ok {
		if float64(int64(f)) == f {
			return "int", false
		}
		return "float64", false
	}

	switch v.(type) {
	case string:
		return "string", false
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "int", false
	case float32:
		return "float64", false
	case bool:
		return "bool", false
	}

	val := reflect.ValueOf(v)
	kind := val.Kind()

	if kind == reflect.Map {
		return "map", false
	}

	if kind == reflect.Slice || kind == reflect.Array {
		// Check for byte slice -> blob
		if val.Type().Elem().Kind() == reflect.Uint8 {
			return "blob", false // Blob is treated as a type, not array of bytes
		}
		// Otherwise it's an array of something
		if val.Len() > 0 {
			elem := val.Index(0).Interface()
			t, _ := inferType(elem)
			return t, true
		}
		return "string", true
	}

	return "string", false
}

func handleGetStoreInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// We need to open the store to get its info.
	// We can use a dummy comparer since we are not doing any operations.
	comparer := func(a, b any) int { return 0 }
	store, err := database.OpenBtree[any, any](ctx, dbOpts, storeName, trans, comparer)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v", storeName, err), http.StatusInternalServerError)
		return
	}

	si := store.GetStoreInfo()

	// Prepare response
	response := map[string]any{
		"name":           si.Name,
		"description":    si.Description,
		"count":          si.Count,
		"isPrimitiveKey": si.IsPrimitiveKey,
		"celExpression":  si.CELexpression,
		"slotLength":     si.SlotLength,
		"isUnique":       si.IsUnique,
		"isValueInNode":  si.IsValueDataInNodeSegment,
		// isValueActivelyPersisted is used by the UI to determine if the Data Size is "Big".
		"isValueActivelyPersisted": si.IsValueDataActivelyPersisted,
		"cacheDuration":            int(si.CacheConfig.ValueDataCacheDuration.Minutes()),
		"relations":                si.Relations,
	}

	if !si.IsPrimitiveKey {
		if si.MapKeyIndexSpecification != "" {
			var is jsondb.IndexSpecification
			if err := encoding.DefaultMarshaler.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
				response["indexSpec"] = is
			}
		}
	}

	// Fetch a sample key to infer structure/type for UI if store is not empty
	keyType := "string"
	valueType := "string"
	keyIsArray := false
	valueIsArray := false

	if si.Count > 0 {
		if ok, _ := store.First(ctx); ok {
			k := store.GetCurrentKey().Key
			response["sampleKey"] = k
			keyType, keyIsArray = inferType(k)

			if v, err := store.GetCurrentValue(ctx); err == nil {
				response["sampleValue"] = v
				valueType, valueIsArray = inferType(v)
			}
		}
	} else {
		if !si.IsPrimitiveKey {
			keyType = "map"
		}
		// If primitive and empty, we default to "string" in UI,
		// but we don't really know if it was intended to be int.
		// StoreInfo doesn't persist "int" vs "string", only "IsPrimitiveKey".
		// So "string" default is acceptable.
	}

	response["keyType"] = keyType
	response["valueType"] = valueType
	response["keyIsArray"] = keyIsArray
	response["valueIsArray"] = valueIsArray

	json.NewEncoder(w).Encode(response)
}

func normalizeJSON(s string) string {
	var j interface{}
	if err := json.Unmarshal([]byte(s), &j); err != nil {
		return s
	}
	b, _ := json.Marshal(j)
	return string(b)
}

// handleUpdateStoreInfo handles updates to store metadata and structure.
//
// EDITING RULES:
// 1. Empty Store (Count == 0):
//   - Full structural rebuild is allowed.
//   - Can change Key Type, Value Type, Slot Length, IsUnique, etc.
//   - Can add Seed Data (which effectively sets the type for inference).
//   - No Admin Token required.
//
// 2. Non-Empty Store (Count > 0):
//   - Structural fields are LOCKED (Key/Value Type, Slot Length, IsUnique).
//   - EXCEPTION: Index Specification and CEL Expression can be updated IF AND ONLY IF they are currently empty (missing).
//     This allows "fixing" stores created via code that lack these definitions.
//   - Description and Cache Configuration are ALWAYS editable.
//   - Admin Token can override locks (though UI may not expose this).
//
// handleUpdateStoreInfo handles updates to store metadata and structure.
//
// EDITING RULES:
// 1. Empty Store (Count == 0):
//   - Rebuild allowed for: Key Type, Value Type, Index Spec, CEL, Seed Data.
//   - LOCKED: Slot Length, IsUnique, ValueInNode (require Admin Token).
//   - No Admin Token required for allowed fields.
//
// 2. Non-Empty Store (Count > 0):
//   - Structural fields are LOCKED (Key/Value Type, Slot Length, IsUnique).
//   - EXCEPTION: Index Specification and CEL Expression can be updated IF AND ONLY IF they are currently empty (missing).
//     This allows "fixing" stores created via code that lack these definitions.
//   - Description and Cache Configuration are ALWAYS editable.
//   - Admin Token can override locks (though UI may not expose this).
func handleUpdateStoreInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Database      string         `json:"database"`
		StoreName     string         `json:"storeName"`
		CelExpression string         `json:"celExpression"`
		Description   string         `json:"description"`
		IndexSpec     *string        `json:"indexSpec"`
		KeyType       string         `json:"keyType"` // "map" or "primitive" (string, int, etc)
		SeedKey       any            `json:"seedKey"`
		SeedValue     any            `json:"seedValue"`
		AdminToken    string         `json:"adminToken"`
		SlotLength    int            `json:"slotLength"`
		IsUnique      bool           `json:"isUnique"`
		DataSize      int            `json:"dataSize"` // 0=Small, 1=Medium, 2=Big
		CacheDuration int            `json:"cacheDuration"`
		IsCacheTTL    bool           `json:"isCacheTTL"`
		ValueType     string         `json:"valueType"`
		Relations     []sop.Relation `json:"relations"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Error("Failed to decode JSON", "error", err)
		http.Error(w, fmt.Sprintf("Invalid JSON body: %v", err), http.StatusBadRequest)
		return
	}

	// Hardening: Prevent modifying stores in System DB
	if IsSystemDB(req.Database) {
		http.Error(w, "Access Denied: Modifying store configuration in the System DB is not allowed.", http.StatusForbidden)
		return
	}

	// We need to handle IndexSpec as a string (JSON) or object.
	// Since the frontend sends it as a JSON string (via JSON.stringify), we should decode it as string first,
	// OR fix the frontend to send it as an object.
	// The frontend sends: indexSpec: JSON.stringify(...) -> which is a string.
	// So req.IndexSpec should be *string.

	if req.StoreName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(req.Database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Always rollback if not committed.
	defer trans.Rollback(ctx)

	// Open the store to get current info
	comparer := func(a, b any) int { return 0 }
	store, err := database.OpenBtree[any, any](ctx, dbOpts, req.StoreName, trans, comparer)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v", req.StoreName, err), http.StatusInternalServerError)
		return
	}

	si := store.GetStoreInfo()

	// DEBUG LOGGING
	log.Info("UpdateStoreInfo", "Name", req.StoreName, "Desc", req.Description)
	log.Info("REQ", "Cel", req.CelExpression, "IndexSpec", req.IndexSpec, "SeedValue", req.SeedValue)
	log.Info("SI", "Cel", si.CELexpression, "IndexSpec", si.MapKeyIndexSpecification)
	if req.IndexSpec != nil {
		log.Info("IndexSpec Diff", "REQ", *req.IndexSpec, "SI", si.MapKeyIndexSpecification)
	}

	// Update Description (Allowed for all stores)
	si.Description = req.Description
	si.Relations = req.Relations

	// Compute StoreOptions based on DataSize
	// 0=Small, 1=Medium, 2=Big
	dataSize := sop.ValueDataSize(req.DataSize)
	computedOpts := sop.ConfigureStore(req.StoreName, req.IsUnique, req.SlotLength, req.Description, dataSize, "")

	// Update Cache Config
	// IMPORTANT:
	// - UI only configures Value cache duration + TTL.
	// - Do NOT overwrite other cache settings; SOP defaults should remain intact.
	// - When Value data is stored in-node, Value caching is not applicable; ignore cache updates.
	isCacheTTL := req.IsCacheTTL
	newDuration := time.Duration(req.CacheDuration) * time.Minute

	// Handle "No Cache" (-1 from UI)
	if req.CacheDuration < 0 {
		newDuration = 0
		isCacheTTL = false
	} else if newDuration == 0 {
		// 0 means default, but here we might want to respect what ConfigureStore returned or keep existing?
		// Actually, UI sends 0 for "Use Default".
		// But wait, ConfigureStore sets defaults.
		// If user sends 0, we should probably stick to what ConfigureStore gave us OR what is currently there?
		// Let's assume 0 means "don't change" or "default".
		// If we are updating, we should probably respect the computedOpts defaults if it's a structural change,
		// or keep existing if not.
		// However, the requirement says: "When size is Medium... allow user to convey 'no caching' (-1)..."
		// So if 0, it implies default.
		isCacheTTL = false
	}

	// Validate that no structural changes are attempted, unless authorized as root
	var structuralChange bool
	var shouldAddSeed bool

	// Check for Structural Changes (SlotLength, IsUnique, ValueInNode)
	slotLengthChanged := req.SlotLength > 0 && req.SlotLength != si.SlotLength
	isUniqueChanged := req.IsUnique != si.IsUnique

	// Compare computed options with current SI
	isValueInNodeChanged := computedOpts.IsValueDataInNodeSegment != si.IsValueDataInNodeSegment
	isActivelyPersistedChanged := computedOpts.IsValueDataActivelyPersisted != si.IsValueDataActivelyPersisted
	isGloballyCachedChanged := computedOpts.IsValueDataGloballyCached != si.IsValueDataGloballyCached

	if slotLengthChanged || isUniqueChanged || isValueInNodeChanged || isActivelyPersistedChanged || isGloballyCachedChanged {
		if si.Count > 0 {
			http.Error(w, "Structural fields (SlotLength, IsUnique, Data Size) cannot be changed for non-empty stores.", http.StatusBadRequest)
			return
		}
		if slotLengthChanged {
			si.SlotLength = req.SlotLength
		}
		if isUniqueChanged {
			si.IsUnique = req.IsUnique
		}

		// Apply computed options
		si.IsValueDataInNodeSegment = computedOpts.IsValueDataInNodeSegment
		si.IsValueDataActivelyPersisted = computedOpts.IsValueDataActivelyPersisted
		si.IsValueDataGloballyCached = computedOpts.IsValueDataGloballyCached

		structuralChange = true
	}

	// Apply Cache Config if allowed (Medium Data)
	if dataSize == sop.MediumData {
		if req.CacheDuration == -1 {
			// User explicitly wants to disable caching
			si.CacheConfig.ValueDataCacheDuration = -1 * time.Minute
			si.CacheConfig.IsValueDataCacheTTL = false
		} else if req.CacheDuration > 0 {
			// User specified a duration
			si.CacheConfig.ValueDataCacheDuration = time.Duration(req.CacheDuration) * time.Minute
			si.CacheConfig.IsValueDataCacheTTL = isCacheTTL
		} else {
			// req.CacheDuration == 0. Use Default?
			// If we are updating, and user sends 0, maybe they mean "don't change" or "reset to default"?
			// In the context of "Advanced Mode" dropdowns, usually 0 is "Default".
			// If I want to reset to default:
			si.CacheConfig.ValueDataCacheDuration = computedOpts.CacheConfig.ValueDataCacheDuration
			si.CacheConfig.IsValueDataCacheTTL = computedOpts.CacheConfig.IsValueDataCacheTTL
		}
	} else {
		// For Small and Big, enforce computed defaults
		si.CacheConfig.ValueDataCacheDuration = computedOpts.CacheConfig.ValueDataCacheDuration
		si.CacheConfig.IsValueDataCacheTTL = computedOpts.CacheConfig.IsValueDataCacheTTL
	}

	// Check for other changes
	celChanged := req.CelExpression != si.CELexpression

	var indexSpecChanged bool
	if req.IndexSpec != nil {
		normalizedReq := normalizeJSON(*req.IndexSpec)
		normalizedSI := normalizeJSON(si.MapKeyIndexSpecification)
		indexSpecChanged = normalizedReq != normalizedSI
	}

	seedValueChanged := req.SeedValue != nil

	if celChanged || indexSpecChanged || seedValueChanged {
		// Allow setting Index/CEL if they are currently empty (fixing a store created without them)
		// Also allow updating Seed Value (Schema) as it's just metadata for the UI.
		// Otherwise, require Admin Token for structural changes.
		// One-time fix behavior:
		// - Non-empty store: IndexSpec may be set only if currently missing.
		// - CEL is also structural, and is only allowed when Index is editable.
		//   That means: for non-empty stores, CEL can only be set when IndexSpec is missing.
		isFixingMissingIndex := indexSpecChanged && si.MapKeyIndexSpecification == ""
		isFixingMissingCel := celChanged && si.CELexpression == "" && si.MapKeyIndexSpecification == ""
		isFixingMissingSpec := isFixingMissingIndex || isFixingMissingCel

		// We allow seed value changes freely as they don't affect the B-Tree structure, only UI hints.
		isSeedChangeOnly := seedValueChanged && !celChanged && !indexSpecChanged

		// STRICT RULE: If store is not empty, we ONLY allow fixing MISSING specs.
		// If a spec already exists, it CANNOT be changed, even with Admin Token.
		// This prevents corruption of existing B-Tree data.

		// Check if we are trying to change an EXISTING spec on a non-empty store
		isModifyingExistingSpec := false
		if si.Count > 0 {
			if celChanged && si.CELexpression != "" {
				isModifyingExistingSpec = true
			}
			if indexSpecChanged && si.MapKeyIndexSpecification != "" {
				isModifyingExistingSpec = true
			}
		}

		isAdmin := config.RootPassword != "" && req.AdminToken == config.RootPassword

		if isModifyingExistingSpec {
			if !isAdmin {
				http.Error(w, "Cannot modify existing Index Specification or CEL Expression on a non-empty store. This action risks data corruption. Please delete and recreate the store if you need to change its structure.", http.StatusBadRequest)
				return
			}
			log.Warn("Admin Token used to modify existing Index/CEL on non-empty store", "Store", req.StoreName)
		}

		// If IndexSpec already exists on a non-empty store, CEL is locked even if missing.
		if si.Count > 0 && celChanged && si.CELexpression == "" && si.MapKeyIndexSpecification != "" {
			if !isAdmin {
				http.Error(w, "Cannot set CEL Expression on a non-empty store that already has an Index Specification. This action is intentionally blocked to avoid structural mistakes. Use manual file edit (storeinfo.txt) or recreate the store.", http.StatusBadRequest)
				return
			}
		}

		authorized := (si.Count == 0) || isFixingMissingSpec || isSeedChangeOnly || isAdmin

		if !authorized {
			if celChanged {
				http.Error(w, "Cannot update existing CEL Expression on a non-empty store. Please delete and recreate the store if you need to change its structure.", http.StatusBadRequest)
				return
			}
			if indexSpecChanged {
				http.Error(w, "Cannot update existing Index Specification on a non-empty store. Please delete and recreate the store if you need to change its structure.", http.StatusBadRequest)
				return
			}
			// Seed value check removed as it is now allowed or covered by authorized
		}
		// Apply authorized changes
		if celChanged {
			// If fixing, ensure we don't overwrite existing if not authorized (though logic above handles it)
			si.CELexpression = req.CelExpression
			structuralChange = true
		}
		if indexSpecChanged {
			si.MapKeyIndexSpecification = *req.IndexSpec
			structuralChange = true
		}
		if seedValueChanged {
			// We don't persist SeedValue in StoreInfo directly as it's inferred from the first item.
			// So we update the first item if it exists, or add it if empty.
			if si.Count > 0 {
				if ok, _ := store.First(ctx); ok {
					key := store.GetCurrentKey().Key
					// Only update if value changed significantly?
					// For now, we trust the user wants to update the sample value.
					if _, err := store.Update(ctx, key, req.SeedValue); err != nil {
						http.Error(w, fmt.Sprintf("Failed to update sample item with new value: %v", err), http.StatusInternalServerError)
						return
					}
					structuralChange = true
				}
			} else {
				// If empty, we can't update an item.
				// But handleAddStore adds it. Here we are updating.
				// If the store is empty, we could add the seed item?
				// But we don't have the SeedKey here easily (it's in req but might be nil if not sent).
				// If req.SeedKey is provided, we can add.
				if req.SeedKey != nil {
					// Defer adding seed data until after metadata is saved to avoid overwrite issues
					shouldAddSeed = true
					structuralChange = true
				}
			}
		}
	}
	if req.KeyType != "" {
		isPrimitive := req.KeyType != "map"
		if si.IsPrimitiveKey != isPrimitive {
			// We allow Key Type metadata change even on non-empty stores as it is a hint for bindings.
			// It does not affect the physical B-Tree structure in Go.
			si.IsPrimitiveKey = isPrimitive
			structuralChange = true
		}
	}

	// Update StoreInfo via StoreRepository
	t := trans.GetPhasedTransaction()
	if ct, ok := t.(*common.Transaction); ok {
		if _, err := ct.StoreRepository.Update(ctx, []sop.StoreInfo{si}); err != nil {
			http.Error(w, "Failed to update store info: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		http.Error(w, "Transaction type not supported for update", http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Failed to commit transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}

	if shouldAddSeed {
		// Start new transaction for data
		trans, err = database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
		if err != nil {
			http.Error(w, "Failed to begin transaction for seed data: "+err.Error(), http.StatusInternalServerError)
			return
		}
		defer trans.Rollback(ctx)

		// Re-open store (will pick up new metadata)
		store, err = database.OpenBtree[any, any](ctx, dbOpts, req.StoreName, trans, comparer)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to open store for seed data: %v", err), http.StatusInternalServerError)
			return
		}

		if _, err := store.Add(ctx, req.SeedKey, req.SeedValue); err != nil {
			http.Error(w, fmt.Sprintf("Failed to add seed item: %v", err), http.StatusInternalServerError)
			return
		}

		if err := trans.Commit(ctx); err != nil {
			http.Error(w, "Failed to commit seed data: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	resp := map[string]string{"status": "ok"}
	if structuralChange {
		resp["warning"] = "Overriding store metadata is safe but you need to make sure the new metadata being saved captures the correct sorting characteristics of this store."
	}
	json.NewEncoder(w).Encode(resp)
}

func handleListItems(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	dbName := r.URL.Query().Get("database")
	if storeName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// Variables to hold state for the closure
	var indexSpec *jsondb.IndexSpecification
	var isPrimitiveKey bool

	// Check if primitive key before opening
	if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, storeName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
			if !isPrimitiveKey && stores[0].MapKeyIndexSpecification != "" {
				var is jsondb.IndexSpecification
				if err := encoding.DefaultMarshaler.Unmarshal([]byte(stores[0].MapKeyIndexSpecification), &is); err == nil {
					indexSpec = &is
				}
			}
		}
	}

	var comparer btree.ComparerFunc[any]

	if !isPrimitiveKey {
		// Proxy comparer
		comparer = func(a, b any) int {
			mapA, okA := a.(map[string]any)
			mapB, okB := b.(map[string]any)
			if !okA || !okB {
				return btree.Compare(a, b)
			}

			if indexSpec != nil {
				return indexSpec.Comparer(mapA, mapB)
			}

			// Default Map Comparer (Dynamic)
			// Collect all keys, sort them, compare values.
			keys := make([]string, 0, len(mapA)+len(mapB))
			seen := make(map[string]struct{})
			for k := range mapA {
				if _, exists := seen[k]; !exists {
					keys = append(keys, k)
					seen[k] = struct{}{}
				}
			}
			for k := range mapB {
				if _, exists := seen[k]; !exists {
					keys = append(keys, k)
					seen[k] = struct{}{}
				}
			}
			// We need to sort keys to be deterministic
			sort.Strings(keys)

			for _, k := range keys {
				valA, existsA := mapA[k]
				valB, existsB := mapB[k]

				if !existsA && !existsB {
					continue
				}
				if !existsA {
					return -1 // A is missing key, so A < B
				}
				if !existsB {
					return 1 // B is missing key, so A > B
				}

				res := btree.Compare(valA, valB)
				if res != 0 {
					return res
				}
			}
			return 0
		}
	}

	// Open the B-Tree using 'any' for Key and Value to support generic browsing.
	store, err := database.OpenBtree[any, any](ctx, dbOpts, storeName, trans, comparer)
	if err != nil {
		log.Error(fmt.Sprintf("Failed to open store '%s': %v.", storeName, err))
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v.", storeName, err), http.StatusInternalServerError)
		return
	}

	// Configure the comparer based on StoreInfo
	si := store.GetStoreInfo()
	if !si.IsPrimitiveKey {
		if si.MapKeyIndexSpecification != "" {
			var is jsondb.IndexSpecification
			if err := encoding.DefaultMarshaler.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
				indexSpec = &is
			} else {
				log.Warn(fmt.Sprintf("Error unmarshaling index spec: %v", err))
			}
		}
	}

	// Detect sample key for primitive types to guide parsing
	var sampleKey any
	if si.IsPrimitiveKey && si.Count > 0 {
		if ok, _ := store.First(ctx); ok {
			sampleKey = store.GetCurrentKey().Key
		}
	}

	var items []map[string]any
	limit := config.PageSize
	if limit <= 0 {
		limit = 100
	}
	count := 0

	// Determine start position
	query := r.URL.Query().Get("q")
	action := r.URL.Query().Get("action")
	refKeyStr := r.URL.Query().Get("key")

	var ok bool

	// Helper to parse key
	parseKey := func(kStr string) any {
		var k any = kStr
		if !isPrimitiveKey {
			var mapKey map[string]any
			if err := json.Unmarshal([]byte(kStr), &mapKey); err == nil {
				k = mapKey
			}
			return k
		}

		// Primitive Key Parsing based on sample
		if sampleKey != nil {
			switch sampleKey.(type) {
			case int:
				if v, err := strconv.Atoi(kStr); err == nil {
					return v
				}
			case int8:
				if v, err := strconv.ParseInt(kStr, 10, 8); err == nil {
					return int8(v)
				}
			case int16:
				if v, err := strconv.ParseInt(kStr, 10, 16); err == nil {
					return int16(v)
				}
			case int32:
				if v, err := strconv.ParseInt(kStr, 10, 32); err == nil {
					return int32(v)
				}
			case int64:
				if v, err := strconv.ParseInt(kStr, 10, 64); err == nil {
					return v
				}
			case uint:
				if v, err := strconv.ParseUint(kStr, 10, 64); err == nil {
					return uint(v)
				}
			case uint8:
				if v, err := strconv.ParseUint(kStr, 10, 8); err == nil {
					return uint8(v)
				}
			case uint16:
				if v, err := strconv.ParseUint(kStr, 10, 16); err == nil {
					return uint16(v)
				}
			case uint32:
				if v, err := strconv.ParseUint(kStr, 10, 32); err == nil {
					return uint32(v)
				}
			case uint64:
				if v, err := strconv.ParseUint(kStr, 10, 64); err == nil {
					return v
				}
			case float32:
				if v, err := strconv.ParseFloat(kStr, 32); err == nil {
					return float32(v)
				}
			case float64:
				if v, err := strconv.ParseFloat(kStr, 64); err == nil {
					return v
				}
			case string:
				return kStr
			case sop.UUID:
				if v, err := sop.ParseUUID(kStr); err == nil {
					return v
				}
			case uuid.UUID:
				if v, err := uuid.Parse(kStr); err == nil {
					return v
				}
			}
		}

		// Fallback guessing if empty store or unknown type
		var i int
		if _, err := fmt.Sscanf(kStr, "%d", &i); err == nil {
			// Check if the original string was actually just digits
			// This prevents "123abc" being parsed as 123
			if fmt.Sprintf("%d", i) == kStr {
				k = i
			}
		}
		return k
	}

	switch action {
	case "first":
		ok, err = store.First(ctx)
	case "last":
		ok, err = store.Last(ctx)
		if ok {
			// Go back limit-1 items to find start of last page
			for i := 0; i < limit-1; i++ {
				if okPrev, _ := store.Previous(ctx); !okPrev {
					store.First(ctx)
					break
				}
			}
		}
	case "current":
		if refKeyStr != "" {
			k := parseKey(refKeyStr)
			// Find(ctx, k, true) positions cursor at k or next item if k is missing
			ok, err = store.Find(ctx, k, true)
			if !ok && err == nil {
				// If key not found, we might be positioned at the next item.
				// Check if we have a valid current item.
				if _, err := store.GetCurrentItem(ctx); err == nil {
					ok = true
				}
			}
		} else {
			ok, err = store.First(ctx)
		}
	case "next":
		if refKeyStr != "" {
			k := parseKey(refKeyStr)
			if found, _ := store.Find(ctx, k, true); found {
				ok, err = store.Next(ctx)
			} else {
				// If key not found, fallback to first
				ok, err = store.First(ctx)
			}
		} else {
			ok, err = store.First(ctx)
		}
	case "prev":
		if refKeyStr != "" {
			k := parseKey(refKeyStr)
			if found, _ := store.Find(ctx, k, true); found {
				if ok, _ = store.Previous(ctx); ok {
					// Go back limit-1 more
					for i := 0; i < limit-1; i++ {
						if okPrev, _ := store.Previous(ctx); !okPrev {
							store.First(ctx)
							break
						}
					}
					// We are positioned at the start of the previous page
					ok = true
				} else {
					// Already at start
					ok, err = store.First(ctx)
				}
			} else {
				ok, err = store.First(ctx)
			}
		} else {
			ok, err = store.First(ctx)
		}
	default:
		if query != "" {
			// Try to find the item with the query key.
			searchKey := parseKey(query)
			fmt.Printf("searchKey: %v/n", searchKey)
			ok, err = store.Find(ctx, searchKey, false)
			if store.GetCurrentKey().Key != nil {
				ok = true
			}
		} else {
			// Iterate from the first item
			ok, err = store.First(ctx)
		}
	}

	// Check for NDJSON Streaming Mode
	isNDJSON := r.Header.Get("Accept") == "application/x-ndjson"
	if isNDJSON {
		w.Header().Set("Content-Type", "application/x-ndjson")
	}

	var encoder *json.Encoder
	if isNDJSON {
		encoder = json.NewEncoder(w)
	}

	for ok && err == nil && count < limit {
		kItem := store.GetCurrentKey()
		v, err := store.GetCurrentValue(ctx)
		if err != nil {
			log.Error(fmt.Sprintf("Error reading value for key %v: %v", kItem.Key, err))
		}

		itemMap := map[string]any{
			"key":   kItem.Key,
			"value": v,
		}

		if isNDJSON {
			if err := encoder.Encode(itemMap); err != nil {
				log.Error(fmt.Sprintf("Error streaming item: %v", err))
				return
			}
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
		} else {
			items = append(items, itemMap)
		}

		ok, err = store.Next(ctx)
		count++
	}

	if err != nil {
		log.Error(fmt.Sprintf("Error during iteration: %v", err))
	}

	if !isNDJSON {
		json.NewEncoder(w).Encode(items)
	}
}

func handleUpdateItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Database  string `json:"database"`
		StoreName string `json:"store"`
		Key       any    `json:"key"`
		Value     any    `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(req.Database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Open transaction for writing
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Ensure rollback in case of error, but we will Commit on success
	defer trans.Rollback(ctx)

	// We need to open the store to perform update
	// We need to determine if it's a primitive key store to set up the comparer correctly
	// and to cast the key from JSON (float64) to the correct type (int, etc).

	// Peek at store info first (requires a read transaction or just checking repository)
	// But we are already in a write transaction. We can access the store repository directly via the transaction.
	var isPrimitiveKey bool
	var indexSpec *jsondb.IndexSpecification

	if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, req.StoreName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
			if !isPrimitiveKey && stores[0].MapKeyIndexSpecification != "" {
				var is jsondb.IndexSpecification
				if err := encoding.DefaultMarshaler.Unmarshal([]byte(stores[0].MapKeyIndexSpecification), &is); err == nil {
					indexSpec = &is
				}
			}
		}
	}

	var comparer btree.ComparerFunc[any]
	// If not primitive, we need a map comparer.
	// For update, we need to find the EXACT key.
	if !isPrimitiveKey {
		if indexSpec != nil {
			comparer = func(a, b any) int {
				return indexSpec.Comparer(a.(map[string]any), b.(map[string]any))
			}
		} else {
			// Default Map Comparer (Dynamic) for generic maps (e.g. ModelStore)
			comparer = func(a, b any) int {
				mapA, okA := a.(map[string]any)
				mapB, okB := b.(map[string]any)
				if !okA || !okB {
					return btree.Compare(a, b)
				}

				keys := make([]string, 0, len(mapA)+len(mapB))
				seen := make(map[string]struct{})
				for k := range mapA {
					if _, exists := seen[k]; !exists {
						keys = append(keys, k)
						seen[k] = struct{}{}
					}
				}
				for k := range mapB {
					if _, exists := seen[k]; !exists {
						keys = append(keys, k)
						seen[k] = struct{}{}
					}
				}
				sort.Strings(keys)

				for _, k := range keys {
					valA, existsA := mapA[k]
					valB, existsB := mapB[k]

					if !existsA && !existsB {
						continue
					}
					if !existsA {
						return -1
					}
					if !existsB {
						return 1
					}

					res := btree.Compare(valA, valB)
					if res != 0 {
						return res
					}
				}
				return 0
			}
		}
	}

	store, err := database.OpenBtree[any, any](ctx, dbOpts, req.StoreName, trans, comparer)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v", req.StoreName, err), http.StatusInternalServerError)
		return
	}

	// Detect sample key to cast req.Key
	var sampleKey any
	if isPrimitiveKey {
		if ok, _ := store.First(ctx); ok {
			sampleKey = store.GetCurrentKey().Key
		}
	}

	// Cast Key
	finalKey := req.Key
	if sampleKey != nil {
		// Handle JSON number (float64) to Integer types
		if f, ok := req.Key.(float64); ok {
			switch sampleKey.(type) {
			case int:
				finalKey = int(f)
			case int8:
				finalKey = int8(f)
			case int16:
				finalKey = int16(f)
			case int32:
				finalKey = int32(f)
			case int64:
				finalKey = int64(f)
			case uint:
				finalKey = uint(f)
			case uint8:
				finalKey = uint8(f)
			case uint16:
				finalKey = uint16(f)
			case uint32:
				finalKey = uint32(f)
			case uint64:
				finalKey = uint64(f)
			case float32:
				finalKey = float32(f)
			}
		}
		// Handle String input for integer keys (UI compat)
		if s, ok := req.Key.(string); ok {
			switch sampleKey.(type) {
			case int:
				if v, err := strconv.Atoi(s); err == nil {
					finalKey = v
				}
			case int8:
				if v, err := strconv.ParseInt(s, 10, 8); err == nil {
					finalKey = int8(v)
				}
			case int16:
				if v, err := strconv.ParseInt(s, 10, 16); err == nil {
					finalKey = int16(v)
				}
			case int32:
				if v, err := strconv.ParseInt(s, 10, 32); err == nil {
					finalKey = int32(v)
				}
			case int64:
				if v, err := strconv.ParseInt(s, 10, 64); err == nil {
					finalKey = v
				}
			case uint:
				if v, err := strconv.ParseUint(s, 10, 64); err == nil {
					finalKey = uint(v)
				}
			case uint8:
				if v, err := strconv.ParseUint(s, 10, 8); err == nil {
					finalKey = uint8(v)
				}
			case uint16:
				if v, err := strconv.ParseUint(s, 10, 16); err == nil {
					finalKey = uint16(v)
				}
			case uint32:
				if v, err := strconv.ParseUint(s, 10, 32); err == nil {
					finalKey = uint32(v)
				}
			case uint64:
				if v, err := strconv.ParseUint(s, 10, 64); err == nil {
					finalKey = uint64(v)
				}
			}
		}
		// Handle String to UUID
		if s, ok := req.Key.(string); ok {
			switch sampleKey.(type) {
			case sop.UUID:
				if id, err := sop.ParseUUID(s); err == nil {
					finalKey = id
				}
			case uuid.UUID:
				if id, err := uuid.Parse(s); err == nil {
					finalKey = id
				}
			}
		}
	}

	// Special handling for "scripts" store (ModelStore validation)
	// ModelStore expects the value to be a JSON string, but the UI might send a JSON object.
	if req.StoreName == "scripts" {
		if _, isString := req.Value.(string); !isString {
			if b, err := json.Marshal(req.Value); err == nil {
				req.Value = string(b)
			}
		}
	}

	// Perform Update
	if ok, err := store.Update(ctx, finalKey, req.Value); err != nil {
		http.Error(w, "Update failed: "+err.Error(), http.StatusInternalServerError)
		return
	} else if !ok {
		http.Error(w, "Update failed: Item not found", http.StatusNotFound)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleAddStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Database      string `json:"database"`
		StoreName     string `json:"store"`
		KeyType       string `json:"key_type"` // string, int, uuid, map
		ValueType     string `json:"value_type"`
		Description   string `json:"description"`
		IndexSpec     string `json:"index_spec"`     // Optional, for map keys
		CelExpression string `json:"cel_expression"` // Optional, for custom sorting
		SeedKey       any    `json:"seed_key"`       // Optional, for seeding
		SeedValue     any    `json:"seed_value"`     // Optional, for seeding

		// Store creation options.
		// NOTE: The UI may hide/show these behind an "Advanced" toggle, but the backend
		// must not depend on any UI-only concept like "advanced_mode".
		SlotLength    *int  `json:"slot_length"`
		IsUnique      *bool `json:"is_unique"`
		DataSize      *int  `json:"data_size"` // 0=Small, 1=Medium, 2=Big
		CacheDuration *int  `json:"cache_duration"`
		IsCacheTTL    *bool `json:"is_cache_ttl"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Hardening: Prevent adding stores to System DB
	if IsSystemDB(req.Database) {
		http.Error(w, "Access Denied: Creating new stores in the System DB is not allowed.", http.StatusForbidden)
		return
	}

	if req.StoreName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(req.Database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Open transaction for writing
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// Defaults (must not depend on any UI-only toggle)
	slotLength := 1000
	if req.SlotLength != nil {
		slotLength = *req.SlotLength
	}
	if slotLength < 2 {
		slotLength = 2
	}
	if slotLength > 10000 {
		slotLength = 10000
	}

	isUnique := true
	if req.IsUnique != nil {
		isUnique = *req.IsUnique
	}

	// Compute StoreOptions based on DataSize
	dataSize := sop.SmallData
	if req.DataSize != nil {
		dataSize = sop.ValueDataSize(*req.DataSize)
	}

	// Use ConfigureStore to get the correct structural flags and defaults
	computedOpts := sop.ConfigureStore(req.StoreName, isUnique, slotLength, req.Description, dataSize, "")

	cacheDuration := 0
	if req.CacheDuration != nil {
		cacheDuration = *req.CacheDuration
	}

	isCacheTTL := false
	if req.IsCacheTTL != nil {
		isCacheTTL = *req.IsCacheTTL
	}

	storeOpts := sop.StoreOptions{
		Name:           req.StoreName,
		SlotLength:     slotLength,
		IsUnique:       isUnique,
		Description:    req.Description,
		IsPrimitiveKey: req.KeyType != "map",
		CELexpression:  req.CelExpression,

		// Apply computed structural flags
		IsValueDataInNodeSegment:     computedOpts.IsValueDataInNodeSegment,
		IsValueDataActivelyPersisted: computedOpts.IsValueDataActivelyPersisted,
		IsValueDataGloballyCached:    computedOpts.IsValueDataGloballyCached,
	}

	// Apply Cache Config
	// If Medium Data, allow user override. Otherwise use computed defaults.
	if dataSize == sop.MediumData {
		// Start with computed defaults (which has correct global cache settings)
		storeOpts.CacheConfig = computedOpts.CacheConfig

		if cacheDuration == -1 {
			// User explicitly wants to disable caching
			storeOpts.CacheConfig.ValueDataCacheDuration = -1 * time.Minute
			storeOpts.CacheConfig.IsValueDataCacheTTL = false
		} else if cacheDuration > 0 {
			// User specified a duration
			storeOpts.CacheConfig.ValueDataCacheDuration = time.Duration(cacheDuration) * time.Minute
			storeOpts.CacheConfig.IsValueDataCacheTTL = isCacheTTL
		} else {
			// cacheDuration == 0. Use Default (already set from computedOpts)
		}
	} else {
		// For Small and Big, enforce computed defaults
		storeOpts.CacheConfig = computedOpts.CacheConfig
	}

	// Cast SeedValue based on ValueType
	finalSeedValue := req.SeedValue
	if req.SeedValue != nil {
		switch req.ValueType {
		case "int":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = int(f)
			}
		case "int8":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = int8(f)
			}
		case "int16":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = int16(f)
			}
		case "int32":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = int32(f)
			}
		case "int64":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = int64(f)
			}
		case "uint":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = uint(f)
			}
		case "uint8":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = uint8(f)
			}
		case "uint16":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = uint16(f)
			}
		case "uint32":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = uint32(f)
			}
		case "uint64":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = uint64(f)
			}
		case "float32":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = float32(f)
			}
		case "rune":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = rune(f)
			}
		case "uuid":
			if s, ok := req.SeedValue.(string); ok {
				if id, err := sop.ParseUUID(s); err == nil {
					finalSeedValue = id
				}
			}
		case "timestamp":
			if f, ok := req.SeedValue.(float64); ok {
				finalSeedValue = int64(f)
			}
		}
	}

	var storeErr error
	switch req.KeyType {
	case "string":
		var s btree.BtreeInterface[string, any]
		s, storeErr = database.NewBtree[string, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			if kStr, ok := req.SeedKey.(string); ok {
				_, storeErr = s.Add(ctx, kStr, finalSeedValue)
			}
		}
	case "int":
		var s btree.BtreeInterface[int, any]
		s, storeErr = database.NewBtree[int, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			if f, ok := req.SeedKey.(float64); ok {
				_, storeErr = s.Add(ctx, int(f), finalSeedValue)
			}
		}
	case "uuid":
		var s btree.BtreeInterface[sop.UUID, any]
		s, storeErr = database.NewBtree[sop.UUID, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			if str, ok := req.SeedKey.(string); ok {
				if id, err2 := sop.ParseUUID(str); err2 == nil {
					_, storeErr = s.Add(ctx, id, finalSeedValue)
				} else {
					storeErr = err2
				}
			}
		}
	case "map":
		var s *jsondb.JsonDBMapKey
		s, storeErr = jsondb.NewJsonBtreeMapKey(ctx, dbOpts, storeOpts, trans, req.IndexSpec)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			// JsonDBMapKey expects []jsondb.Item[map[string]any, any] for Add
			if kMap, ok := req.SeedKey.(map[string]any); ok {
				item := jsondb.Item[map[string]any, any]{
					Key:   kMap,
					Value: &finalSeedValue,
				}
				_, storeErr = s.Add(ctx, []jsondb.Item[map[string]any, any]{item})
			}
		}
	case "array":
		var s btree.BtreeInterface[[]any, any]
		s, storeErr = database.NewBtree[[]any, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			if kArr, ok := req.SeedKey.([]any); ok {
				_, storeErr = s.Add(ctx, kArr, finalSeedValue)
			}
		}
	case "int64":
		var s btree.BtreeInterface[int64, any]
		s, storeErr = database.NewBtree[int64, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			if f, ok := req.SeedKey.(float64); ok {
				_, storeErr = s.Add(ctx, int64(f), finalSeedValue)
			}
		}
	case "float64":
		var s btree.BtreeInterface[float64, any]
		s, storeErr = database.NewBtree[float64, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			if f, ok := req.SeedKey.(float64); ok {
				_, storeErr = s.Add(ctx, f, finalSeedValue)
			}
		}
	case "bool":
		var s btree.BtreeInterface[bool, any]
		s, storeErr = database.NewBtree[bool, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if storeErr == nil && req.SeedKey != nil && req.SeedValue != nil {
			if b, ok := req.SeedKey.(bool); ok {
				_, storeErr = s.Add(ctx, b, finalSeedValue)
			}
		}
	default:
		http.Error(w, "Invalid key type. Supported: string, int, int64, float64, bool, uuid, map, array", http.StatusBadRequest)
		return
	}

	if storeErr != nil {
		http.Error(w, "Failed to create store: "+storeErr.Error(), http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Store created successfully"})
}

func handleDeleteStore(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Database  string `json:"database"`
		StoreName string `json:"store"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Hardening: Prevent deleting stores from System DB
	if IsSystemDB(req.Database) {
		http.Error(w, "Access Denied: Deleting stores from the System DB is not allowed.", http.StatusForbidden)
		return
	}

	if req.StoreName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	dbOpts, err := getDBOptions(req.Database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if err := database.RemoveBtree(r.Context(), dbOpts, req.StoreName); err != nil {
		http.Error(w, "Failed to delete store: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "message": "Store deleted successfully"})
}

func handleAddItem(w http.ResponseWriter, r *http.Request) {
	handleWriteOperation(w, r, "add")
}

func handleDeleteItem(w http.ResponseWriter, r *http.Request) {
	handleWriteOperation(w, r, "delete")
}

func handleWriteOperation(w http.ResponseWriter, r *http.Request, op string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Database  string `json:"database"`
		StoreName string `json:"store"`
		Key       any    `json:"key"`
		Value     any    `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	// Hardening: Prevent manual modification of critical System DB stores
	// Users should not manually add items to llm_knowledge as it is managed by the AI/System
	if IsSystemDB(req.Database) && req.StoreName == "llm_knowledge" {
		if op == "add" {
			http.Error(w, "Access Denied: The 'llm_knowledge' store is managed by the system. Manual additions are restricted.", http.StatusForbidden)
			return
		}
	}

	ctx := r.Context()
	dbOpts, err := getDBOptions(req.Database)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Open transaction for writing
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// Determine if primitive key
	var isPrimitiveKey bool
	var indexSpec *jsondb.IndexSpecification

	if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, req.StoreName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
			if !isPrimitiveKey && stores[0].MapKeyIndexSpecification != "" {
				var is jsondb.IndexSpecification
				if err := encoding.DefaultMarshaler.Unmarshal([]byte(stores[0].MapKeyIndexSpecification), &is); err == nil {
					indexSpec = &is
				}
			}
		}
	}

	var comparer btree.ComparerFunc[any]
	if !isPrimitiveKey {
		if indexSpec != nil {
			comparer = func(a, b any) int {
				return indexSpec.Comparer(a.(map[string]any), b.(map[string]any))
			}
		} else {
			// Default Map Comparer (Dynamic) for generic maps
			comparer = func(a, b any) int {
				mapA, okA := a.(map[string]any)
				mapB, okB := b.(map[string]any)
				if !okA || !okB {
					return btree.Compare(a, b)
				}

				keys := make([]string, 0, len(mapA)+len(mapB))
				seen := make(map[string]struct{})
				for k := range mapA {
					if _, exists := seen[k]; !exists {
						keys = append(keys, k)
						seen[k] = struct{}{}
					}
				}
				for k := range mapB {
					if _, exists := seen[k]; !exists {
						keys = append(keys, k)
						seen[k] = struct{}{}
					}
				}
				sort.Strings(keys)

				for _, k := range keys {
					valA, existsA := mapA[k]
					valB, existsB := mapB[k]

					if !existsA && !existsB {
						continue
					}
					if !existsA {
						return -1
					}
					if !existsB {
						return 1
					}

					res := btree.Compare(valA, valB)
					if res != 0 {
						return res
					}
				}
				return 0
			}
		}
	}

	store, err := database.OpenBtree[any, any](ctx, dbOpts, req.StoreName, trans, comparer)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v", req.StoreName, err), http.StatusInternalServerError)
		return
	}

	// Detect sample key to cast req.Key
	var sampleKey any
	if isPrimitiveKey {
		if ok, _ := store.First(ctx); ok {
			sampleKey = store.GetCurrentKey().Key
		}
	}

	// Cast Key
	finalKey := req.Key
	if sampleKey != nil {
		if f, ok := req.Key.(float64); ok {
			switch sampleKey.(type) {
			case int:
				finalKey = int(f)
			case int8:
				finalKey = int8(f)
			case int16:
				finalKey = int16(f)
			case int32:
				finalKey = int32(f)
			case int64:
				finalKey = int64(f)
			case uint:
				finalKey = uint(f)
			case uint8:
				finalKey = uint8(f)
			case uint16:
				finalKey = uint16(f)
			case uint32:
				finalKey = uint32(f)
			case uint64:
				finalKey = uint64(f)
			case float32:
				finalKey = float32(f)
			}
		}
		if s, ok := req.Key.(string); ok {
			switch sampleKey.(type) {
			case sop.UUID:
				if id, err := sop.ParseUUID(s); err == nil {
					finalKey = id
				}
			case uuid.UUID:
				if id, err := uuid.Parse(s); err == nil {
					finalKey = id
				}
			}
		}
	}

	// Validate Key Type if possible
	if sampleKey != nil {
		sT := fmt.Sprintf("%T", sampleKey)
		fT := fmt.Sprintf("%T", finalKey)
		if sT != fT {
			http.Error(w, fmt.Sprintf("Key type mismatch: expected %s, got %s", sT, fT), http.StatusBadRequest)
			return
		}
	}

	// Perform Operation
	var opErr error
	switch op {
	case "add":
		var added bool
		added, opErr = store.Add(ctx, finalKey, req.Value)
		if opErr == nil && !added {
			opErr = fmt.Errorf("key already exists or is invalid")
		}
	case "delete":
		if store.Count() <= 1 {
			opErr = fmt.Errorf("cannot delete the last item; store must contain at least one item")
		} else {
			var removed bool
			removed, opErr = store.Remove(ctx, finalKey)
			if opErr == nil && !removed {
				opErr = fmt.Errorf("key not found")
			}
		}
	}

	if opErr != nil {
		http.Error(w, opErr.Error(), http.StatusInternalServerError)
		return
	}

	if err := trans.Commit(ctx); err != nil {
		http.Error(w, "Commit failed: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func handleValidateAdminToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		AdminToken string `json:"adminToken"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	if config.RootPassword == "" {
		// If no root password set, we can't validate (fail closed)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	if req.AdminToken != config.RootPassword {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}
