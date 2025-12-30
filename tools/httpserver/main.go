package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	log "log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/jsondb"
)

// DatabaseConfig holds configuration for a single SOP database
type DatabaseConfig struct {
	Name     string `json:"name"`
	Path     string `json:"path"`
	Mode     string `json:"mode"`  // "standalone" or "clustered"
	RedisURL string `json:"redis"` // Optional, for clustered
	IsSystem bool   `json:"is_system,omitempty"`

	// // DetectedRoot is the inferred root directory for relative paths in this database.
	// // It is calculated at startup and not read from JSON.
	// DetectedRoot string `json:"-"`
}

// Config holds the server configuration
type Config struct {
	Port      int              `json:"port"`
	Databases []DatabaseConfig `json:"databases"`
	PageSize  int              `json:"pageSize"`
	SystemDB  *DatabaseConfig  `json:"system_db,omitempty"`

	// Legacy/CLI fields
	DatabasePath string
	Mode         string
	ConfigFile   string
	RedisURL     string
}

//go:embed templates/*
var content embed.FS

var config Config
var loadedAgents = make(map[string]ai.Agent[map[string]any])

// Version is the application version, set at build time via -ldflags
var Version = "dev"

func main() {

	l := log.New(log.NewJSONHandler(os.Stdout, &log.HandlerOptions{
		Level: log.LevelDebug,
	}))
	log.SetDefault(l) // configures log package to print with LevelInfo

	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "Show version and exit")
	flag.IntVar(&config.Port, "port", 8080, "Port to run the server on")
	flag.StringVar(&config.DatabasePath, "database", "/tmp/sop_data", "Path to the SOP database/data directory")
	flag.StringVar(&config.Mode, "mode", "standalone", "SOP mode: 'standalone' or 'clustered'")
	flag.StringVar(&config.ConfigFile, "config", "", "Path to configuration file (optional)")
	flag.StringVar(&config.RedisURL, "redis", "localhost:6379", "Redis URL for clustered mode (e.g. localhost:6379)")
	flag.IntVar(&config.PageSize, "pageSize", 40, "Number of items to display per page")
	flag.Parse()

	if showVersion {
		fmt.Printf("SOP Data Manager v%s\n", Version)
		os.Exit(0)
	}

	// Load config from file if provided
	if config.ConfigFile != "" {
		if err := loadConfig(config.ConfigFile); err != nil {
			log.Error(fmt.Sprintf("Failed to load config file: %v", err))
		}
	}

	// If no databases loaded (e.g. no config file or empty), use CLI flags as default
	if len(config.Databases) == 0 {
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
	http.HandleFunc("/api/databases", handleListDatabases)
	http.HandleFunc("/api/stores", handleListStores)
	http.HandleFunc("/api/db/options", handleGetDBOptions)
	http.HandleFunc("/api/store/info", handleGetStoreInfo)
	http.HandleFunc("/api/store/items", handleListItems)
	http.HandleFunc("/api/store/item/update", handleUpdateItem)
	http.HandleFunc("/api/store/item/add", handleAddItem)
	http.HandleFunc("/api/store/add", handleAddStore)
	http.HandleFunc("/api/store/delete", handleDeleteStore)
	http.HandleFunc("/api/store/item/delete", handleDeleteItem)
	http.HandleFunc("/api/ai/chat", handleAIChat)

	// Initialize Agents
	initAgents()

	// Start Server
	addr := fmt.Sprintf(":%d", config.Port)
	log.Info(fmt.Sprintf("SOP Data Manager v%s running at http://localhost%s", Version, addr))
	for _, db := range config.Databases {
		log.Debug(fmt.Sprintf("Database '%s': %s (%s)", db.Name, db.Path, db.Mode))
	}

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Error(err.Error())
	}
}

func loadConfig(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := json.NewDecoder(f).Decode(&config); err != nil {
		return err
	}

	// Default System DB logic
	configDir := filepath.Dir(path)
	if config.SystemDB == nil {
		config.SystemDB = &DatabaseConfig{
			Name: "System",
			Mode: "standalone",
			Path: configDir,
		}
	} else {
		if config.SystemDB.Path == "" {
			config.SystemDB.Path = configDir
		}
		if config.SystemDB.Name == "" {
			config.SystemDB.Name = "System"
		}
	}
	return nil
}

func getSystemDBOptions() (sop.DatabaseOptions, error) {
	if config.SystemDB == nil {
		// Fallback if no config file was loaded
		cwd, _ := os.Getwd()
		return sop.DatabaseOptions{
			Type:          sop.Standalone,
			StoresFolders: []string{cwd},
		}, nil
	}
	return getDBOptionsFromConfig(config.SystemDB)
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
	var db *DatabaseConfig
	if dbName == "" {
		if len(config.Databases) > 0 {
			db = &config.Databases[0]
		}
	} else {
		// Check System DB first
		if config.SystemDB != nil {
			sysName := config.SystemDB.Name
			if sysName == "" {
				sysName = "system"
			}
			if dbName == sysName {
				db = config.SystemDB
			}
		}

		if db == nil {
			for i := range config.Databases {
				if config.Databases[i].Name == dbName {
					db = &config.Databases[i]
					break
				}
			}
		}
	}

	if db == nil {
		return sop.DatabaseOptions{}, fmt.Errorf("database '%s' not found", dbName)
	}

	return getDBOptionsFromConfig(db)
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.ParseFS(content, "templates/index.html")
	if err != nil {
		http.Error(w, "Could not load template: "+err.Error(), http.StatusInternalServerError)
		return
	}
	data := map[string]any{
		"Version": Version,
		"Mode":    config.Mode,
	}
	tmpl.Execute(w, data)
}

func handleListDatabases(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	dbs := make([]DatabaseConfig, len(config.Databases))
	copy(dbs, config.Databases)

	if config.SystemDB != nil {
		sysDB := *config.SystemDB
		if sysDB.Name == "" {
			sysDB.Name = "system"
		}
		sysDB.IsSystem = true
		dbs = append(dbs, sysDB)
	}

	json.NewEncoder(w).Encode(dbs)
}

func handleListStores(w http.ResponseWriter, r *http.Request) {
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
	dbName := r.URL.Query().Get("database")
	dbOpts, err := getDBOptions(dbName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(dbOpts)
}

func handleGetStoreInfo(w http.ResponseWriter, r *http.Request) {
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
	}

	if !si.IsPrimitiveKey {
		if si.MapKeyIndexSpecification != "" {
			var is jsondb.IndexSpecification
			if err := encoding.DefaultMarshaler.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
				response["indexSpec"] = is
			}
		} else if si.Count > 0 {
			// Fetch a sample key to infer structure for UI
			if ok, _ := store.First(ctx); ok {
				response["sampleKey"] = store.GetCurrentKey().Key
			}
		}
	}

	json.NewEncoder(w).Encode(response)
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
				if ok, _ := store.Previous(ctx); !ok {
					break
				}
			}
		}
	case "current":
		if refKeyStr != "" {
			k := parseKey(refKeyStr)
			// Find(ctx, k, true) positions cursor at k or next item if k is missing
			ok, err = store.Find(ctx, k, true)
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

	for ok && err == nil && count < limit {
		kItem := store.GetCurrentKey()
		v, err := store.GetCurrentValue(ctx)
		if err != nil {
			log.Error(fmt.Sprintf("Error reading value for key %v: %v", kItem.Key, err))
			// Continue to next item even if value fetch fails? Or break?
			// Let's try to continue.
		}

		items = append(items, map[string]any{
			"key":   kItem.Key,
			"value": v,
		})

		ok, err = store.Next(ctx)
		count++
	}

	if err != nil {
		log.Error(fmt.Sprintf("Error during iteration: %v", err))
	}

	json.NewEncoder(w).Encode(items)
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
		Database    string `json:"database"`
		StoreName   string `json:"store"`
		KeyType     string `json:"key_type"` // string, int, uuid, map
		Description string `json:"description"`
		IndexSpec   string `json:"index_spec"` // Optional, for map keys
		SeedKey     any    `json:"seed_key"`   // Optional, for seeding
		SeedValue   any    `json:"seed_value"` // Optional, for seeding

		// Advanced options
		AdvancedMode             bool `json:"advanced_mode"`
		SlotLength               int  `json:"slot_length"`
		IsUnique                 bool `json:"is_unique"`
		IsValueDataInNodeSegment bool `json:"is_value_data_in_node_segment"`
		CacheDuration            int  `json:"cache_duration"`
		IsCacheTTL               bool `json:"is_cache_ttl"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
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

	storeOpts := sop.StoreOptions{
		Name:        req.StoreName,
		SlotLength:  1000,
		IsUnique:    true,
		Description: req.Description,
	}

	if req.AdvancedMode {
		storeOpts.SlotLength = req.SlotLength
		storeOpts.IsUnique = req.IsUnique
		storeOpts.IsValueDataInNodeSegment = req.IsValueDataInNodeSegment

		// Always start with SOP defaults
		def := sop.GetDefaultCacheConfig()
		storeOpts.CacheConfig = &def

		// Only override ValueDataCacheDuration if not in node segment and duration provided
		if !req.IsValueDataInNodeSegment {
			storeOpts.IsValueDataGloballyCached = true
			if req.CacheDuration > 0 {
				storeOpts.CacheConfig.ValueDataCacheDuration = time.Duration(req.CacheDuration) * time.Minute
				storeOpts.CacheConfig.IsValueDataCacheTTL = req.IsCacheTTL
			}
		}
	}

	switch req.KeyType {
	case "string":
		var s btree.BtreeInterface[string, any]
		s, err = database.NewBtree[string, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if err == nil && req.SeedKey != nil && req.SeedValue != nil {
			if kStr, ok := req.SeedKey.(string); ok {
				s.Add(ctx, kStr, req.SeedValue)
			}
		}
	case "int":
		var s btree.BtreeInterface[int, any]
		s, err = database.NewBtree[int, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if err == nil && req.SeedKey != nil && req.SeedValue != nil {
			if f, ok := req.SeedKey.(float64); ok {
				s.Add(ctx, int(f), req.SeedValue)
			}
		}
	case "uuid":
		var s btree.BtreeInterface[sop.UUID, any]
		s, err = database.NewBtree[sop.UUID, any](ctx, dbOpts, req.StoreName, trans, nil, storeOpts)
		if err == nil && req.SeedKey != nil && req.SeedValue != nil {
			if str, ok := req.SeedKey.(string); ok {
				if id, err := sop.ParseUUID(str); err == nil {
					s.Add(ctx, id, req.SeedValue)
				}
			}
		}
	case "map":
		var s *jsondb.JsonDBMapKey
		s, err = jsondb.NewJsonBtreeMapKey(ctx, dbOpts, storeOpts, trans, req.IndexSpec)
		if err == nil && req.SeedKey != nil && req.SeedValue != nil {
			// JsonDBMapKey expects []jsondb.Item[map[string]any, any] for Add
			// But wait, does it? Let's check jsondb.NewJsonBtreeMapKey return type.
			// It returns *JsonDBMapKey.
			// Let's check Add signature.
			// It seems it might be batch add?
			// If so, we need to wrap it.
			// But wait, if it's a Btree, it should have Add(ctx, key, value).
			// The error said: have Add(context.Context, []jsondb.Item[map[string]any, any]) (bool, error)
			// So it is batch add.

			// We need to construct the item.
			if kMap, ok := req.SeedKey.(map[string]any); ok {
				item := jsondb.Item[map[string]any, any]{
					Key:   kMap,
					Value: &req.SeedValue,
				}
				s.Add(ctx, []jsondb.Item[map[string]any, any]{item})
			}
		}
	default:
		http.Error(w, "Invalid key type. Supported: string, int, uuid, map", http.StatusBadRequest)
		return
	}

	if err != nil {
		http.Error(w, "Failed to create store: "+err.Error(), http.StatusInternalServerError)
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
