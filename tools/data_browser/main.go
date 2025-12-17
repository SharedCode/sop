package main

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/btree"
	"github.com/sharedcode/sop/common"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/infs"
	"github.com/sharedcode/sop/jsondb"
)

// Config holds the server configuration
type Config struct {
	Port         int
	RegistryPath string
	Mode         string
	ConfigFile   string
	RedisURL     string
	PageSize     int
}

//go:embed templates/*
var content embed.FS

var config Config

// Version is the application version, set at build time via -ldflags
var Version = "dev"

func main() {
	var showVersion bool
	flag.BoolVar(&showVersion, "version", false, "Show version and exit")
	flag.IntVar(&config.Port, "port", 8080, "Port to run the server on")
	flag.StringVar(&config.RegistryPath, "registry", "/tmp/sop_data", "Path to the SOP registry/data directory")
	flag.StringVar(&config.Mode, "mode", "standalone", "SOP mode: 'standalone' or 'clustered'")
	flag.StringVar(&config.ConfigFile, "config", "", "Path to configuration file (optional)")
	flag.StringVar(&config.RedisURL, "redis", "localhost:6379", "Redis URL for clustered mode (e.g. localhost:6379)")
	flag.IntVar(&config.PageSize, "pageSize", 100, "Number of items to display per page")
	flag.Parse()

	if showVersion {
		fmt.Printf("SOP Data Browser v%s\n", Version)
		os.Exit(0)
	}

	// Load config from file if provided
	if config.ConfigFile != "" {
		if err := loadConfig(config.ConfigFile); err != nil {
			log.Fatalf("Failed to load config file: %v", err)
		}
	}

	// Resolve RegistryPath to absolute
	if abs, err := filepath.Abs(config.RegistryPath); err == nil {
		config.RegistryPath = abs
	}

	// Ensure registry path exists (basic check)
	if _, err := os.Stat(config.RegistryPath); os.IsNotExist(err) {
		log.Printf("Warning: Registry path %s does not exist. Please ensure it is correct.", config.RegistryPath)
	} else {
		// Attempt to auto-detect the correct CWD based on stored paths
		if err := autoAdjustCWD(); err != nil {
			log.Printf("Auto-adjust CWD failed: %v", err)
			// Fallback to parent directory
			parentDir := filepath.Dir(config.RegistryPath)
			if err := os.Chdir(parentDir); err != nil {
				log.Printf("Warning: Failed to change directory to %s: %v", parentDir, err)
			} else {
				log.Printf("Working directory set to %s (fallback)", parentDir)
			}
		}
	}

	// Setup Routes
	http.HandleFunc("/", handleIndex)
	http.HandleFunc("/api/stores", handleListStores)
	http.HandleFunc("/api/store/info", handleGetStoreInfo)
	http.HandleFunc("/api/store/items", handleListItems)
	http.HandleFunc("/api/store/item/update", handleUpdateItem)
	http.HandleFunc("/api/store/item/add", handleAddItem)
	http.HandleFunc("/api/store/item/delete", handleDeleteItem)

	// Start Server
	addr := fmt.Sprintf(":%d", config.Port)
	log.Printf("SOP Data Browser v%s (%s) running at http://localhost%s", Version, config.Mode, addr)
	log.Printf("Target Registry: %s", config.RegistryPath)
	if config.Mode == "clustered" {
		log.Printf("Redis: %s", config.RedisURL)
	} else {
		log.Printf("Warning: Running in Standalone mode. If the target registry is managed by a cluster, updates may corrupt the database.")
	}
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

func autoAdjustCWD() error {
	ctx := context.Background()
	dbOpts := getDBOptions()

	// Open a read-only transaction to fetch stores
	// Note: This might fail if CWD is already wrong and GetStores relies on it?
	// Usually GetStores relies on RegistryPath which is absolute in dbOpts.
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %v", err)
	}
	defer trans.Rollback(ctx)

	stores, err := trans.GetStores(ctx)
	if err != nil {
		return fmt.Errorf("failed to list stores: %v", err)
	}
	if len(stores) == 0 {
		return fmt.Errorf("no stores found")
	}

	// Pick the first store to check its path
	storeName := stores[0]

	// We need to access the StoreRepository directly to get StoreInfo without opening the B-Tree
	t2, ok := trans.GetPhasedTransaction().(*common.Transaction)
	if !ok {
		return fmt.Errorf("failed to cast transaction")
	}

	storeInfos, err := t2.StoreRepository.Get(ctx, storeName)
	if err != nil || len(storeInfos) == 0 {
		return fmt.Errorf("failed to get store info for %s: %v", storeName, err)
	}

	si := storeInfos[0]
	blobTable := si.BlobTable

	// blobTable is the relative path stored in DB, e.g., "data/large_complex_db/people"
	// config.RegistryPath is absolute, e.g., "/.../examples/data/large_complex_db"
	// The physical path of the store should be filepath.Join(config.RegistryPath, storeName) if standard structure?
	// Or simply, the blobTable path must be a suffix of the physical path.

	// Let's assume the physical file exists at <RegistryPath>/<StoreName> (standard SOP structure for B-Tree nodes?)
	// Actually, SOP creates a folder with the store name inside the registry folder.
	// So physical location is config.RegistryPath + "/" + storeName.

	// However, if the user created the store with a path "data/large_complex_db",
	// and the store name is "people", the internal path is "data/large_complex_db/people".

	// We want to find a RootDir such that filepath.Join(RootDir, blobTable) exists.
	// And we know that the file actually exists at filepath.Join(config.RegistryPath, storeName).

	// So: RootDir + blobTable = config.RegistryPath + storeName
	// RootDir = config.RegistryPath + storeName - blobTable

	// We need to strip the components of blobTable from the end of (config.RegistryPath + storeName).

	targetPath := filepath.Join(config.RegistryPath, storeName)

	// Normalize separators
	blobTable = filepath.Clean(blobTable)

	if strings.HasSuffix(targetPath, blobTable) {
		// Found the overlap!
		// The root dir is the part of targetPath BEFORE blobTable.
		rootDir := targetPath[:len(targetPath)-len(blobTable)]
		// Clean up trailing separator
		rootDir = filepath.Clean(rootDir)

		log.Printf("Auto-detected Root Directory: %s (based on store '%s' path '%s')", rootDir, storeName, blobTable)
		if err := os.Chdir(rootDir); err != nil {
			return fmt.Errorf("failed to chdir to %s: %v", rootDir, err)
		}
		return nil
	}

	return fmt.Errorf("path mismatch: target='%s', blob='%s'", targetPath, blobTable)
}

func loadConfig(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(&config)
}

func getDBType() sop.DatabaseType {
	if config.Mode == "clustered" {
		return sop.Clustered
	}
	return sop.Standalone
}

func getDBOptions() sop.DatabaseOptions {
	opts := sop.DatabaseOptions{
		Type:          getDBType(),
		StoresFolders: []string{config.RegistryPath},
	}
	if opts.Type == sop.Clustered {
		opts.RedisConfig = &sop.RedisCacheConfig{
			Address: config.RedisURL,
		}
	}
	return opts
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

func handleListStores(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	dbOpts := getDBOptions()

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

func handleGetStoreInfo(w http.ResponseWriter, r *http.Request) {
	storeName := r.URL.Query().Get("name")
	if storeName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts := getDBOptions()

	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// We need to open the store to get its info.
	// We can use a dummy comparer since we are not doing any operations.
	comparer := func(a, b any) int { return 0 }
	store, err := infs.OpenBtree[any, any](ctx, storeName, trans, comparer)
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

	if !si.IsPrimitiveKey && si.MapKeyIndexSpecification != "" {
		var is jsondb.IndexSpecification
		if err := encoding.DefaultMarshaler.Unmarshal([]byte(si.MapKeyIndexSpecification), &is); err == nil {
			response["indexSpec"] = is
		}
	}

	json.NewEncoder(w).Encode(response)
}

func handleListItems(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, no-cache, must-revalidate")
	storeName := r.URL.Query().Get("name")
	if storeName == "" {
		http.Error(w, "Store name is required", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts := getDBOptions()

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
	store, err := infs.OpenBtree[any, any](ctx, storeName, trans, comparer)
	if err != nil {
		// If opening fails, it might be because the store was created with a specific path structure
		// that the generic OpenBtree doesn't know about if it's not in the registry correctly.
		// However, for Standalone mode, OpenBtree relies on the registry.
		// The error "no such file or directory" suggests it's trying to open a file that doesn't exist.
		// This can happen if the store name in the registry points to a path that is relative
		// and the browser is running from a different CWD than the creator.
		http.Error(w, fmt.Sprintf("Failed to open store '%s': %v. Ensure you are running sop-browser from the correct directory relative to the data.", storeName, err), http.StatusInternalServerError)
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
				log.Printf("Error unmarshaling index spec: %v", err)
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
			log.Printf("Error reading value for key %v: %v", kItem.Key, err)
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
		log.Printf("Error during iteration: %v", err)
	}

	json.NewEncoder(w).Encode(items)
}

func handleUpdateItem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		StoreName string `json:"store"`
		Key       any    `json:"key"`
		Value     any    `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts := getDBOptions()

	// Open transaction for writing
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	// Ensure rollback in case of error, but we will Commit on success
	defer trans.Rollback(ctx)

	// Warn if standalone mode
	if config.Mode == "standalone" {
		log.Printf("Warning: Performing update in Standalone mode. If this database is managed by a cluster, this operation may corrupt the database.")
	}

	// We need to open the store to perform update
	// We need to determine if it's a primitive key store to set up the comparer correctly
	// and to cast the key from JSON (float64) to the correct type (int, etc).

	// Peek at store info first (requires a read transaction or just checking repository)
	// But we are already in a write transaction. We can access the store repository directly via the transaction.
	var isPrimitiveKey bool
	if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, req.StoreName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
		}
	}

	var comparer btree.ComparerFunc[any]
	// If not primitive, we need a map comparer.
	// For update, we need to find the EXACT key.
	if !isPrimitiveKey {
		// Use the same dynamic map comparer as in handleListItems
		comparer = func(a, b any) int {
			mapA, okA := a.(map[string]any)
			mapB, okB := b.(map[string]any)
			if !okA || !okB {
				return btree.Compare(a, b)
			}
			// Simple dynamic comparison
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

	store, err := infs.OpenBtree[any, any](ctx, req.StoreName, trans, comparer)
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
		StoreName string `json:"store"`
		Key       any    `json:"key"`
		Value     any    `json:"value"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	dbOpts := getDBOptions()

	// Open transaction for writing
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		http.Error(w, "Failed to begin transaction: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer trans.Rollback(ctx)

	// Warn if standalone mode
	if config.Mode == "standalone" {
		log.Printf("Warning: Performing %s in Standalone mode. If this database is managed by a cluster, this operation may corrupt the database.", op)
	}

	// Determine if primitive key
	var isPrimitiveKey bool
	if t2, ok := trans.GetPhasedTransaction().(*common.Transaction); ok {
		stores, err := t2.StoreRepository.Get(ctx, req.StoreName)
		if err == nil && len(stores) > 0 {
			isPrimitiveKey = stores[0].IsPrimitiveKey
		}
	}

	var comparer btree.ComparerFunc[any]
	if !isPrimitiveKey {
		// Use the same dynamic map comparer
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

	store, err := infs.OpenBtree[any, any](ctx, req.StoreName, trans, comparer)
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
			_, opErr = store.Remove(ctx, finalKey)
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
