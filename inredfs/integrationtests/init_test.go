//go:build integration
// +build integration

package integrationtests

import (
    "cmp"
    "context"
    "fmt"
    log "log/slog"
    "os"
    "testing"
    "time"

    "github.com/sharedcode/sop"
    "github.com/sharedcode/sop/fs"
    "github.com/sharedcode/sop/inredfs"
    "github.com/sharedcode/sop/redis"
)

// Redis config used by integration tests.
var redisConfig = redis.Options{
    Address:  "localhost:6379",
    Password: "",
    DB:       0,
}

func getDataPath() string {
    if s := os.Getenv("datapath"); s != "" {
        return s
    }
    return "/Users/grecinto/sop_data"
}

var dataPath = getDataPath()

var testDefaultCacheConfig sop.StoreCacheConfig

// Shared person types for tests.
type PersonKey struct {
    Firstname string
    Lastname  string
}

type Person struct {
    Gender string
    Email  string
    Phone  string
    SSN    string
}

func newPerson(fname, lname, gender, email, phone string) (PersonKey, Person) {
    return PersonKey{Firstname: fname, Lastname: lname}, Person{Gender: gender, Email: email, Phone: phone, SSN: "1234"}
}

func Compare(x PersonKey, y PersonKey) int {
    if i := cmp.Compare[string](x.Lastname, y.Lastname); i != 0 {
        return i
    }
    return cmp.Compare[string](x.Firstname, y.Firstname)
}

const (
    nodeSlotLength = 200
    batchSize      = 50
    tableName1     = "person2db_it"
    tableName2     = "twophase22"
)

// Basic EC config used by replication tests.
func initErasureCoding() {
    ec := make(map[string]fs.ErasureCodingConfig)
    ec["barstoreec"] = fs.ErasureCodingConfig{
        DataShardsCount:   2,
        ParityShardsCount: 1,
        BaseFolderPathsAcrossDrives: []string{
            fmt.Sprintf("%s%cdisk1", dataPath, os.PathSeparator),
            fmt.Sprintf("%s%cdisk2", dataPath, os.PathSeparator),
            fmt.Sprintf("%s%cdisk3", dataPath, os.PathSeparator),
        },
        RepairCorruptedShards: true,
    }
    ec[""] = fs.ErasureCodingConfig{
        DataShardsCount:   2,
        ParityShardsCount: 2,
        BaseFolderPathsAcrossDrives: []string{
            fmt.Sprintf("%s%cdisk4", dataPath, os.PathSeparator),
            fmt.Sprintf("%s%cdisk5", dataPath, os.PathSeparator),
            fmt.Sprintf("%s%cdisk6", dataPath, os.PathSeparator),
            fmt.Sprintf("%s%cdisk7", dataPath, os.PathSeparator),
        },
        RepairCorruptedShards: true,
    }
    fs.SetGlobalErasureConfig(ec)
}

var storesFolders = []string{
    fmt.Sprintf("%s%cdisk1", dataPath, os.PathSeparator),
    fmt.Sprintf("%s%cdisk2", dataPath, os.PathSeparator),
}

var storesFoldersDefault = []string{
    fmt.Sprintf("%s%cdisk4", dataPath, os.PathSeparator),
    fmt.Sprintf("%s%cdisk5", dataPath, os.PathSeparator),
}

func TestMain(m *testing.M) {
    // Configure logging to Info.
    l := log.New(log.NewJSONHandler(os.Stdout, &log.HandlerOptions{Level: log.LevelInfo}))
    log.SetDefault(l)

    // Initialize Redis-backed components.
    inredfs.Initialize(redisConfig)

    // Initialize erasure coding for replication tests.
    initErasureCoding()

    // Clear Redis cache between runs to avoid cross-test contamination.
    cache := redis.NewClient()
    _ = cache.Clear(context.Background())

    // Shorten default node cache to keep tests snappy.
    testDefaultCacheConfig = sop.GetDefaulCacheConfig()
    testDefaultCacheConfig.NodeCacheDuration = time.Minute
    sop.SetDefaultCacheConfig(testDefaultCacheConfig)

    os.Exit(m.Run())
}
