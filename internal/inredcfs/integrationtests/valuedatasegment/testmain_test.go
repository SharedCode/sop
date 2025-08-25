//go:build integration
// +build integration

package valuedatasegment

import (
    "fmt"
    "os"
    "testing"

    "github.com/sharedcode/sop/internal/inredcfs"
)

func TestMain(m *testing.M) {
    if os.Getenv("SOP_RUN_INREDCFS_IT") != "1" {
        fmt.Println("[skip] internal/inredcfs/integrationtests/valuedatasegment: set SOP_RUN_INREDCFS_IT=1 to enable these tests")
        os.Exit(0)
        return
    }
    if !inredcfs.IsInitialized() {
        fmt.Println("[skip] valuedatasegment: redis/cassandra not initialized")
        os.Exit(0)
        return
    }
    os.Exit(m.Run())
}
