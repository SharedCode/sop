//go:build integration
// +build integration

package integrationtests

import (
    "fmt"
    "os"
    "testing"

    "github.com/sharedcode/sop/inredcfs"
)

// Gate these tests behind an explicit opt-in and ready environment.
func TestMain(m *testing.M) {
    if os.Getenv("SOP_RUN_INREDCFS_IT") != "1" {
        fmt.Println("[skip] internal/inredcfs/integrationtests: set SOP_RUN_INREDCFS_IT=1 to enable these tests")
        os.Exit(0)
        return
    }
    if !inredcfs.IsInitialized() {
        fmt.Println("[skip] internal/inredcfs/integrationtests: redis/cassandra not initialized")
        os.Exit(0)
        return
    }
    os.Exit(m.Run())
}
