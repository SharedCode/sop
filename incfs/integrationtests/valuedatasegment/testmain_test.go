//go:build integration
// +build integration

package valuedatasegment

import (
	"fmt"
	"os"
	"testing"

	"github.com/sharedcode/sop/incfs"
)

func TestMain(m *testing.M) {
	if os.Getenv("SOP_RUN_INCFS_IT") != "1" {
		fmt.Println("[skip] incfs/integrationtests/valuedatasegment: set SOP_RUN_INCFS_IT=1 to enable these tests")
		os.Exit(0)
		return
	}
	if !incfs.IsInitialized() {
		fmt.Println("[skip] valuedatasegment: redis/cassandra not initialized")
		os.Exit(0)
		return
	}
	os.Exit(m.Run())
}
