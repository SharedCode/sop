package vector

import (
	"fmt"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
)

func TestRepro(t *testing.T) {
	fmt.Printf("NoCache: %d\n", sop.NoCache)
	fmt.Printf("InMemory: %d\n", sop.InMemory)
	fmt.Printf("Redis: %d\n", sop.Redis)

	db := database.NewDatabase(database.DatabaseOptions{
		StoresFolders: []string{"/tmp/test"},
	})

	cache := db.Cache()
	fmt.Printf("Cache type: %T\n", cache)

	if cache == nil {
		t.Fatal("Cache is nil")
	}
}
