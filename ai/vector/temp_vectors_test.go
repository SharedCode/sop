package vector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
)

func TestOptimizeWithTempVectors(t *testing.T) {
	// Setup
	tmpDir, err := os.MkdirTemp("", "sop-ai-test-temp-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	db := NewDatabase[map[string]any](ai.Standalone)
	db.SetStoragePath(tmpDir)
	idx := db.Open(context.Background(), "test_temp")
	dIdx := idx.(*domainIndex[map[string]any])

	// 1. Manually populate TempVectors
	// We need to open a transaction and get the architecture
	trans, err := db.beginTransaction(context.Background(), sop.ForWriting, tmpDir)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Open Store (Version 0)
	arch, err := OpenDomainStore(context.Background(), trans, "test_temp", 0, sop.MediumData)
	if err != nil {
		t.Fatalf("Failed to open domain store: %v", err)
	}

	// Add items to TempVectors
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("item-%d", i)
		vec := []float32{float32(i), float32(i)}
		if _, err := arch.TempVectors.Add(context.Background(), id, vec); err != nil {
			t.Fatalf("Failed to add to TempVectors: %v", err)
		}

		stored := storedItem[map[string]any]{
			Payload: map[string]any{"val": i},
			// CentroidID/Distance are 0/0 initially
		}
		data, _ := json.Marshal(stored)
		if _, err := arch.Content.Add(context.Background(), id, string(data)); err != nil {
			t.Fatalf("Failed to add to Content: %v", err)
		}
	}

	if err := trans.Commit(context.Background()); err != nil {
		t.Fatalf("Failed to commit setup: %v", err)
	}

	// 2. Run Optimize
	if err := dIdx.Optimize(context.Background()); err != nil {
		t.Fatalf("Optimize failed: %v", err)
	}

	// 3. Verify Vectors are populated
	// We can use Query or Get
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("item-%d", i)
		item, err := dIdx.Get(context.Background(), id)
		if err != nil {
			t.Errorf("Get(%s) failed: %v", id, err)
		} else {
			if item.CentroidID == 0 {
				t.Errorf("Item %s has CentroidID 0 (not assigned)", id)
			}
			if len(item.Vector) == 0 {
				t.Errorf("Item %s has empty vector", id)
			}
		}
	}

	// 4. Verify TempVectors folder is gone
	// Since we performed a hard delete of the files, we verify the folder is missing.
	// Note: The B-Tree metadata (Registry) might still be stale (Count > 0), but the data is gone.
	tempVectorsPath := filepath.Join(tmpDir, "test_temp_temp_vectors")
	if _, err := os.Stat(tempVectorsPath); !os.IsNotExist(err) {
		t.Errorf("TempVectors folder still exists at %s", tempVectorsPath)
	}
}
