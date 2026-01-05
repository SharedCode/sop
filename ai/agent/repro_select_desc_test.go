package agent

import (
	"context"
	"fmt"
	"testing"

	"strings"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
	sopdb "github.com/sharedcode/sop/database"
)

func TestReproSelectDesc(t *testing.T) {
	// Setup
	ctx := context.Background()
	dbPath := t.TempDir()

	dbOpts := sop.DatabaseOptions{
		StoresFolders: []string{dbPath},
	}

	// Create DB and Store
	db := database.NewDatabase(dbOpts)
	tx, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}

	// Create the store first
	store, err := sopdb.NewBtree[string, any](ctx, dbOpts, "employees", tx, func(a, b string) int {
		return strings.Compare(a, b)
	})
	if err != nil {
		t.Fatalf("NewBtree failed: %v", err)
	}

	// Add Data
	for i := 0; i < 5000; i++ {
		store.Add(ctx, fmt.Sprintf("%04d", i), map[string]any{"name": fmt.Sprintf("Name%d", i)})
	}

	tx.Commit(ctx)

	// Re-open for reading
	tx, err = db.BeginTransaction(ctx, sop.ForReading)
	if err != nil {
		t.Fatalf("BeginTransaction failed: %v", err)
	}
	store, err = sopdb.OpenBtree[string, any](ctx, dbOpts, "employees", tx, func(a, b string) int {
		return strings.Compare(a, b)
	})
	if err != nil {
		t.Fatalf("OpenBtree failed: %v", err)
	}
	fmt.Printf("SlotLength: %d\n", store.GetStoreInfo().SlotLength)

	// Test FindInDescendingOrder directly
	// Case 1: Exact Match
	if ok, err := store.FindInDescendingOrder(ctx, "0500"); err != nil {
		t.Fatalf("FindInDescendingOrder failed: %v", err)
	} else if !ok {
		t.Errorf("FindInDescendingOrder('0500') returned false")
	}
	item := store.GetCurrentKey()
	if item.Key != "0500" {
		t.Errorf("FindInDescendingOrder('0500') positioned at '%v', expected '0500'", item.Key)
	}

	// Case 2: Non-existent key
	// "0500.5" is between "0500" and "0501".
	// Should position at "0501" (smallest > 0500.5).
	if ok, err := store.FindInDescendingOrder(ctx, "0500.5"); err != nil {
		t.Fatalf("FindInDescendingOrder('0500.5') failed: %v", err)
	} else if ok {
		t.Errorf("FindInDescendingOrder('0500.5') returned true")
	}
	item = store.GetCurrentKey()
	if item.Key != "0501" {
		t.Errorf("FindInDescendingOrder('0500.5') positioned at '%v', expected '0501'", item.Key)
	}

	// Case 3: Target > all items
	// "9999". Should position at "4999" (largest < 9999).
	if ok, err := store.FindInDescendingOrder(ctx, "9999"); err != nil {
		t.Fatalf("FindInDescendingOrder('9999') failed: %v", err)
	} else if ok {
		t.Errorf("FindInDescendingOrder('9999') returned true")
	}
	item = store.GetCurrentKey()
	if item.Key != "4999" {
		t.Errorf("FindInDescendingOrder('9999') positioned at '%v', expected '4999'", item.Key)
	}

	// Simulate select tool logic with limit
	startKey := "9999"
	isDesc := true
	limit := 2

	// FindInDescendingOrder
	ok, err := store.FindInDescendingOrder(ctx, startKey)
	if err != nil {
		t.Fatalf("FindInDescendingOrder failed: %v", err)
	}

	item = store.GetCurrentKey()
	fmt.Printf("Start Item: %s\n", item.Key)

	count := 0
	for item.Key != "" {
		// Check match (assuming we want <= startKey)
		if item.Key <= startKey {
			fmt.Printf("Found: %s\n", item.Key)
			count++
			if count >= limit {
				break
			}
		}

		if isDesc {
			ok, err = store.Previous(ctx)
		} else {
			ok, err = store.Next(ctx)
		}
		if !ok {
			break
		}
		item = store.GetCurrentKey()
	}

	if count != 2 {
		t.Errorf("Expected 2 items, got %d", count)
	}
}
