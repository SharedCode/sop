package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	log "log/slog"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/adapters/redis"
	"github.com/sharedcode/sop/infs"
)

func init() {
	redis.OpenConnectionWithURL("redis://localhost:6379")
	sop.RegisterL2CacheFactory(sop.Redis, func() sop.L2Cache {
		return redis.NewClient()
	})
}

func main() {
	fmt.Println("--- Concurrent Transactions Demo (Go Standalone) ---")

	maxJitter := flag.Int("maxJitter", 0, "Maximum random jitter in milliseconds to simulate slowness")
	reverse := flag.Int("reverse", 0, "Populate in reverse, 0 is false, 1 is true")
	flag.Parse()

	// Setup
	dbPath := "data/concurrent_demo_go"

	logger := log.New(log.NewJSONHandler(os.Stdout, &log.HandlerOptions{
		Level: log.LevelDebug,
	}))
	log.SetDefault(logger)

	log.Debug("data path", "dbPath", dbPath)

	ctx := context.Background()

	// Seed the B-Tree
	seed(ctx, dbPath)

	// Run concurrent workers
	var wg sync.WaitGroup
	threadCount := 40
	itemsPerThread := 300
	targetCount := int64((threadCount+1)*itemsPerThread+1)

	fmt.Printf("Launching %d threads, %d items each...\n", threadCount, itemsPerThread)

	if *reverse > 0 {
		for i := threadCount; i > 0; i-- {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				worker(ctx, id, dbPath, itemsPerThread, *maxJitter, targetCount)
			}(i)
		}
	} else {
		for i := 0; i < threadCount; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				worker(ctx, id, dbPath, itemsPerThread, *maxJitter, targetCount)
			}(i)
		}
	}

	wg.Wait()
	fmt.Println("done")

	// Verify
	verify(ctx, dbPath, (threadCount+1)*itemsPerThread+1) // +1 for seed
}

func seed(ctx context.Context, dbPath string) {
	t, err := infs.NewTransaction(ctx, sop.TransactionOptions{
		StoresFolders: []string{dbPath},
		Mode:          sop.ForWriting,
		CacheType:     sop.Redis,
	})
	if err != nil {
		log.Error("Seed transaction failed", "err", err)
		return
	}
	if err := t.Begin(ctx); err != nil {
		log.Error("Trans Begin failed", "err", err)
		return
	}

	storeOpts := sop.StoreOptions{
		Name:       "concurrent_tree",
		SlotLength: 500,
		IsUnique:   true,
	}

	b3, err := infs.NewBtree[int, string](ctx, storeOpts, t, func(a, b int) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if err != nil {
		log.Error(fmt.Sprintf("Seed NewBtree failed: %v", err))
	}

	b3.Add(ctx, -1, "Root Seed Item")
	if err := t.Commit(ctx); err != nil {
		log.Error(fmt.Sprintf("Seed commit failed: %v", err))
	}
}

func worker(ctx context.Context, id int, dbPath string, itemsPerThread int, maxJitter int, targetCount int64) bool {
	retryCount := 0
	committed := false

	for !committed && retryCount < 10 {
		t, err := infs.NewTransaction(ctx, sop.TransactionOptions{
			StoresFolders: []string{dbPath},
			Mode:          sop.ForWriting,
			CacheType:     sop.Redis,
		})
		if err != nil {
			log.Debug(fmt.Sprintf("Thread %d transaction creation failed: %v", id, err))
			return false
		}
		t.Begin(ctx)

		b3, err := infs.OpenBtree[int, string](ctx, "concurrent_tree", t, func(a, b int) int {
			if a < b {
				return -1
			}
			if a > b {
				return 1
			}
			return 0
		})
		if err != nil {
			t.Rollback(ctx)
			log.Debug(fmt.Sprintf("Thread %d OpenBtree failed: %v", id, err))
			retryCount++
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			continue
		}

		if b3.Count() >= targetCount {
			log.Info(fmt.Sprintf("Reached target count(%v), exiting", targetCount))
			t.Commit(ctx)
			return true
		}

		for j := 0; j < itemsPerThread; j++ {
			key := (id * itemsPerThread) + j
			value := fmt.Sprintf("Thread %d - Item %d", id, j)
			if ok, err := b3.Add(ctx, key, value); !ok || err != nil {
				log.Debug(fmt.Sprintf("Thread %d add failed for key %d: %v", id, key, err))
			}
			// Simulate work/network latency
			if maxJitter > 0 {
				time.Sleep(time.Duration(rand.Intn(maxJitter)) * time.Millisecond)
			}
		}

		if err := t.Commit(ctx); err != nil {
			retryCount++
			delay := time.Duration(rand.Intn(500)) * time.Millisecond
			log.Debug(fmt.Sprintf("Thread %d conflict detected (Retry %d): %v", id, retryCount, err))
			time.Sleep(delay)
		} else {
			committed = true
		}
	}

	if !committed {
		fmt.Printf("Thread %d failed after retries.\n", id)
	}

	return false
}

func verify(ctx context.Context, dbPath string, expectedCount int) {
	t, err := infs.NewTransaction(ctx, sop.TransactionOptions{
		StoresFolders: []string{dbPath},
		Mode:          sop.ForWriting,
		CacheType:     sop.Redis,
	})
	if err != nil {
		log.Error(fmt.Sprintf("Verify transaction failed: %v", err))
	}
	t.Begin(ctx)

	b3, err := infs.OpenBtree[int, string](ctx, "concurrent_tree", t, func(a, b int) int {
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	})
	if err != nil {
		log.Error(fmt.Sprintf("Verify OpenBtree failed: %v", err))
	}

	count := b3.Count()
	fmt.Printf("Final Count: %d (Expected: %d)\n", count, expectedCount)
	if int(count) != expectedCount {
		fmt.Println("FAILURE: Count mismatch.")
	} else {
		fmt.Println("SUCCESS: Count matches.")
	}
	t.Commit(ctx)
}
