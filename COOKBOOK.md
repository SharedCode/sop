# SOP API Cookbook

Simple, copy-pasteable examples for common SOP scenarios.

> **Note**: This cookbook focuses on the Go API. For Python examples, see the [Python Cookbook](jsondb/python/COOKBOOK.md).

## 1. Storing 100k User Profiles (`database`)

This example demonstrates how to store structured data using the high-level `database` package.

```go
package main

import (
	"context"
	"fmt"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
)

type UserProfile struct {
	ID    string
	Name  string
	Email string
}

func main() {
	// 1. Initialize Database (Standalone or Clustered)
	// Standalone uses in-memory caching; Clustered uses Redis.
	db := database.NewDatabase(database.DatabaseOptions{
		DBType:      database.Standalone,
		StoragePath: "/tmp/sop_data",
	})

	// 2. Start Transaction
	ctx := context.Background()
	trans, err := db.BeginTransaction(ctx, sop.ForWriting)
	if err != nil {
		panic(err)
	}

	// 3. Open/Create Store
	// Configure: Medium data size
	opts := sop.ConfigureStore("users", true, 1000, "User Profiles", sop.MediumData, "")
	store, err := db.NewBtree(ctx, "users", trans, opts)
	if err != nil {
		panic(err)
	}

	// 4. Add Data
	for i := 0; i < 100000; i++ {
		id := fmt.Sprintf("user_%d", i)
		profile := UserProfile{ID: id, Name: "John Doe", Email: "john@example.com"}
		// Note: In a real app, you might want to serialize 'profile' to JSON string if using generic store,
		// or use the generic B-Tree directly if you want type safety.
		// The Database wrapper currently returns BtreeInterface[string, any].
		store.Add(ctx, id, profile)
		
		// Commit every 1000 items to keep memory usage low
		if i%1000 == 0 {
			trans.Commit(ctx)
			// Start new transaction
			trans, _ = db.BeginTransaction(ctx, sop.ForWriting)
			// Re-open store in new transaction
			store, _ = db.OpenBtree(ctx, "users", trans)
		}
	}
	
	trans.Commit(ctx)
	fmt.Println("Done!")
}
```

## 2. Multi-Store Transaction

Atomically update a "Bank Account" and a "Transaction Log" in the same transaction.

```go
func TransferFunds(ctx context.Context, db *database.Database, fromID, toID string, amount float64) error {
	// 1. Start Transaction
	trans, _ := db.BeginTransaction(ctx, sop.ForWriting)

	// 2. Open Stores
	accounts, _ := db.OpenBtree(ctx, "accounts", trans)
	logs, _ := db.OpenBtree(ctx, "logs", trans)

	// 3. Deduct
	var fromAccount Account
	if found, _ := accounts.FindOne(ctx, fromID, true); found {
		val, _ := accounts.GetCurrentValue(ctx)
		// Cast/Unmarshal val to Account...
		fromAccount = val.(Account)
		fromAccount.Balance -= amount
		accounts.UpdateCurrentValue(ctx, fromAccount)
	}

	// 4. Add
	var toAccount Account
	if found, _ := accounts.FindOne(ctx, toID, true); found {
		val, _ := accounts.GetCurrentValue(ctx)
		toAccount = val.(Account)
		toAccount.Balance += amount
		accounts.UpdateCurrentValue(ctx, toAccount)
	}

	// 5. Log
	logs.Add(ctx, uuid.NewString(), LogEntry{Action: "Transfer", Amount: amount})

	// 6. Commit (All or Nothing)
	return trans.Commit(ctx)
}
```

## 3. Using Streaming Data (Large Files)

For objects larger than 1MB (e.g., videos, high-res images), use the `streamingdata` package.

```go
import "github.com/sharedcode/sop/streamingdata"

func UploadVideo(ctx context.Context, videoID string, data []byte) error {
	// Create a streaming store
	store := streamingdata.NewStore(ctx, "videos")
	
	// Create a writer (Encoder)
	writer, err := store.NewWriter(ctx, videoID)
	if err != nil {
		return err
	}
	
	// Write data (chunks are handled automatically)
	if _, err := writer.Write(data); err != nil {
		return err
	}
	
	// Close to flush and finalize
	return writer.Close()
}
```

## 4. Advanced: Building a Vector Database
SOP's B-Trees are perfect for storing vector embeddings and their metadata.

```go
// 1. Define your Vector Item
type VectorItem struct {
    ID        uuid.UUID
    Embedding []float32
    Content   string
}

// 2. Initialize Database
db := database.NewDatabase(database.DatabaseOptions{
    DBType:      database.Standalone,
    StoragePath: "/tmp/sop_vectors",
})

// 3. Start Transaction
trans, _ := db.BeginTransaction(ctx, sop.ForWriting)

// 4. Open Vector Store
// Note: VectorStore expects map[string]any payload by default in Database wrapper
store, _ := db.OpenVectorStore(ctx, "vector_store", trans, vector.Config{
    UsageMode: ai.Dynamic,
})

// 5. Insert Vectors
item := ai.Item[map[string]any]{
    ID:     uuid.NewString(),
    Vector: []float32{0.1, 0.2, 0.3},
    Payload: map[string]any{"content": "hello world"},
}
store.Upsert(ctx, item)

// 6. Commit
trans.Commit(ctx)
```

## 4. Text Search

Index and search text documents transactionally.

```go
package main

import (
	"context"
	"fmt"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai/database"
)

func main() {
	ctx := context.Background()
	db := database.NewDatabase(database.DatabaseOptions{
		DBType:      database.Standalone,
		StoragePath: "/tmp/sop_data",
	})

	// 1. Index Documents
	t1, _ := db.BeginTransaction(ctx, sop.ForWriting)
	idx, _ := db.OpenSearch(ctx, "articles", t1)
	
	idx.Add(ctx, "doc1", "The quick brown fox")
	idx.Add(ctx, "doc2", "jumps over the lazy dog")
	
	t1.Commit(ctx)

	// 2. Search
	t2, _ := db.BeginTransaction(ctx, sop.ForReading)
	idx, _ = db.OpenSearch(ctx, "articles", t2)
	
	results, _ := idx.Search(ctx, "fox")
	for _, r := range results {
		fmt.Printf("Doc: %s, Score: %f\n", r.DocID, r.Score)
	}
	t2.Commit(ctx)
}
```
