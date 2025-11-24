# SOP API Cookbook

Simple, copy-pasteable examples for common SOP scenarios.

## 1. Storing 100k User Profiles (`inredcfs`)

This example demonstrates how to store structured data using the Hybrid backend.

```go
package main

import (
	"context"
	"fmt"
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/inredcfs"
)

type UserProfile struct {
	ID    string
	Name  string
	Email string
}

func main() {
	// 1. Initialize (Cassandra + Redis)
	// Assume config structs are populated
	if err := inredcfs.Initialize(cassandraConfig, redisConfig); err != nil {
		panic(err)
	}
	defer inredcfs.Shutdown()

	// 2. Open/Create Store
	trans, err := inredcfs.NewTransaction(true, -1)
	if err != nil {
		panic(err)
	}
	
	// Configure: Medium data size, cached in Redis
	opts := sop.ConfigureStore("users", true, 1000, "User Profiles", sop.MediumData, "")
	store, err := inredcfs.NewBtree[string, UserProfile](context.Background(), opts, trans)
	if err != nil {
		panic(err)
	}

	// 3. Add Data in a Transaction
	trans.Begin()
	
	for i := 0; i < 100000; i++ {
		id := fmt.Sprintf("user_%d", i)
		profile := UserProfile{ID: id, Name: "John Doe", Email: "john@example.com"}
		store.Add(context.Background(), id, profile)
		
		// Commit every 1000 items to keep memory usage low
		if i%1000 == 0 {
			trans.Commit(context.Background())
			trans.Begin()
		}
	}
	
	trans.Commit(context.Background())
	fmt.Println("Done!")
}
```

## 2. Multi-Store Transaction

Atomically update a "Bank Account" and a "Transaction Log" in the same transaction.

```go
func TransferFunds(ctx context.Context, fromID, toID string, amount float64) error {
	trans, _ := inredcfs.NewTransaction(true, -1)
	trans.Begin()

	accounts, _ := inredcfs.NewBtree[string, Account](ctx, accountOpts, trans)
	logs, _ := inredcfs.NewBtree[string, LogEntry](ctx, logOpts, trans)

	// 1. Deduct
	var fromAccount Account
	accounts.FindOne(ctx, fromID, true)
	accounts.GetCurrentValue(ctx, &fromAccount)
	fromAccount.Balance -= amount
	accounts.UpdateCurrentItem(ctx, fromAccount)

	// 2. Add
	var toAccount Account
	accounts.FindOne(ctx, toID, true)
	accounts.GetCurrentValue(ctx, &toAccount)
	toAccount.Balance += amount
	accounts.UpdateCurrentItem(ctx, toAccount)

	// 3. Log
	logs.Add(ctx, uuid.NewString(), LogEntry{Action: "Transfer", Amount: amount})

	// 4. Commit (All or Nothing)
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

// 2. Open a Store (using inredfs for local file storage)
store, _ := inredfs.OpenStore[uuid.UUID, VectorItem](ctx, "vector_store")

// 3. Insert Vectors
trans, _ := inredfs.NewTransaction(ctx, true, -1)
trans.Begin()
store.Add(ctx, item.ID, item)
trans.Commit(ctx)

// 4. Retrieve and Search (Conceptual)
// In a real app, you would load vectors into an HNSW index for similarity search,
// using SOP as the durable storage for the actual data.
```
