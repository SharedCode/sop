# SOP AI Cookbook

This cookbook provides practical examples and patterns for using the SOP AI library.

## Table of Contents
1.  [Vector Store API](#vector-store-api)
    *   [Basic Setup (Standalone)](#basic-setup-standalone)
    *   [Ingesting Data](#ingesting-data)
    *   [Searching (Query)](#searching-query)
    *   [Filtering Results](#filtering-results)
    *   [Deleting Items](#deleting-items)
2.  [Model Store API](#model-store-api)
    *   [Saving a Model](#saving-a-model)
    *   [Loading a Model](#loading-a-model)
    *   [Listing Models](#listing-models)
    *   [Transactional Model Updates](#transactional-model-updates)

---

## Vector Store API

The Vector Store is the core component for RAG and semantic search.

### Basic Setup (Standalone)

Initialize a local, file-backed vector database.

```go
package main

import (
	"fmt"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/vector"
)

func main() {
	// 1. Create the Database Manager
	// Standalone mode = Local Filesystem + In-Memory Cache
	db := vector.NewDatabase[map[string]any](ai.Standalone)
	
	// 2. Configure Storage Path
	db.SetStoragePath("./my_vector_db")

	// 3. Open a Domain (Index)
	// This creates/opens a specific "table" or "collection" named "documents"
	idx := db.Open("documents")
	
	fmt.Println("Vector Store opened successfully!")
}
```

### Basic Setup (Clustered)

Initialize a clustered vector database using Redis for caching.

```go
func main() {
	// 1. Create the Database Manager
	// Clustered mode = Local Filesystem (Shared) + Redis Cache
	// Ensure Redis is running on localhost:6379
	db := vector.NewDatabase[map[string]any](ai.Clustered)
	
	// 2. Configure Storage Path
	db.SetStoragePath("./my_cluster_db")

	// 3. Open a Domain (Index)
	idx := db.Open("documents")
	
	fmt.Println("Clustered Vector Store opened successfully!")
}
```

### Ingesting Data

Add items (vectors + metadata) to the store.

```go
func ingestData(idx ai.VectorStore[map[string]any]) {
	// Define an item
	item := ai.Item[map[string]any]{
		ID:     "doc-101",
		Vector: []float32{0.1, 0.2, 0.3, 0.4}, // Normalized vector
		Payload: map[string]any{
			"title": "SOP Guide",
			"tags":  []string{"tech", "database"},
		},
	}

	// Upsert (Insert or Update)
	if err := idx.Upsert(item); err != nil {
		panic(err)
	}

	// Batch Upsert (More efficient for bulk load)
	items := []ai.Item[map[string]any]{
		{ID: "doc-102", Vector: []float32{0.5, 0.5, 0.5, 0.5}, Payload: map[string]any{"title": "AI Agents"}},
		{ID: "doc-103", Vector: []float32{0.9, 0.1, 0.1, 0.9}, Payload: map[string]any{"title": "Vector Search"}},
	}
	if err := idx.UpsertBatch(items); err != nil {
		panic(err)
	}
}
```

### Searching (Query)

Find the nearest neighbors to a query vector.

```go
func search(idx ai.VectorStore[map[string]any]) {
	queryVec := []float32{0.1, 0.2, 0.3, 0.4}
	k := 5 // Number of results

	// Perform Search
	hits, err := idx.Query(queryVec, k, nil)
	if err != nil {
		panic(err)
	}

	for _, hit := range hits {
		fmt.Printf("ID: %s, Score: %.4f, Title: %v\n", 
			hit.ID, hit.Score, hit.Payload["title"])
	}
}
```

### Filtering Results

Apply metadata filters to search results.

```go
func searchWithFilter(idx ai.VectorStore[map[string]any]) {
	queryVec := []float32{0.1, 0.2, 0.3, 0.4}

	// Define Filter Function
	filter := func(payload map[string]any) bool {
		// Only return items with "tech" tag
		tags, ok := payload["tags"].([]string)
		if !ok {
			return false
		}
		for _, t := range tags {
			if t == "tech" {
				return true
			}
		}
		return false
	}

	hits, _ := idx.Query(queryVec, 5, filter)
	// ... process hits
}
```

### Deleting Items

Remove items by ID.

```go
func deleteItem(idx ai.VectorStore[map[string]any]) {
	if err := idx.Delete("doc-101"); err != nil {
		panic(err)
	}
}
```

---

## Model Store API

The Model Store allows you to persist AI model artifacts (weights, configs) alongside your data.

### Saving a Model

Save a struct or any serializable object as a named model.

```go
import (
	"context"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
)

type MyPerceptron struct {
	Weights []float64
	Bias    float64
}

func saveModel() {
	// Initialize Database
	db := database.NewDatabase(ai.Standalone, "./my_models")
	store, _ := db.OpenModelStore("default")

	model := MyPerceptron{
		Weights: []float64{0.5, -0.2, 1.0},
		Bias:    0.1,
	}

	// Save
	ctx := context.Background()
	if err := store.Save(ctx, "classifiers", "perceptron_v1", model); err != nil {
		panic(err)
	}
}
```

### Loading a Model

Retrieve a model by name.

```go
func loadModel() {
	db := database.NewDatabase(ai.Standalone, "./my_models")
	store, _ := db.OpenModelStore("default")
	ctx := context.Background()

	var loadedModel MyPerceptron
	if err := store.Load(ctx, "classifiers", "perceptron_v1", &loadedModel); err != nil {
		panic(err)
	}

	fmt.Printf("Loaded Bias: %f\n", loadedModel.Bias)
}
```

### Listing Models

See what's in the store.

```go
func listModels() {
	db := database.NewDatabase(ai.Standalone, "./my_models")
	store, _ := db.OpenModelStore("default")
	names, _ := store.List(context.Background(), "classifiers")
	
	for _, name := range names {
		fmt.Println("Found model:", name)
	}
}
```

### Transactional Model Updates

Use `BTreeModelStore` to update models and vectors atomically. This ensures that your model weights always match the vector index version.

```go
import (
	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/database"
	"github.com/sharedcode/sop/inredfs"
)

func atomicUpdate() {
	// 1. Start a Transaction
	// (Assuming we have a transaction factory or manager set up)
	trans, _ := inredfs.NewTransaction(ctx, options)
	trans.Begin(ctx)

	// 2. Open Transactional Stores
	// Vector Store (bound to transaction)
	vecStore := myVectorDB.Open("documents").WithTransaction(trans)
	
	// Model Store (bound to transaction)
	modelStore, _ := database.NewBTreeModelStore(ctx, trans)

	// 3. Perform Updates
	// Update Vector
	vecStore.Upsert(ai.Item[any]{ID: "doc-1", Vector: newVec, Payload: "updated"})

	// Update Model
	newWeights := trainOn(newVec)
	modelStore.Save(ctx, "classifiers", "embedding_model", newWeights)

	// 4. Commit (All or Nothing)
	// If this fails, neither the vector nor the model is updated.
	trans.Commit(ctx)
}
```
