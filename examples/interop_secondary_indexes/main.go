package main

import (
	"context"
	"fmt"

	"github.com/sharedcode/sop"
	"github.com/sharedcode/sop/database"
	"github.com/sharedcode/sop/encoding"
	"github.com/sharedcode/sop/jsondb"
)

// Product is the value we want to store.
type Product struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

func main() {
	fmt.Println("--- Interoperability Demo (Secondary Indexes) ---")
	fmt.Println("This example creates a database with a composite index on the Key.")
	fmt.Println("The Key is a map[string]any, allowing dynamic fields.")

	ctx := context.Background()

	// 1. Initialize Database Options
	dbOpts := sop.DatabaseOptions{
		Type:          sop.Standalone,
		StoresFolders: []string{"./data/interop_indexes"},
	}

	// 2. Define Index Specification
	// We want to index by "category" (Ascending) and then "price" (Descending).
	// This means products will be grouped by category, and within each category, expensive ones come first.
	idxSpec := jsondb.NewIndexSpecification([]jsondb.IndexFieldSpecification{
		{FieldName: "category", AscendingSortOrder: true},
		{FieldName: "price", AscendingSortOrder: false},
	})

	// Marshal spec to JSON string
	specBytes, _ := encoding.DefaultMarshaler.Marshal(idxSpec)
	specStr := string(specBytes)

	// 3. Start Transaction
	trans, err := database.BeginTransaction(ctx, dbOpts, sop.ForWriting)
	if err != nil {
		panic(err)
	}

	// 4. Create Store with Index Specification
	// Note: We use NewJsonBtreeMapKey for dynamic keys.
	// This mode is highly performant and fully interoperable with other language bindings.
	storeOpts := sop.StoreOptions{
		Name:       "products_by_category_price",
		SlotLength: 100,
	}
	store, err := jsondb.NewJsonBtreeMapKey(ctx, dbOpts, storeOpts, trans, specStr)
	if err != nil {
		panic(err)
	}

	// 5. Add Data
	// We construct keys that match our index specification.
	// Extra fields in the key (like "id") are allowed but ignored by the sorter unless added to spec.

	products := []struct {
		Category string
		Product  Product
	}{
		{"Electronics", Product{"p1", "Laptop", 1200.00}},
		{"Electronics", Product{"p2", "Mouse", 25.00}},
		{"Electronics", Product{"p3", "Monitor", 300.00}},
		{"Books", Product{"b1", "Go Programming", 45.00}},
		{"Books", Product{"b2", "Novel", 15.00}},
	}

	items := make([]jsondb.Item[map[string]any, any], len(products))
	for i, p := range products {
		// Key must contain the fields defined in IndexSpecification
		key := map[string]any{
			"category": p.Category,
			"price":    p.Product.Price,
			"id":       p.Product.ID, // Optional, useful for uniqueness if needed
		}
		val := any(p.Product)
		items[i] = jsondb.Item[map[string]any, any]{
			Key:   key,
			Value: &val,
		}
	}

	store.Add(ctx, items)

	if err := trans.Commit(ctx); err != nil {
		panic(err)
	}
	fmt.Println("Wrote 5 products to the database.")

	// 6. Read Data Back (Verify Order)
	transRead, _ := database.BeginTransaction(ctx, dbOpts, sop.ForReading)
	storeRead, _ := jsondb.OpenJsonBtreeMapKey(ctx, dbOpts, "products_by_category_price", transRead)

	fmt.Println("\nReading all items (Should be sorted by Category ASC, Price DESC):")

	// Iterate through the store
	storeRead.First(ctx)
	for {
		k := storeRead.BtreeInterface.GetCurrentKey()
		v, _ := storeRead.GetCurrentValue(ctx) // Returns JSON string of the item

		// We can also get the Go value directly
		// val, _ := storeRead.BtreeInterface.GetCurrentValue(ctx)

		fmt.Printf("Key: %-15s | Price: %-8v | JSON: %s\n",
			k.Key["category"],
			k.Key["price"],
			v)

		if ok, _ := storeRead.Next(ctx); !ok {
			break
		}
	}

	transRead.Commit(ctx)
	fmt.Println("\nSuccess! The data is sorted according to the IndexSpecification.")
}
