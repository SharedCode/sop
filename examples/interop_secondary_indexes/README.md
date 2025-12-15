# Interoperability Demo: Secondary Indexes

This example demonstrates how to use SOP's `jsondb` package to create a "schema-less" store with composite secondary indexes. This approach ensures that your data sorting and indexing rules are compatible across all SOP language bindings (Go, Python, Java, C#, Rust).

## Overview

The example creates a database of products and indexes them by:
1.  **Category** (Ascending)
2.  **Price** (Descending)

This means products will be grouped by category, and within each category, the most expensive items will appear first.

## Key Features

*   **`jsondb.NewJsonBtreeMapKey`**: Uses a dynamic map for keys, allowing flexible fields.
*   **`IndexSpecification`**: Defines the sorting order for multiple fields.
*   **Data Generation**: The script automatically generates **500 records** across various categories (Electronics, Books, Clothing, Home, Garden) to populate the database for testing and visualization.

## Running the Example

From the root of the `sop` repository:

```bash
go run examples/interop_secondary_indexes/main.go
```

## Verifying with Data Browser

After running this example, you can use the **SOP Data Browser** to inspect the data.

1.  Start the Data Browser:
    ```bash
    go run tools/data_browser/main.go -registry ./data/interop_indexes
    ```
2.  Open [http://localhost:8080](http://localhost:8080).
3.  Select the `products_by_category_price` store.
4.  You can now browse the 500 generated records, navigate through pages, and search for specific items using the complex key search feature.
