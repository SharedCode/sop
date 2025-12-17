# SOP Python Examples

This directory contains examples of how to use the SOP Python wrapper.

## Large Complex Data Demo (`large_complex_demo.py`)

This script generates a substantial dataset to demonstrate SOP's capability to handle complex, composite keys and large value payloads. It is the **perfect companion for testing the SOP Data Browser**.

It creates a database with two distinct stores:
1.  **`people`**: Uses a composite key of `(Country, City, SSN)`. This demonstrates how SOP can efficiently index and search multi-part keys.
2.  **`products`**: Uses a composite key of `(Category, SKU)`.

### Running the Demo & Browser

1.  **Generate the Data**:
    ```bash
    python3 examples/large_complex_demo.py
    ```
    This will create a database folder at `data/large_complex_db`.

2.  **Explore with Data Browser**:
    Now you can use the `sop-browser` to inspect, search, and modify this data.
    ```bash
    sop-browser -registry data/large_complex_db
    ```
    *   Try searching for a specific Country (e.g., "US") to see B-Tree prefix matching in action.
    *   Edit a record to test the transactional update capabilities.

## LangChain Demo (`langchain_demo.py`)

This script demonstrates how to adapt the SOP Vector Database to work with the **LangChain** interface pattern.

It implements a simple `SOPVectorStore` class that mimics `langchain.vectorstores.VectorStore`, allowing you to use SOP as a backend for your LangChain applications.

### Running the Demo

1.  Ensure you have the SOP shared library built and available.
2.  Run the script:

```bash
python3 examples/langchain_demo.py
```

**Note**: This demo uses a "Mock" embedder (simple hashing) to avoid requiring external dependencies like OpenAI or HuggingFace. In a real application, you would replace `SimpleHashEmbedder` with `OpenAIEmbeddings` or similar.

## Model Store Demo (`modelstore_demo.py`)

This script demonstrates how to use the **Model Store** feature of SOP. The Model Store allows you to save, load, list, and delete arbitrary Python objects (like ML models, configurations, or metadata) alongside your vector data.

It showcases:
*   Initializing a Unified Database.
*   Opening a named Model Store.
*   Saving a Python object (dataclass).
*   Retrieving and verifying the object.
*   Deleting the object.

### Running the Demo

```bash
python3 examples/modelstore_demo.py
```
