# SOP Python Examples

This directory contains examples of how to use the SOP Python wrapper.

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
