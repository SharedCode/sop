# SOP AI Kit

> **Note**: This package was developed with an AI copilot. I want to keep an open development approach (not finicky and narrow) in this package to keep efficient in "automaton" cycles. Thus, source codes here may be subject to refactor because of AI first philosophy.

The `sop/ai` package is the **SOP AI Kit** — a versatile **AI Platform** that transforms SOP from a storage engine into a complete **Computing Platform**.

It enables you to build:
*   **Domain-Specific AI Assistants**: Adapt the Scripting engine to any vertical (Finance, Healthcare, Media).
*   **Embedded Applications**: Combine the database + scripting engine for powerful edge devices.
*   **Personal AI Assistants**: Privacy-first, web-based assistants.
*   **Enterprise AI Systems**: Acid-compliant RAG and autonomous agents.

It provides a complete toolkit for building local, privacy-first AI applications backed by the power of SOP's B-Tree storage engine.

## Core Components

### 1. Vector Database (`ai/vector`)
A persistent, ACID-compliant vector store that runs on your local filesystem.
*   **Storage**: Uses SOP B-Trees to store vectors and metadata.
*   **Architecture**: Uses a "Flat" directory structure where all B-Trees (Vectors, Centroids, Content) reside in a shared root folder, managed by a `sys_config` registry.
*   **Modes**: Supports **Standalone** (In-Memory Cache) for local use and **Clustered** (Redis Cache) for distributed deployments.
*   **Search**: Supports cosine similarity search with metadata filtering.
*   **Partitioning**: Designed for massive scale via natural partitioning.
*   **Optimization**: Built-in `Optimize()` method to rebalance clusters (Centroids) and ensure optimal search performance as data grows.
    *   **Scalability**: The optimization process is batched (commits every 200 items), allowing it to scale to millions of records without hitting transaction timeouts.
    *   **Operational Constraint**: To ensure data consistency and simplicity, the Vector Store enters a **Read-Only** mode during optimization. Any attempts to `Upsert` or `Delete` will return an error until `Optimize` completes.
    *   **Crash Recovery**: If the process crashes during optimization, simply restart it. The next call to `Optimize` will automatically detect and clean up any stale artifacts before starting fresh.
    *   **Rolling Version Safety**: When migrating items to a new optimization version, the system promotes the "Next" state to the "Current" state before overwriting it. This ensures that if the optimization process crashes mid-flight, the data remains accessible via the valid "Current" version, preventing data loss or corruption during the transition.
*   **Deletion & Cleanup**: Implements a **Tombstone** mechanism for efficient deletions.
    *   **Soft Delete**: `Delete()` marks items as deleted in both the Index and Content stores, ensuring they are immediately hidden from search results.
    *   **Garbage Collection**: The `Optimize()` process acts as a Garbage Collector. It detects these tombstones and performs a physical delete on the underlying data, reclaiming storage space during the maintenance cycle.
*   **Deduplication**: Optional deduplication check during ingestion. Can be disabled (`SetDeduplication(false)`) for maximum write performance when data is known to be unique.
*   **Rich Key Structure**: The Vector Store uses a specialized `ContentKey` struct as the B-Tree key.
    *   **Metadata Carrier**: Stores `CentroidID`, `Distance`, `Version`, and `Deleted` status directly in the key.
    *   **Efficiency**: Allows the system to perform structural operations (like filtering deleted items or finding vectors in a specific cluster) by scanning only the keys, without fetching the potentially large vector payload.
*   **Usage Modes**:
    *   **BuildOnceQueryMany**: Optimized for static datasets. Ingest data -> Call `Optimize()` -> Serve queries. Discards temporary build artifacts for efficiency.
    *   **Dynamic**: For systems with continuous updates. Maintains auxiliary structures to handle frequent inserts/deletes.
    *   **DynamicWithVectorCountTracking**: Specialized mode for external centroid management (e.g., Agents). It tracks vector counts per centroid to help you decide when to trigger `Optimize()`.

### 2. Versatile Scripting Engine (`ai/agent`)
The core of the **Computing Platform**. It allows you to define complex, multi-step workflows using **Natural Language Programming** (Scripts/Scripts).
*   **Adaptable**: Can be tailored to any domain (Finance, Media, etc.) by registering custom Tools.
*   **Swarm Computing**: Async execution of steps for high-performance parallel processing.
*   **Agent Framework**: Define agents with Personality, Memory, and Tool access.
*   **Interoperable**: Scripts are stored as JSON and can be managed/visualized via the SOP Data Manager.

See [ai/agent/README.md](ai/agent/README.md) for full documentation on Scripts, Swarm Computing, and the Tool Registry.

### 3. Generators & Embedders (`ai/generator`, `ai/embed`)
Interfaces for connecting to AI models:
*   **Generators**: Connect to LLMs like OpenAI, Gemini, or local Ollama instances.
*   **Embedders**: Convert text to vectors. Includes a "Simple" keyword-based embedder (for testing) and an "Agent Embedder" (for semantic understanding).

### 4. Model Store (`ai/database/model_store.go`)
A unified interface for persisting AI models, from small "Skills" (Perceptrons) to large "Brains" (Neural Nets).
*   **Backend**: Uses transactional B-Tree storage (`BTreeModelStore`) for reliability and consistency.
*   **Categorization**: Models are stored with a composite key `{Category, Name}`, allowing for organized grouping of model artifacts.
*   **Transactional**: The B-Tree backend allows model updates to be part of the same ACID transaction as vector data changes.

### 5. Text Search (`search`)
A transactional, embedded text search engine.
*   **ACID Compliant**: Index documents within the same transaction as your data.
*   **BM25 Scoring**: Uses industry-standard ranking for relevance.
*   **Architecture**: Stores Inverted Indices in SOP B-Trees.
*   **Usage**: Ideal for "Search this wiki" or "Filter by text" features alongside Vector Search.

### 6. Script System (`ai/SCRIPTS.md`)
A "Natural Language Programming" engine.
*   **Compiled Instructions**: Turns natural language intent into deterministic, high-performance SOP programs.
*   **Mini-SDK**: A stable JSON schema (`ask`, `set`, `if`, `loop`, `fetch`) for scripting complex logic.
*   **Bare Metal Performance**: Executes loops and data fetches directly in Go, using the LLM only for reasoning.
*   [Read the full documentation](SCRIPTS.md).

### 7. AI Assistant (Interactive Mode)
A conversational interface for interacting with your data and building scripts.
*   **Natural Language Queries**: "Select all users where role is admin".
*   **CRUD Operations**: Add, update, and delete records using plain English.
*   **Script Recording**: Teach the assistant workflows by doing them step-by-step.
*   [Read the User Guide](AI_ASSISTANT_USAGE.md).

## Standards & Compatibility

The SOP AI Kit is designed to play nicely with the broader AI ecosystem while adhering to strict software engineering standards.

### Supported Interfaces
*   **Generators (LLMs)**:
    *   **Google (Gemini)**: Native support for Gemini Pro. **(Default & Tested)**
    *   **OpenAI (ChatGPT)**: Native support for GPT-3.5/4.
    *   **Ollama**: Native support for local models (Llama 3, Mistral, Gemma).
    *   **Custom**: Implement the `ai.Generator` interface to connect any other provider.
*   **Embedders**:
    *   **Ollama**: Use local models for embeddings.
    *   **Agent-as-Embedder**: Use another SOP Agent to "embed" (translate) text, enabling recursive agent architectures.
*   **Vector Store**:
    *   **LangChain**: The Python wrapper (`sop4py`) includes convenience methods for LangChain integration.
    *   **Generic**: The Go API uses generics (`VectorStore[T]`), allowing you to store strongly-typed structs or dynamic `map[string]any` payloads.

### Deployment Standards
*   **ACID Compliance**: Full Two-Phase Commit (2PC) support for distributed transactions.
*   **Storage**: Uses standard filesystem paths (no proprietary binary blobs hidden in OS folders).
*   **Caching**: Supports standard Redis protocol for clustered caching. **Note**: Redis is NOT used for data storage, just for coordination & to offer built-in caching.
*   **Replication & High Availability**:
    *   **General Purpose**: Supports full replication (Erasure Coding, Active/Passive) in all modes.
    *   **AI Package**:
        *   **Standalone Mode**: Supports replication if configured, though typically used for single-folder local storage.
        *   **Clustered Mode**: Supports full replication. Configurable via `VectorDBOptions` (Python) or `Database` struct (Go). Replication is **optional** in both modes.

## Unified Architecture

The SOP AI package is built as a high-level abstraction layer on top of the General Purpose SOP engine. This design ensures that both use cases share the same robust foundation while offering appropriate interfaces for their respective domains.

*   **Shared Engine**: Both packages use the same `infs` B-Tree storage engine, ensuring identical performance, reliability, and ACID compliance.
*   **Separation of Concerns**:
    *   **General Purpose (`sop`)**: Exposes low-level B-Tree primitives and explicit transaction management for building custom data structures (Key-Value stores, Registries).
    *   **AI Package (`sop/ai`)**: Abstracts B-Trees into domain-specific "Vector Stores" and "Model Stores" with implicit transaction handling for ease of use.
*   **The Bridge**: You can mix both worlds in a single atomic transaction. By creating a General Purpose transaction and "binding" an AI Store to it, you can update a User Profile (Key-Value) and their Embedding (Vector) simultaneously. See the [Tutorial](TUTORIAL.md#step-12-unified-architecture-the-bridge) for an example.

## API Cookbook

For detailed code examples and usage patterns, please see the [AI Cookbook](COOKBOOK.md).

## Model Store Tutorial

For a deep dive into persisting AI models, configurations, and weights, see the [Model Store Tutorial](MODEL_STORE_TUTORIAL.md).

## Usage as a Library

You can use the `ai` package directly in your Go applications to build custom solutions.

### Example: Building a Simple RAG App

```go
package main

import (
    "context"
    "fmt"
    "github.com/sharedcode/sop/ai"
    "github.com/sharedcode/sop/database"
    "github.com/sharedcode/sop/ai/embed"
)

func main() {
    // 1. Initialize the Vector Database
    db := database.NewDatabase(sop.DatabaseOptions{
        Type:          sop.Standalone,
        StoresFolders: []string{"./my_knowledge_base"},
    })
    
    // 2. Start a Transaction
    ctx := context.Background()
    trans, _ := db.BeginTransaction(ctx, sop.ForWriting)
    defer trans.Rollback(ctx) // Safety rollback

    // 3. Open an index for a specific domain (e.g., "documents")
    idx, _ := db.OpenVectorStore(ctx, "documents", trans, vector.Config{})

    // 4. Initialize an Embedder
    // (In production, use a real embedding model. Here we use the simple keyword hasher)
    emb := embed.NewSimple("simple-embedder", 64, nil)

    // 5. Add Data (Upsert)
    item := ai.Item[map[string]any]{
        ID: "doc-1",
        Vector: nil, // Will be filled below
        Payload: map[string]any{
            "text": "SOP is a high-performance Go library for storage.",
            "category": "tech",
        },
    }
    // Generate vector
    vecs, _ := emb.EmbedTexts(ctx, []string{item.Payload["text"].(string)})
    item.Vector = vecs[0]

    // Save to DB
    idx.UpsertBatch(ctx, []ai.Item[map[string]any]{item})
    
    // Commit the transaction
    trans.Commit(ctx)

    // 6. Search (Retrieve) - New Read Transaction
    trans, _ = db.BeginTransaction(ctx, sop.ForReading)
    idx, _ = db.OpenVectorStore(ctx, "documents", trans, vector.Config{})
    
    query := "storage library"
    queryVecs, _ := emb.EmbedTexts(ctx, []string{query})
    
    hits, _ := idx.Query(ctx, queryVecs[0], 5, nil)
    
    for _, hit := range hits {
        fmt.Printf("Found: %s (Score: %.2f)\n", hit.Payload["text"], hit.Score)
    }
    trans.Commit(ctx)
}
```

# The Doctor Demo: A Local RAG Pipeline

This demo showcases a complete "Doctor-Nurse" AI pipeline running entirely locally. It demonstrates how to chain agents together using the SOP AI framework.

## Architecture

The system consists of two agents working in a pipeline:

1.  **Nurse Agent (`nurse_local`)**:
    *   **Role**: The "Translator".
    *   **Task**: Takes colloquial patient symptoms (e.g., "tummy hurt", "hot") and translates them into standardized clinical terms (e.g., "abdominal pain", "fever").
    *   **Mechanism**: Uses a local vector database to find the closest matching clinical terms.

2.  **Doctor Agent (`doctor_pipeline`)**:
    *   **Role**: The "Diagnostician".
    *   **Task**: Takes the clinical terms from the Nurse and searches its medical knowledge base to suggest possible conditions.
    *   **Mechanism**: Uses a separate local vector database populated with disease-symptom mappings.

## ETL Workflow (Data Ingestion)

Before the agents can run, we must build their knowledge bases. We use a dedicated ETL (Extract, Transform, Load) tool called `sop-etl`.

The entire process is defined in `etl_workflow.json` and consists of three steps:

1.  **Prepare**: Downloads a raw healthcare dataset (CSV) and converts it into JSON format (`doctor_data.json`).
2.  **Build Nurse DB**: Ingests the data into the Nurse's vector store (`data/nurse_local`), indexing symptoms for semantic retrieval.
3.  **Build Doctor DB**: Ingests the data into the Doctor's vector store (`data/doctor_core`), indexing diseases and their associated symptoms.

## Quick Start

We provide a script to build the tools, run the ETL pipeline, and verify the agents.

1.  **Run the Rebuild Script**:
    ```bash
    ./rebuild_doctor.sh
    ```
    This script will:
    *   Build `sop-etl` and `sop-ai` binaries.
    *   Clean up old data.
    *   Run the ETL workflow defined in `etl_workflow.json`.
    *   Run sanity tests.

2.  **Run the Agent Manually**:
    Once the data is built, you can chat with the Doctor agent:
    ```bash
    ./sop-ai -config data/doctor_pipeline.json
    ```
    **Example Interaction**:
    ```text
    Patient> I have a bad cough and a runny nose
    AI Doctor: [1] Common Cold... (Score: 0.92)
    ```

## Configuration Files

*   **`etl_workflow.json`**: Defines the ETL pipeline steps and parameters (e.g., batch sizes, input/output paths).
*   **`data/doctor_pipeline.json`**: Configuration for the main Doctor agent. It specifies that it should use the `nurse_local` agent as its "embedder" (translator).
*   **`data/nurse_local.json`**: Configuration for the Nurse agent.

## Heuristic vs LLM Embedders

The system supports two types of "Nurse" agents for embedding/translation:

1.  **Heuristic Agent (`nurse_local`)**:
    *   **How it works**: Uses a local dictionary and vector search with manually curated synonyms.
    *   **Performance**: Extremely fast and deterministic.
    *   **Use Case**: Default for this demo. Tuned for high performance in specific areas (e.g., lung-related diseases).
    *   **Pros**: No external dependencies (no Ollama required), predictable.
    *   **Cons**: Requires manual tuning for new slang/terms.

2.  **LLM Agent (`nurse_translator`)**:
    *   **How it works**: Uses a local LLM (via Ollama) to semantically understand and translate user input.
    *   **Performance**: Slower (depends on GPU/CPU), but more flexible.
    *   **Use Case**: General-purpose understanding without manual synonym mapping.
    *   **Pros**: Understands context and nuance better out-of-the-box.
    *   **Cons**: Requires running Ollama, higher latency.

To switch between them, you would update the `embedder` configuration in the agent's JSON file.
