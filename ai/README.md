# AI Package

This package was developed with an AI copilot, I want to keep an open development approach(not finicky and narrow) in this package to keep efficient in "automaton" usage. Please note that this package's codebase may undergo refinements as time permits. But it seems the way AI
copilot modelled the constructs follow industry/golang community standards.

# SOP AI Library

The `sop/ai` package provides a complete toolkit for building local, privacy-first AI applications backed by the power of SOP's B-Tree storage engine. It enables you to build Vector Databases, RAG (Retrieval-Augmented Generation) systems, and Autonomous Agents without external database dependencies.

## Core Components

### 1. Vector Database (`ai/vector`)
A persistent, ACID-compliant vector store that runs on your local filesystem.
*   **Storage**: Uses SOP B-Trees to store vectors and metadata.
*   **Search**: Supports cosine similarity search with metadata filtering.
*   **Partitioning**: Designed for massive scale via natural partitioning.

### 2. Agent Framework (`ai/agent`)
A flexible framework for defining AI agents with:
*   **Personality**: System prompts and policies.
*   **Memory**: Long-term knowledge retrieval from the Vector DB.
*   **Tools**: Capability to use other agents (e.g., for translation or embedding).

### 3. Generators & Embedders (`ai/generator`, `ai/embed`)
Interfaces for connecting to AI models:
*   **Generators**: Connect to LLMs like OpenAI, Gemini, or local Ollama instances.
*   **Embedders**: Convert text to vectors. Includes a "Simple" keyword-based embedder (for testing) and an "Agent Embedder" (for semantic understanding).

## Usage as a Library

You can use the `ai` package directly in your Go applications to build custom solutions.

### Example: Building a Simple RAG App

```go
package main

import (
    "context"
    "fmt"
    "github.com/sharedcode/sop/ai"
    "github.com/sharedcode/sop/ai/vector"
    "github.com/sharedcode/sop/ai/embed"
)

func main() {
    // 1. Initialize the Vector Database
    db := vector.NewDatabase()
    db.SetStoragePath("./my_knowledge_base")
    
    // Open an index for a specific domain (e.g., "documents")
    idx := db.Open("documents")

    // 2. Initialize an Embedder
    // (In production, use a real embedding model. Here we use the simple keyword hasher)
    emb := embed.NewSimple("simple-embedder", 64, nil)

    // 3. Add Data (Upsert)
    ctx := context.Background()
    item := ai.Item{
        ID: "doc-1",
        Meta: map[string]any{
            "text": "SOP is a high-performance Go library for storage.",
            "category": "tech",
        },
    }
    // Generate vector
    vecs, _ := emb.EmbedTexts([]string{item.Meta["text"].(string)})
    item.Vector = vecs[0]

    // Save to DB
    idx.UpsertBatch([]ai.Item{item})

    // 4. Search (Retrieve)
    query := "storage library"
    queryVecs, _ := emb.EmbedTexts([]string{query})
    
    hits, _ := idx.Query(queryVecs[0], 5, nil)
    
    for _, hit := range hits {
        fmt.Printf("Found: %s (Score: %.2f)\n", hit.Meta["text"], hit.Score)
    }
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
