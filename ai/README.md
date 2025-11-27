# AI Package

This package was developed with an AI copilot, I want to keep an open development approach(not finicky and narrow) in this package to keep efficient in "automaton" usage. Please note that this codebase will need to undergo refinements as time permits.

AI whisperer or "code whisperer" style. :)

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

# AI Agent Setup with Local LLM (Ollama)

This guide explains how to run the SOP AI Agent with a local LLM (Ollama) to enable the "Embedder Agent" pattern. This allows you to have a "Nurse" agent that translates user slang into clinical terms before searching the "Doctor" database, all running locally for free.

## Prerequisites

1.  **Install Ollama**: Download and install Ollama from [ollama.com](https://ollama.com).
2.  **Pull the Model**: Open your terminal and run:
    ```bash
    ollama pull llama3
    ```
    (You can also use `mistral`, `gemma`, etc., but the config defaults to `llama3`).
3.  **Go 1.24+**: Ensure you have Go installed.

## Configuration Pattern

The system uses two configuration files to achieve this:

1.  **`doctor.json` (The Main Agent)**:
    *   This is the agent the user talks to.
    *   It has an `embedder` section pointing to the "Nurse" agent.
    *   It has `data: []` because it relies on the pre-built SOP binary database.

    ```json
    {
      "id": "doctor",
      "name": "Dr. AI",
      "embedder": {
        "type": "agent",
        "agent_id": "nurse_translator",
        "instruction": "Translate the following patient description into these specific clinical terms..."
      },
      "storage_path": "doctor",
      "data": []
    }
    ```

2.  **`nurse_translator.json` (The Helper Agent)**:
    *   This agent is never called directly by the user.
    *   It uses the **Ollama Generator** to perform the translation.

    ```json
    {
      "id": "nurse_translator",
      "system_prompt": "You are a clinical terminology expert...",
      "generator": {
        "type": "ollama",
        "options": {
          "model": "llama3",
          "base_url": "http://localhost:11434"
        }
      }
    }
    ```

## Running the Agent

1.  **Ensure Ollama is running**:
    ```bash
    ollama serve
    ```
2.  **Run the Agent**:
    ```bash
    echo "I have the runs and my tummy hurts" | go run ai/cmd/agent/main.go -config ai/data/doctor.json
    ```

## The "Factory Reset" Kit

If you ever delete the `ai/data/doctor/` folder (the binary database), the agent will fail to start. To fix this, we provide a **Fallback Config** (`doctor_fallback.json`) that contains the raw source data.

To rebuild the database:
1.  Run the agent with the fallback config:
    ```bash
    go run ai/cmd/agent/main.go -config ai/data/doctor_fallback.json
    ```
    *This will detect the missing DB, read the 5,000 records from the JSON, and seed the B-Trees.*
2.  Once initialized, switch back to the production config:
    ```bash
    go run ai/cmd/agent/main.go -config ai/data/doctor.json
    ```

## Heuristic vs LLM Embedders

The system supports two types of "Nurse" agents for embedding/translation:

1.  **Heuristic Agent (`nurse_local`)**:
    *   **How it works**: Uses a local dictionary and vector search with manually curated synonyms.
    *   **Performance**: Extremely fast and deterministic.
    *   **Use Case**: We use this for the default "Doctor" demo. It has been tuned for high performance in specific areas, such as **lung-related diseases** (e.g., mapping "cough" to "Common Cold" or "Pneumonia") using specific heuristic mappings.
    *   **Pros**: No external dependencies (no Ollama required), predictable.
    *   **Cons**: Requires manual tuning for new slang/terms.

2.  **LLM Agent (`nurse_translator`)**:
    *   **How it works**: Uses a local LLM (via Ollama) to semantically understand and translate user input.
    *   **Performance**: Slower (depends on GPU/CPU), but more flexible.
    *   **Use Case**: General-purpose understanding without manual synonym mapping.
    *   **Pros**: Understands context and nuance better out-of-the-box.
    *   **Cons**: Requires running Ollama, higher latency.

To switch between them, update the `embedder.agent_id` in `doctor.json`.
