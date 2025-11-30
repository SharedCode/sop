# Build Your Own AI Expert System: A Guide to SOP's "Doctor & Nurse" Pattern

Welcome to the "Green Field" of AI development. 

In this tutorial, we will build a privacy-first, local AI Expert System using the **SOP (Scalable Objects Persistence)** library. We will implement the **"Doctor & Nurse"** pattern—a dual-agent architecture that runs entirely on your machine (or cluster) with zero API fees.

## The Concept: Why Two Agents?

Most RAG (Retrieval-Augmented Generation) systems fail because users speak casually ("my tummy hurts"), but the database contains technical facts ("abdominal pain causes").

We solve this with two specialized roles:

1.  **The Nurse (Local Embedder/LLM)**: 
    *   **Role**: The "Front Desk". It listens to the user, sanitizes input, and translates vague complaints into precise domain terminology.
    *   **Tech**: A small, fast local LLM (like Llama 3 via Ollama).
2.  **The Doctor (SOP Vector Store)**: 
    *   **Role**: The "Specialist". It holds the massive knowledge base (textbooks, manuals, logs) and performs the deep search.
    *   **Tech**: SOP Vector Database (Transactional B-Trees).

---

## The SOP Advantage: Why Not Just Use a Generic Vector DB?

Before we code, it is crucial to understand why SOP produces **higher quality results** than standard vector stores.

### 1. The "Perfect" Index (The Lookup Tree)
Most vector databases train their index (K-Means clustering) by grabbing the first 1,000 items they find. If your data is sorted by date, your index only knows about "January" and fails to categorize "December" data correctly.

SOP implements a **Transactional Lookup B-Tree**. 
*   It maintains a dense integer map (`0, 1, 2... N`) to every item ID in the system.
*   **Result**: When SOP trains its index, it performs **Ideal Random Sampling** across the *entire* dataset (TB+). 
*   **Benefit**: Your search clusters (Centroids) are mathematically representative of your total knowledge, not just the data you loaded first.

### 2. Scalable Deduping
"Garbage In, Garbage Out." Duplicate vectors skew search results and confuse LLMs.
*   SOP is built on **B-Trees**, which enforce key uniqueness at the storage engine level.
*   **Benefit**: You can enforce strict uniqueness on Document IDs or Content Hashes. If you try to load the same medical manual twice, SOP handles the deduplication transactionally. Your "Doctor" never gets confused by duplicate conflicting info.

---

## Step 1: The Setup

You need Go (1.24+) and the SOP library.

```bash
go get github.com/sharedcode/sop
```

(Optional) For the "Nurse" LLM, install [Ollama](https://ollama.com/) and pull a model:
```bash
ollama pull llama3
```

## Step 2: The Data (ETL)

First, we need to teach our Doctor. We will ingest "medical knowledge" (text chunks). SOP handles the heavy lifting of chunking and indexing.

```go
package main

import (
	"fmt"
	"github.com/sharedcode/sop/ai"
	"github.com/sharedcode/sop/ai/vector"
)

func main() {
	// 1. Initialize the Database (No Redis required for standalone!)
	db := vector.NewDatabase[map[string]any](ai.Standalone)
	db.SetStoragePath("./data/doctor_brain")

	// 2. Open the "Doctor" index
	doctor := db.Open(context.Background(), "doctor")

	// 3. Create some knowledge (In reality, you'd load this from PDFs/Textbooks)
	knowledge := []ai.Item[map[string]any]{
		{
			ID:     "doc-101",
			Vector: []float32{0.1, 0.2, 0.9}, // Simplified vector
			Payload:   map[string]any{"text": "Appendicitis presents with pain in the lower right abdomen."},
		},
		{
			ID:     "doc-102",
			Vector: []float32{0.8, 0.1, 0.1},
			Payload:   map[string]any{"text": "Migraines are often accompanied by sensitivity to light."},
		},
	}

	// 4. Upsert (Transactional!)
	// SOP automatically handles the "Lookup" tree updates and Centroid assignment here.
	if err := doctor.UpsertBatch(context.Background(), knowledge); err != nil {
		panic(err)
	}

	fmt.Println("The Doctor has studied the material.")
}
```

## Step 3: The Nurse (The Translator)

The Nurse intercepts the user's raw query.

```go
func NurseTranslate(userQuery string) []float32 {
	// In a real app, you would call Ollama here:
	// prompt := "Translate this symptom to medical terms: " + userQuery
	
	fmt.Printf("Nurse: User complained of '%s'. Translating to medical terminology...\n", userQuery)
	
	// Mocking the embedding generation for this tutorial
	// "My tummy hurts" -> "Abdominal pain" -> [0.1, 0.2, 0.9]
	if userQuery == "my tummy hurts" {
		return []float32{0.1, 0.2, 0.9} 
	}
	return []float32{0.0, 0.0, 0.0}
}
```

## Step 4: The Consultation (Search)

Now we put it together. The user speaks to the Nurse, the Nurse speaks to the Doctor.

```go
func main() {
	// ... (Open DB as before) ...
	doctor := db.Open(context.Background(), "doctor")

	// 1. User Input
	userComplaint := "my tummy hurts"

	// 2. Nurse Action
	searchVector := NurseTranslate(userComplaint)

	// 3. Doctor Action (The Search)
	// SOP performs a partitioned search using the high-quality Centroids.
	results, _ := doctor.Query(context.Background(), searchVector, 1, nil)

	// 4. Diagnosis
	if len(results) > 0 {
		fmt.Printf("Doctor found match (Score: %.2f):\n", results[0].Score)
		fmt.Printf("Reference: %s\n", results[0].Payload["text"])
	} else {
		fmt.Println("Doctor: I need more information.")
	}
}

## Step 5: Long-Term Health (Rebalancing & Self-Healing)

A Vector Database is a living organism. As you add more knowledge, your initial clusters (Centroids) might become unbalanced—one topic might get too huge while others remain empty.

SOP includes a built-in **Optimize** feature that uses its self-generated statistics to heal itself.

### The "Self-Aware" Index
Remember the `Centroid` struct? It tracks its own `VectorCount`.
*   **Real-time Stats**: Every time you add or delete a document, SOP updates the count on the affected Centroid.
*   **Smart Management**: The system knows exactly which clusters are "heavy" and which are "light" without expensive scans.

### The Optimize Protocol
When your Doctor's knowledge grows significantly, you simply call:

```go
// Re-trains the index using the current data distribution
if err := doctor.Optimize(context.Background()); err != nil {
    panic(err)
}
```

This triggers a background process that:
1.  Uses the **Lookup Tree** to pull a perfect random sample of the *current* data.
2.  Calculates new, optimal Centroids.
3.  Re-assigns vectors to these new homes.
4.  Swaps the index atomically (Zero downtime for searches!).

This ensures your Expert System gets *smarter* and *faster* as it grows, rather than degrading like traditional vector stores.

## Step 6: Performance Tuning (Modes & Caching)

SOP gives you two powerful knobs to tune your Expert System for its specific role.

### 1. Usage Modes: Build-Once vs. Dynamic
*   **`BuildOnceQueryMany`**: Ideal for static knowledge bases (e.g., a Law Library). SOP optimizes the index for pure read speed and discards temporary build artifacts.
*   **`Dynamic`**: Ideal for dynamic systems (e.g., User Logs). SOP maintains the auxiliary structures needed for continuous updates.
*   **`Static`**: Optimized for read-only or append-only datasets.

```go
db.SetUsageMode(vector.BuildOnceQueryMany) // or vector.Dynamic
```

### 2. The "NoCheck" Speed Mode
For the "Doctor" agent serving queries, you want raw speed. SOP supports a **`NoCheck`** transaction mode.
*   **What it does**: It bypasses the heavy transactional isolation checks (conflict detection) during reads.
*   **Result**: Reads become as fast as accessing a raw flat file, but with the structure of a B-Tree.
*   **Bonus**: SOP includes a built-in **MRU (Most Recently Used) Cache**. Frequently accessed medical facts stay in RAM, making subsequent queries instant.

```go
// Configure the Doctor for maximum read speed
db.SetReadMode(sop.NoCheck)
```

All of these enterprise-grade features—Transactional Integrity, Self-Healing Indexes, and In-Memory Caching—come **for free** just by using the SOP library.

## Step 7: Zero-Code Agents (JSON Config & ETL)

You don't always need to write Go code to build an agent. SOP includes a powerful configuration system that lets you define agents using simple JSON files. It is a prebuilt expert system, just needing your content! And has ability to delegate to LLM (Gemini, ChatGPT) or local heuristics right out of the box.

### The Agent Configuration
You can define your agent's personality, knowledge base, and policies in a file like `doctor_pipeline.json`:

```json
{
  "id": "doctor_pipeline",
  "name": "Dr. AI Pipeline",
  "description": "Orchestrates the interaction between Nurse and Doctor.",
  "agents": [
    {
      "id": "nurse_local",
      "name": "Nurse Joy",
      "description": "Translates symptoms to medical terms.",
      "system_prompt": "You are a nurse. Translate user symptoms to medical terminology.",
      "storage_path": "nurse_local",
      "embedder": { "type": "simple" },
      "data": [
        { "id": "1", "text": "tummy hurt", "description": "abdominal pain" },
        { "id": "2", "text": "hot", "description": "fever" }
      ]
    },
    {
      "id": "doctor_core",
      "name": "Dr. House",
      "description": "Medical specialist.",
      "system_prompt": "Analyze the medical terms and provide a diagnosis.",
      "storage_path": "doctor_core",
      "embedder": {
        "type": "agent",
        "agent_id": "nurse_local",
        "instruction": "Find matching symptoms:"
      }
    }
  ],
  "pipeline": [
    {
      "agent": "nurse_local",
      "output_to": "context"
    },
    {
      "agent": "doctor_core"
    }
  ]
}
```

### Running the Configurable Agent
SOP provides a standard runner that loads these configurations:

```bash
go run ai/cmd/agent/main.go -config ai/data/doctor_pipeline.json
```

This command:
1.  Loads the JSON config.
2.  Initializes the pipeline and all referenced agents.
3.  Starts the interactive chat loop.

### ETL: Automating Knowledge Ingestion (Production Mode)
For real-world agents, you can't type thousands of records into the `data` array manually. This is where **ETL (Extract, Transform, Load)** comes in.

We provide a dedicated ETL tool (`sop-etl`) to ingest massive datasets into the Vector Store efficiently.

**1. Define the Workflow**
Create a workflow file (e.g., `etl_workflow.json`) that defines the pipeline steps:

```json
{
  "steps": [
    {
      "name": "Prepare Data",
      "action": "prepare",
      "params": {
        "url": "https://example.com/data.csv",
        "output": "data/doctor_data.json"
      }
    },
    {
      "name": "Build Nurse DB",
      "action": "ingest",
      "params": {
        "agent_config": "data/nurse_local.json",
        "source_data": "data/doctor_data.json"
      }
    },
    {
      "name": "Build Doctor DB",
      "action": "ingest",
      "params": {
        "agent_config": "data/doctor_core.json",
        "source_data": "data/doctor_data.json"
      }
    }
  ]
}
```

**2. Run the ETL Tool**
Run the tool with the workflow flag:

```bash
./sop-etl -workflow etl_workflow.json
```

This will sequentially download the data, process it, and populate the vector databases for both agents.

**3. Run the Agent**
Now you can run your agent using the pre-populated database:
```bash
./sop-ai -config data/doctor_pipeline.json
```

## Step 8: Running the Full Example

We have included a complete, working example in the repository. You can build the tools, ingest the data, and run the "Doctor & Nurse" agents with a single script.

### 1. Run the Rebuild Script
The `rebuild_doctor.sh` script performs the following:
1.  **Builds** the `sop-etl` and `sop-ai` binaries.
2.  **Cleans** up old data.
3.  **Runs** the ETL workflow defined in `etl_workflow.json`.
4.  **Runs** sanity tests.

```bash
cd ai
./rebuild_doctor.sh
```

### 2. Chat with the Doctor
Once the rebuild is complete, you can start the interactive agent loop:

```bash
./sop-ai -config data/doctor_pipeline.json
```

**The script runs these sanity checks:**
*   "I have a tummy hurt and feel hot" (Verifies "nurse" translation of "tummy hurt" -> "abdominal pain" and "hot" -> "fever")
*   "I have a bad cough and a runny nose" (Verifies "doctor" detection of Common Cold symptoms)

## Step 9: The Developer's Toolkit (Go API)

The `sop/ai` module is a modular kit. You can use the high-level `agent` package, or pick and choose the components you need.

### 1. `ai/vector`: The Vector Database
If you just want a high-performance, local vector store without the agent logic, use the `vector` package directly.

```go
import "github.com/sharedcode/sop/ai/vector"

// Create a persistent store
store := vector.NewDatabase[map[string]any](ai.Standalone)
if err := store.Open(context.Background(), "data/my_vectors"); err != nil {
    panic(err)
}
defer store.Close()

// Add a vector
err := store.Upsert(context.Background(), ai.Item[map[string]any]{
    ID: "item1", 
    Vector: []float32{0.1, 0.2, 0.3}, 
    Payload: map[string]any{"label": "test"},
})

// Search
hits, err := store.Query(context.Background(), []float32{0.1, 0.2, 0.3}, 5, nil)
```

### 2. `ai/policy`: Safety & Guardrails
SOP includes a flexible policy engine designed to build **Responsible, Secured, and Safe AI systems**.

The kit supports a hierarchical policy model:
*   **Global Policies**: Enforced across all agents (e.g., "No Hate Speech", "GDPR Compliance").
*   **Local Policies**: Specific to a single agent (e.g., "Medical Disclaimer" for the Doctor agent).

This allows software teams to easily author and manage governance at the appropriate level.

```go
import "github.com/sharedcode/sop/ai/policy"

// 1. Define a Global Policy (e.g., Corporate Safety Standards)
globalPol, _ := policy.NewProfanityGuardrail(3)

// 2. Define a Local Policy (e.g., Custom Business Logic)
// You can implement the ai.PolicyEngine interface for custom rules
localPol := &MyCustomPolicy{AllowedTopics: []string{"medical"}}

// 3. Chain them together for enforcement
// The chain evaluates policies in order; if any policy blocks, the action is blocked.
finalPol := policy.NewChain(globalPol, localPol)

// Evaluate content
decision, err := finalPol.Evaluate(context.Background(), "input", sample, labels)
if decision.Action == "block" {
    fmt.Println("Blocked by Policy:", decision.PolicyID)
}
```

### 3. `ai/embed`: Embeddings
The `embed` package provides a unified interface for turning text into vectors. It supports local heuristics and can wrap other agents.

```go
import "github.com/sharedcode/sop/ai/embed"

// A simple embedder (e.g., for testing or simple keyword matching)
embedder := embed.NewSimple("simple-embedder", 64, nil)
vectors, _ := embedder.EmbedTexts(context.Background(), []string{"Hello world"})
```

### 4. `ai/etl`: Data Pipelines
The `etl` package helps you ingest data from various sources (CSV, Web, APIs) and prepare it for the Vector Store.

```go
import "github.com/sharedcode/sop/ai/etl"

// Example: Fetching and cleaning data
err := etl.PrepareData("https://example.com/data.csv", "output.json", 1000)
```

### Putting It All Together: A Custom Agent
You can mix these packages to build something unique. For example, a "Safe Search" agent:

```go
package main

import (
    "context"
    "github.com/sharedcode/sop/ai/agent"
    "github.com/sharedcode/sop/ai/generator"
    "github.com/sharedcode/sop/ai/policy"
)

func main() {
    // 1. Load Domain (Vector Store + Embedder)
    domain := myCustomDomainLoader() 

    // 2. Add Safety
    pol, cls := policy.NewProfanityGuardrail(1)
    domain.SetPolicy(pol)
    domain.SetClassifier(cls)

    // 3. Connect Brain
    brain, _ := generator.NewGeminiClient("KEY", "gemini-pro")

    // 4. Launch
    svc := agent.NewService(domain, brain)
    svc.RunLoop(context.Background(), os.Stdin, os.Stdout)
}
```

## Going Further: Hosting Other Experts
```

## The Vision: Building "Smart Systems" of Any Scale

The SOP AI Kit is designed to address the entire chain of building intelligent software, from simple automation to enterprise-class AI.

### Lightweight "Automatons"
Developers can build custom agents that act as very lightweight, super high-performance **modules** or **automatons**.
*   **Reuse & Extend**: Start with prebuilt agents and layer new logic on top.
*   **Hybrid Intelligence**: Seamlessly combine **Local Heuristics** (for speed and determinism) with **LLMs** like Gemini or ChatGPT (for reasoning and creativity).
*   **Full Spectrum**: Whether you are building a smarter RESTful API or a complex expert system, the kit provides the foundational blocks.

### Enterprise-Class Architecture
By leveraging SOP's core **Clustered Database** features, software teams can build systems that are not just smart, but robust and scalable.
*   **Collaborative AI**: The kit treats Gemini, ChatGPT, and Local Agents as interoperable components. They can reuse each other's capabilities to solve problems that no single model could handle alone.
*   **Transactional Integrity**: Unlike simple vector libraries, SOP ensures your AI's memory is ACID-compliant, making it suitable for critical enterprise applications.

## Step 10: Going Enterprise (Clustered Mode)

While `ai.Standalone` is perfect for local development and single-node deployments, SOP AI also supports a **Clustered** mode for high availability and scale.

### Switching to Clustered Mode
To enable clustered mode, simply change the database type and ensure you have a Redis instance running (for the L2 Cache).

```go
// 1. Initialize the Database in Clustered Mode
// This will automatically connect to a local Redis instance (localhost:6379) for caching.
db := vector.NewDatabase[map[string]any](ai.Clustered)
db.SetStoragePath("./data/doctor_brain_cluster")

// 2. Open the "Doctor" index
doctor := db.Open(context.Background(), "doctor")
```

In Clustered mode:
*   **L2 Cache**: SOP uses Redis to cache B-Tree nodes, enabling high-performance shared access across multiple application instances.
*   **Storage**: Data is still persisted to the filesystem (or a shared volume), but the Redis cache ensures consistency and speed in a distributed environment.

### Seamless Migration (SDLC)
SOP is designed to support your Software Development Life Cycle (SDLC) from local dev to production.

1.  **Develop Locally**: Build and test your agent on your laptop using `ai.Standalone`.
2.  **Deploy to Prod**: When moving to a higher environment (QA/Prod), simply:
    *   Copy your data folder (e.g., `./data/doctor_brain`) to the target server (or shared volume).
    *   Flip the switch in your code (or config) to `ai.Clustered`.
    *   Ensure Redis is running.

**Note**: For the "easy flip" to work in a multi-node cluster, the storage path must be a **shared volume** (e.g., NFS, EFS, or a mounted SAN) accessible to all nodes. If running on a single node (just for caching benefits), a local path is fine.

The next time your application runs, it will automatically pick up the existing data and start using the Redis cache for coordination. No data migration or export/import is required.

## Step 11: Advanced - Atomic Updates (Transactional Integrity)

One of SOP's unique superpowers is the ability to update **multiple** stores (e.g., the Vector DB and the Model Registry) in a single, atomic transaction.

If your training process crashes halfway through, you don't want a "ghost" state where the vector index is updated but the model weights aren't.

```go
func AtomicTrainAndIndex(ctx context.Context, doc ai.Item[any], newWeights []float64) error {
    // 1. Start a Transaction
    // This transaction will span across both the Vector Store and the Model Store.
    trans, _ := inredfs.NewTransaction(ctx, options)
    if err := trans.Begin(ctx); err != nil {
        return err
    }

    // 2. Open Transactional Views
    // Bind the stores to this specific transaction.
    vecStore := myVectorDB.Open("documents").WithTransaction(trans)
    modelStore, _ := ai.NewBTreeModelStore(ctx, trans)

    // 3. Perform Updates
    // A. Update the Vector Index
    if err := vecStore.Upsert(ctx, doc); err != nil {
        trans.Rollback(ctx)
        return err
    }

    // B. Update the Model Weights
    if err := modelStore.Save(ctx, "classifiers", "sentiment_v2", newWeights); err != nil {
        trans.Rollback(ctx)
        return err
    }

    // 4. Commit
    // Both updates are applied instantly and atomically.
    // If this fails (e.g., power loss), NOTHING is saved.
    return trans.Commit(ctx)
}
```

This pattern is essential for building robust, enterprise-grade AI systems that can recover from failures without data corruption.

## Step 12: Unified Architecture (The Bridge)

SOP's architecture is "Layered". The AI package is a specialized layer built on top of the General Purpose engine. This means you can mix low-level Key-Value operations with high-level Vector operations in the **same atomic transaction**.

This is powerful for scenarios like "User Registration", where you need to create a User Profile (KV) and index their Bio (Vector) simultaneously.

```go
func RegisterUser(ctx context.Context, userID string, bio string) error {
    // 1. Start a General Purpose Transaction
    // This gives us raw access to the storage engine.
    trans, _ := inredfs.NewTransaction(ctx, options)
    if err := trans.Begin(ctx); err != nil {
        return err
    }

    // 2. General Purpose Work (Key-Value Store)
    // Open a raw B-Tree to store user profiles.
    userStore, _ := inredfs.NewBtree[string, UserProfile](ctx, sop.ConfigureStore("users", ...), trans, ...)
    
    profile := UserProfile{ID: userID, Bio: bio, CreatedAt: time.Now()}
    if _, err := userStore.Add(ctx, userID, profile); err != nil {
        trans.Rollback(ctx)
        return err
    }

    // 3. AI Work (Vector Store)
    // "Bind" the AI Vector Store to the SAME transaction.
    // Now, the vector upsert participates in 'trans'.
    vecStore := myVectorDB.Open("user_bios").WithTransaction(trans)
    
    // Generate embedding (mocked)
    vector := embedder.Embed(bio)
    
    item := ai.Item[any]{ID: userID, Vector: vector, Payload: nil}
    if err := vecStore.Upsert(ctx, item); err != nil {
        trans.Rollback(ctx)
        return err
    }

    // 4. Commit
    // Both the User Profile and the Vector Index are saved atomically.
    return trans.Commit(ctx)
}
```

This unification allows you to build complex, data-intensive applications without needing separate databases for your structured data (SQL/KV) and your AI data (Vectors).

## Summary

By using SOP, you aren't just storing vectors; you are managing a **Transactional Knowledge Base**. 

1.  **Data Quality**: The `Lookup` B-Tree ensures your AI is trained on a mathematically perfect sample of your data.
2.  **Deduping**: The B-Tree backend prevents knowledge pollution.
3.  **Privacy**: Everything runs locally in `./data/doctor_brain`. No data leaves your machine.
4.  **AI/LLM**: Wrappers/hooks are available to "talk" (delegate) to Gemini/ChatGPT/Ollama/etc... AI/LLM. Allowing their reuse and your application freedom to combine/take each one(s) strengths and plugin their "intelligence" to your application.

Welcome to the future of Local AI.
