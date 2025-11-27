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
	db := vector.NewDatabase()
	db.SetStoragePath("./data/doctor_brain")

	// 2. Open the "Doctor" index
	doctor := db.Open("doctor")

	// 3. Create some knowledge (In reality, you'd load this from PDFs/Textbooks)
	knowledge := []ai.Item{
		{
			ID:     "doc-101",
			Vector: []float32{0.1, 0.2, 0.9}, // Simplified vector
			Meta:   map[string]any{"text": "Appendicitis presents with pain in the lower right abdomen."},
		},
		{
			ID:     "doc-102",
			Vector: []float32{0.8, 0.1, 0.1},
			Meta:   map[string]any{"text": "Migraines are often accompanied by sensitivity to light."},
		},
	}

	// 4. Upsert (Transactional!)
	// SOP automatically handles the "Lookup" tree updates and Centroid assignment here.
	if err := doctor.UpsertBatch(knowledge); err != nil {
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
	doctor := db.Open("doctor")

	// 1. User Input
	userComplaint := "my tummy hurts"

	// 2. Nurse Action
	searchVector := NurseTranslate(userComplaint)

	// 3. Doctor Action (The Search)
	// SOP performs a partitioned search using the high-quality Centroids.
	results, _ := doctor.Query(searchVector, 1, nil)

	// 4. Diagnosis
	if len(results) > 0 {
		fmt.Printf("Doctor found match (Score: %.2f):\n", results[0].Score)
		fmt.Printf("Reference: %s\n", results[0].Meta["text"])
	} else {
		fmt.Println("Doctor: I need more information.")
	}
}

## Step 5: Long-Term Health (Rebalancing & Self-Healing)

A Vector Database is a living organism. As you add more knowledge, your initial clusters (Centroids) might become unbalanced—one topic might get too huge while others remain empty.

SOP includes a built-in **Rebalance** feature that uses its self-generated statistics to heal itself.

### The "Self-Aware" Index
Remember the `Centroid` struct? It tracks its own `VectorCount`.
*   **Real-time Stats**: Every time you add or delete a document, SOP updates the count on the affected Centroid.
*   **Smart Management**: The system knows exactly which clusters are "heavy" and which are "light" without expensive scans.

### The Rebalance Protocol
When your Doctor's knowledge grows significantly, you simply call:

```go
// Re-trains the index using the current data distribution
if err := doctor.Rebalance(); err != nil {
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

### 1. Usage Modes: Build-Once vs. Read-Write
*   **`BuildOnceQueryMany`**: Ideal for static knowledge bases (e.g., a Law Library). SOP optimizes the index for pure read speed and discards temporary build artifacts.
*   **`ReadWrite`**: Ideal for dynamic systems (e.g., User Logs). SOP maintains the auxiliary structures needed for continuous updates.

```go
db.SetUsageMode(vector.BuildOnceQueryMany) // or vector.ReadWrite
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
You can define your agent's personality, knowledge base, and policies in a file like `doctor.json`:

```json
{
  "id": "doctor",
  "name": "Dr. AI",
  "description": "I am a medical assistant with knowledge of 5000+ disease records.",
  "system_prompt": "You are a helpful medical assistant. Analyze the user's symptoms...",
  "storage_path": "doctor",
  "policies": [
    { "type": "profanity", "max_strikes": 3 }
  ],
  "embedder": {
    "type": "agent",
    "agent_id": "nurse_local",
    "instruction": "Find matching symptoms:"
  },
  "data": [
    { "id": "flu", "text": "Influenza", "description": "Symptoms: fever, chills..." }
  ]
}
```

### Running the Configurable Agent
SOP provides a standard runner that loads these configurations:

```bash
go run ai/cmd/agent/main.go -config ai/data/doctor.json
```

This command:
1.  Loads the JSON config.
2.  Initializes the Vector Store at the specified `storage_path`.
3.  Connects to the "Nurse" (Embedder) agent defined in the config.
4.  Starts the interactive chat loop.

### ETL: Automating Knowledge Ingestion
For real-world agents, you can't type thousands of records into the `data` array manually. This is where **ETL (Extract, Transform, Load)** comes in.

You can write simple Go programs to fetch data from the web (CSV, JSON, APIs), format it into the `agent.Config` structure, and save it.

**Example: The Medical Dataset Loader**
See `ai/etl/doctor.go` for a complete example that:
1.  **Extracts**: Downloads a raw CSV dataset of diseases and symptoms.
2.  **Transforms**: Cleans the text and formats it into `agent.DataItem` objects.
3.  **Loads**: Generates a `doctor.json` config file and uploads the processed vectors directly to the SOP IVF database.

This approach allows you to rebuild your expert's brain daily with fresh data, completely automatically.

## Step 8: The Developer's Toolkit (Go API)

The `sop/ai` module is a modular kit. You can use the high-level `agent` package, or pick and choose the components you need.

### 1. `ai/vector`: The Vector Database
If you just want a high-performance, local vector store without the agent logic, use the `vector` package directly.

```go
import "github.com/sharedcode/sop/ai/vector"

// Create a persistent store
store := vector.NewDatabase()
if err := store.Open("data/my_vectors"); err != nil {
    panic(err)
}
defer store.Close()

// Add a vector
err := store.Upsert("item1", []float32{0.1, 0.2, 0.3}, map[string]any{"label": "test"})

// Search
hits, err := store.Query([]float32{0.1, 0.2, 0.3}, 5, nil)
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
decision, err := finalPol.Evaluate("input", sample, labels)
if decision.Action == "block" {
    fmt.Println("Blocked by Policy:", decision.PolicyID)
}
```

### 3. `ai/embed`: Embeddings
The `embed` package provides a unified interface for turning text into vectors. It supports local heuristics and can wrap other agents.

```go
import "github.com/sharedcode/sop/ai/embed"

// A simple embedder (e.g., for testing or simple keyword matching)
embedder := embed.NewSimpleEmbedder()
vectors, _ := embedder.EmbedTexts([]string{"Hello world"})
```

### 4. `ai/etl`: Data Pipelines
The `etl` package helps you ingest data from various sources (CSV, Web, APIs) and prepare it for the Vector Store.

```go
import "github.com/sharedcode/sop/ai/etl"

// Example: Fetching and cleaning data
config, err := etl.PrepareDoctorDataset("https://example.com/data.csv")
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

*(Note: Advanced tutorials on clustered deployments and multi-agent orchestration will be covered in future documentation.)*

## Summary

By using SOP, you aren't just storing vectors; you are managing a **Transactional Knowledge Base**. 

1.  **Data Quality**: The `Lookup` B-Tree ensures your AI is trained on a mathematically perfect sample of your data.
2.  **Deduping**: The B-Tree backend prevents knowledge pollution.
3.  **Privacy**: Everything runs locally in `./data/doctor_brain`. No data leaves your machine.
4.  **AI/LLM**: Wrappers/hooks are available to "talk" (delegate) to Gemini/ChatGPT/Ollama/etc... AI/LLM. Allowing their reuse and your application freedom to combine/take each one(s) strengths and plugin their "intelligence" to your application.

Welcome to the future of Local AI.
