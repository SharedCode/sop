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

## Going Further: Hosting Other Experts
```

## Going Further: Hosting Other Experts

The beauty of this system is that the **Architecture is Agnostic**. You can swap the data to build:

*   **The Mechanic**: Feed it car manuals. Nurse translates "clunking sound" to "suspension strut failure".
*   **The Lawyer**: Feed it case law. Nurse translates "my boss fired me" to "wrongful termination precedents".
*   **The Chef**: Feed it recipes. Nurse translates "what do I do with these leftovers" to "ingredients list".

## Summary

By using SOP, you aren't just storing vectors; you are managing a **Transactional Knowledge Base**. 

1.  **Data Quality**: The `Lookup` B-Tree ensures your AI is trained on a mathematically perfect sample of your data.
2.  **Deduping**: The B-Tree backend prevents knowledge pollution.
3.  **Privacy**: Everything runs locally in `./data/doctor_brain`. No data leaves your machine.

Welcome to the future of Local AI.
