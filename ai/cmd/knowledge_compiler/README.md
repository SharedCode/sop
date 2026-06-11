# Knowledge Compiler

The Knowledge Compiler generates a structured Knowledge Base JSON file (`sop_base_knowledge.json`) from Markdown sources.

The preferred workflow is to author one curated Markdown source that follows the KB curation manifest and let the compiler translate that file into canonical `ExportData / ExportItem` JSON compatible with `ImportJSON`.

## How it Works

The compiler supports two modes conceptually:

1. **Curated mode (preferred)**
	* A curated Markdown file defines the semantic category hierarchy explicitly.
	* Explicit item blocks define the actual retrievable knowledge payload.
	* The compiler packages those items into canonical `ExportData / ExportItem` JSON.

2. **Legacy fallback mode**
	* If Markdown does not contain explicit item blocks, the compiler can still fall back to heading-and-body extraction from existing documentation.
	* This preserves backward compatibility with older repo-shaped knowledge sources.

## Curated Markdown contract

The preferred curated source uses:

* `#`, `##`, and optional `###` for the bounded semantic category hierarchy
* explicit item blocks for knowledge entries

Example:

```markdown
# AI & Knowledge Systems
## Embedders

- Item: Gemini embedding contract
  Summary: Gemini embedder supports gemini-embedding-2 via batchEmbedContents.
  Summary: Requests use taskType RETRIEVAL_DOCUMENT.
  Summary: Requests set outputDimensionality to 768.
  Body:
  The Gemini embedder uses the Google Generative Language batch endpoint and emits retrieval-oriented 768-dimensional embeddings.
  Sources: ai/embed/gemini2.go, ai/embed/gemini2_test.go
```

Compiler mapping:

* heading path -> `ExportItem.CategoryPath`
* repeated `Summary:` lines -> `ExportItem.Summaries`
* `Item:` / `Body:` / `Sources:` -> `ExportItem.Data`

Recommended payload fields inside `ExportItem.Data`:

* `title`
* `text`
* `description`
* `sources`

The `Summary:` lines are the vector-indexing payload. The `Body:` block is the full explanatory payload returned to the user after retrieval.

## Output model

The compiler emits canonical Knowledge Base import/export JSON compatible with:

* `memory.ExportData`
* `memory.ExportItem`
* `KnowledgeBase.ImportJSON(...)`

This keeps the compiler aligned with the existing import, preload, and vectorization flows instead of introducing a separate ad hoc schema.

## Usage

The simplest developer path is:

```bash
cd ai/cmd/knowledge_compiler
./run.sh
```

This uses the curated source `../../SOP_CURATED_KB.md` and writes the generated KB to `ai/sop_base_knowledge.json` in the repo root.

You can also run the compiler directly from the repo root:
```bash
go run ./ai/cmd/knowledge_compiler/main.go
```

To compile only one curated Markdown file instead of sweeping the whole repo:

```bash
go run ./ai/cmd/knowledge_compiler/main.go -input ai/SOP_CURATED_KB.md
```

For curation rules and the bounded semantic taxonomy, see [ai/KB_CURATION_MANIFEST.md](../../KB_CURATION_MANIFEST.md).

### Developer trial flow

1. Regenerate the KB JSON:
   ```bash
   cd ai/cmd/knowledge_compiler
   ./run.sh
   ```
2. Start the Data Manager from the repo root:
   ```bash
   go run ./tools/httpserver
   ```
3. Use the SOP KB preload path in the UI. The HTTP server automatically looks for the generated file in the built-in candidate locations `sop_base_knowledge.json`, `ai/sop_base_knowledge.json`, and `../ai/sop_base_knowledge.json`.

*Note: This specific README file is internally ignored by the compiler to prevent self-referential clutter in the generated Knowledge Base.*