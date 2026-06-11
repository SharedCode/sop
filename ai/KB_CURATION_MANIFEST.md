# SOP KB Curation Manifest

This file is the repeatable specification for generating the curated Markdown source that the Knowledge Compiler should use to build `ai/sop_base_knowledge.json`.

The goal is to replace raw repo-heading-driven KB generation with one curated Markdown file that:

* uses a stable, bounded semantic taxonomy
* groups knowledge under retrieval-friendly categories
* groups knowledge into explicit item blocks that can be packaged for import/export
* preserves source provenance for URL composition
* reduces duplication, noise, and marketing-style wording

## Repo root

Relative to this manifest file, the repo root is `..` (the parent folder of the `ai/` directory).

## Primary objective

Produce one curated Markdown file from the eligible repo Markdown corpus.

That curated file becomes the high-signal source that the KB compiler uses to build `ai/sop_base_knowledge.json`.

The curated file is not a raw concatenation of docs. It is a normalized semantic knowledge source optimized for category targeting and leaf-level retrieval.

The compiler should treat that curated Markdown as the authoring source for canonical `ExportData / ExportItem` JSON, so the resulting payload can flow directly through the existing `ImportJSON` path.

## Canonical output file

The canonical curated Markdown source file is:

* `ai/SOP_CURATED_KB.md`

Rules:

* This is the maintained curated KB source.
* Temporary drafting files may be created during editing (for example `SOP_CURATED_KB_DRAFT.md`), but they should not remain as the primary source once the curated pass is accepted.
* The compiler should be run against the canonical file unless explicitly testing a temporary draft.

## Regeneration workflow

When regenerating or replacing the curated KB in the future, follow this sequence:

1. Select the eligible Markdown corpus using the source-selection rules below.
2. Read and synthesize the source material into the bounded taxonomy defined in this manifest.
3. Write or update the canonical curated file at `ai/SOP_CURATED_KB.md`.
4. Ensure the curated file uses explicit item blocks with summaries, body, and provenance.
5. Run the Knowledge Compiler against the curated file only. From `ai/cmd/knowledge_compiler/`, use `./run.sh` (or `go run . -input ../../SOP_CURATED_KB.md ...`) so the compiler writes `ai/sop_base_knowledge.json` in the repo root.
6. Inspect the generated `ai/sop_base_knowledge.json` for expected category paths and item payloads.
7. If you want to trial the KB in the UI, start `go run ./tools/httpserver` from the repo root. The built-in SOP preload path automatically detects `sop_base_knowledge.json`, `ai/sop_base_knowledge.json`, and `../ai/sop_base_knowledge.json` when the SOP KB is preloaded.
8. If a temporary draft file was used, either delete it or leave it clearly marked as non-canonical.
## Custom KB workflow for end users

The same workflow can be used to author a custom Knowledge Base for a separate deployment or a private dataset:

1. Copy or adapt this manifest to reflect the domain-specific taxonomy and source rules you want to enforce.
2. Produce the curated Markdown source for your custom KB (for example, a private `SOP_CURATED_KB.md` file or a renamed variant in your own repo).
3. Run the compiler to generate the importable JSON file:
   ```bash
   cd ai/cmd/knowledge_compiler
   ./run.sh
   ```
   If you are using a custom curated input path, pass it to the compiler with the same `-input` option used in `run.sh`.
4. Open the SOP Data Manager UI and use the Import JSON button in the Space/KB toolbar to load the generated file into your installation.
5. Once imported, the custom KB can be queried and managed through the same HTTP Server and SOP Data Manager experience as the built-in SOP KB.

This keeps the authoring and compilation steps identical to the built-in flow, while letting end users ship a domain-specific KB as a normal JSON artifact.
## Source selection

Start from all Markdown files under the repo root (`..`), then exclude any file whose name matches the rules below.

## Workspace-specific source selection requirement

The inclusion and exclusion rules in this manifest are **not universal**. They are specific to the current repository layout and documentation set.

When using this workflow for a different repo, sub-repo, or a standalone folder of Markdown files:

* review the repo root or folder root being curated
* review the Markdown file list in that workspace
* update the inclusion and exclusion rules in this manifest to match that workspace
* update the canonical curated output path if the target workspace does not use `ai/`

This means the manifest itself must be treated as part of the curation input contract.

If the repo or folder structure changes materially, update this manifest before regenerating the curated KB.

### Practical rule

Before every new curation pass, explicitly answer these questions:

1. What is the repo root or folder root for this curation run?
2. Which Markdown files are eligible as source material in this workspace?
3. Which Markdown files are noise, marketing, release notes, meta-docs, or otherwise should be excluded?
4. Is the canonical curated output file path still correct for this workspace?

If any answer changed, update the manifest first.

### Excluded-file rules

Use these rules when deciding whether a Markdown file is eligible for the curated KB pass:

* file names containing `CODE_OF_CONDUCT`
* file names containing `LICENSE`
* file names containing `CHANGELOG`
* file names containing `POST`
* file names containing `ANNOUNCEMENT`
* file names containing `RELEASE`
* file names containing `PROPOSAL`
* file names containing `CONTRIBUTING`
* file names containing `LINKEDIN`
* file names containing `DEV_TO_POST`
* file names containing `AI_COPILOT`
* file names containing `SYSTEM_KNOWLEDGE`
* file names with the prefix `CLASSIFY_`
* file names containing `CURRENT_DESIGN_PLAN`
* this manifest file itself: `KB_CURATION_MANIFEST.md`
* any previously generated curated KB Markdown file

Clarification:

* `ai/SOP_CURATED_KB.md` is excluded from source selection when regenerating a new curated file from raw repo docs.
* It is included only as the compiler input after the curation pass is complete.
* For another repository or Markdown folder set, replace or adjust this exclusion list rather than assuming it is still correct.

## Output contract

The generated curated Markdown file must follow these rules:

1. Use the bounded semantic taxonomy defined below as the primary heading structure.
2. Do not mirror raw source document headings unless they clearly fit the bounded taxonomy.
3. Use headings only for semantic categories and subcategories.
4. Place actual knowledge in explicit item blocks.
5. Items may appear under leaf or non-leaf categories when that placement is semantically appropriate.
6. The compiler output should target the canonical `ExportData / ExportItem` JSON shape used by `ExportJSON` / `ImportJSON`.
7. Summaries are the vector-indexing payload; the body is the full explanatory item payload.
8. Preserve source provenance for every knowledge entry.
9. Prefer concise, high-signal synthesized entries over copied paragraphs.
10. Merge duplicate discussions into one normalized knowledge entry whenever possible.

## Required Markdown structure

Use this structure in the curated file:

* `#` = top-level semantic category
* `##` = bounded subcategory
* optional `###` = only when a deeper layer materially improves retrieval quality
* explicit item blocks beneath the most appropriate category heading

Category notes:

* Multiple `#` headings in one curated file are valid and should become multiple L1 categories.
* Non-leaf categories may still contain items when the concept belongs at that broader level.
* A plain sentence or short paragraph directly under a category heading is the category description.
* Do not prefix that paragraph with `Description:`; the meaning is implied by position.
* It is valid to leave one blank line between the category-description paragraph and the first `Item:` block.
* If a category heading has no paragraph beneath it, the heading text itself acts as the fallback category description.
* A slash inside a heading title, such as `Filesystem / Redis / Cassandra`, is literal title text and must not be split into nested sub-categories unless separate headings define that nesting.
* The heading hierarchy exists to improve retrieval targeting, not merely to mirror source document layout.

Example:

```markdown
## Architecture

This category covers backend topology, transaction flow, and registry-centered consistency.

- Item: Filesystem and hybrid backend model
   Summary: SOP supports a filesystem backend and a hybrid Cassandra-backed metadata backend.
   Body:
   SOP exposes two primary backend models.
   Sources: ARCHITECTURE.md, README.md
```

### Required entry format

Each item block should use this shape:

```markdown
- Item: short item title
   Summary: concise summary line used for vector indexing
   Summary: second summary line if needed
   Summary: third summary line if needed
   Summary: fourth summary line if needed
   Summary: fifth summary line if needed
   Body:
   Full discussion block that explains the concept in complete form.
   Source: relative/path/to/file.md
```

If a single item is synthesized from multiple source files, use:

```markdown
- Item: short item title
   Summary: concise summary line used for vector indexing
   Body:
   Full merged discussion block.
   Sources: path/one.md, path/two.md
```

Rules:

* `Item:` is the human-facing item label.
* Allow **1 to 5** `Summary:` lines.
* `Summary:` lines are the intended semantic index payload.
* `Body:` is the complete discussion payload for the item.
* `Source:` or `Sources:` is required.
* Do not paste large copied bodies from source docs when a synthesized body will work.
* Prefer complete, self-contained body text that can help the user understand the concept without reopening the source file.

### Compiler packaging target

The compiler should package each item block into canonical import/export JSON compatible with:

* `memory.ExportData`
* `memory.ExportItem`
* `KnowledgeBase.ImportJSON(...)`

Conceptually:

* heading path -> `ExportItem.CategoryPath`
* `Summary:` lines -> `ExportItem.Summaries`
* `Item:` / `Body:` / `Source:` fields -> `ExportItem.Data`

Recommended payload fields inside `ExportItem.Data`:

* `title`
* `text`
* `description`
* `sources`

Optional payload fields may be included later if needed, but the curated contract should remain minimal unless a real retrieval or UI requirement justifies expansion.

## Bounded semantic taxonomy

Use this taxonomy as the controlled hierarchy (examples) for the curated Markdown file:

1. Platform Foundations
   - Architecture
   - Core Concepts
   - Design Principles
   - Data Model & Abstractions
2. Installation & Setup
   - Prerequisites
   - Configuration
   - Environment Setup
   - Deployment Modes
3. Storage & Data Management
   - Stores vs Spaces
   - Transactions
   - Caching & Consistency
   - Backend Options
   - Filesystem / Redis / Cassandra
4. Operations & Reliability
   - Testing
   - Failover & Recovery
   - Performance
   - Monitoring & Debugging
   - Security & Access
5. AI & Knowledge Systems
   - Embedders
   - Vector Stores
   - Memory
   - Semantic Retrieval
   - Knowledge Base / KB Compiler
6. Language Bindings & Tooling
   - Go
   - Python
   - C#
   - Rust
   - Java
   - CLI / Examples
7. Developer Workflow
   - Build & Run
   - Scripts & Automation
   - Examples
   - Contributing / Maintenance

## Categorization guidance

* Place a source topic under the closest semantic leaf category, not under its original document title.
* If a source file spans multiple semantic topics, split its ideas across the closest relevant leaf categories.
* Items may also appear on non-leaf categories when the concept is broad and belongs naturally at that level.
* Only create a deeper nested category when it materially improves retrieval precision.
* Do not invent an unbounded taxonomy.
* Prefer stable categories that a user could naturally target in prompts.

## Deduplication guidance

* If multiple files explain the same idea, merge them into one curated entry.
* Keep the merged version factual and concise.
* Preserve all relevant sources on the merged entry.
* Prefer canonical docs when two sources conflict.
* If two sources differ only in wording or level of detail, keep one merged factual version rather than two near-duplicate items.

## Retrieval-oriented guidance

The curated category tree should help the retrieval system in two stages:

1. The query should land near the correct bounded category or subcategory.
2. The item summaries under that category should then be easy to match semantically.

This means:

* categories should be semantically coherent
* sibling categories should be meaningfully distinct
* item blocks should carry both index summaries and the full explanatory payload
* headings should define the semantic topology, not merely the document layout

## Writing style guidance

* Use factual, neutral wording.
* Remove marketing phrasing, article-style framing, and promotional tone.
* Prefer implementation facts, constraints, tradeoffs, and behavior over slogans.
* Avoid long narrative prose when a compact summary will work.

## Validation steps

After generating or updating `ai/SOP_CURATED_KB.md`, validate it with the compiler.

Preferred command:

```bash
cd /path/to/repo/ai/cmd/knowledge_compiler
go run . -input ../../SOP_CURATED_KB.md
```

Expected result:

* the compiler completes successfully
* `ai/sop_base_knowledge.json` is regenerated
* category paths reflect the curated heading hierarchy
* item payloads include summaries and full descriptions

Recommended spot checks:

* confirm the generated category count is consistent with the bounded taxonomy
* confirm item summaries are preserved as separate indexing lines
* confirm `sources` are carried into the output payload
* confirm the compiler did not unexpectedly collapse intended curated subcategories

## Maintenance notes

* The KB compiler still supports legacy repo-wide crawling as a fallback, but the preferred long-term path is curated Markdown input.
* New product or architecture work should be integrated into the curated KB by updating `ai/SOP_CURATED_KB.md`, not by relying on raw heading extraction.
* If the bounded taxonomy needs to evolve, update this manifest first before rewriting the curated file.

## Quick checklist for each curation pass

Before finishing the curated file, confirm:

* only eligible source Markdown files were used
* excluded/meta files were not included
* the bounded taxonomy was followed
* item blocks are concise and deduplicated
* every item has 1 to 5 `Summary:` lines
* every item has a `Body:` block
* every item has a `Source:` or `Sources:` line
* the compiler can map the file directly into canonical `ExportData / ExportItem` JSON
* the curated file reads like a semantic knowledge index, not a copy of the repo docs
* `ai/SOP_CURATED_KB.md` is the canonical curated source after the pass is complete

