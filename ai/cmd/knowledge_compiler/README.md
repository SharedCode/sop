# Knowledge Compiler

The Knowledge Compiler is a powerful utility designed to generate a highly structured Knowledge Base JSON file (`sop_base_knowledge.json`) directly from a repository's Markdown files. 

We highly recommend end-users utilize this compiler to automatically generate Knowledge Base JSON files from their own repository's Markdown files! It allows you to build customized, semantically-aware spaces for your specific projects.

## How it Works

The compiler uses an AST-based Markdown crawler to naturally build a Directed Acyclic Graph (DAG) for categories based on the structure of your documentation:

1. **Hierarchy via Links (The DAG)**: The crawler starts at the repository's root `README.md`. If a section like `## Architecture` links to `[Frontend UI](ui.md)`, the base category for the linked file naturally becomes `Architecture / Frontend UI`. Headers inside the linked file extend this taxonomy (e.g., `Architecture / Frontend UI / State Management`).
2. **Multiple Links**: If a file is linked from multiple locations throughout your documentation, its contents are not duplicated. Instead, the root category for that document receives multiple parent relationships, forming a clean Directed Acyclic Graph.
3. **Unlinked Markdown Files**: After the crawler completes the tree from root reads, it sweeps the repository for any unvisited `.md` files. It dynamically extracts their primary header (`H1`) or filename to act as their Level 1 Category, ensuring no knowledge is missed while keeping the taxonomy clean.

## Usage

Simply run the compiler from the root of your repository:
```bash
go run ./ai/cmd/knowledge_compiler/main.go
```

*Note: This specific README file is internally ignored by the compiler to prevent self-referential clutter in the generated Knowledge Base.*