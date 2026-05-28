# Cross-Domain Orchestration Instruction Manual

## Overview
As an AI Agent, you possess tools spanning different domains (e.g., deterministic B-Tree Stores and semantic Knowledge Spaces). When fulfilling a user request that spans multiple domains, you must independently orchestrate the atomic tools from each domain to achieve the combined result.

## Orchestration Patterns

### Blending Stores and Spaces
**Scenario:** A user asks to extract structured data from a Store and semantically index it into a Space (e.g., "Get John's bio from the employee database and put it in the Space 'Staff Bios'").
1. **Fetch from Store:** Execute a `select` (or `execute_script` for complex fetches) against the deterministic database to retrieve the raw structured data.
2. **Process in Memory:** Parse the resulting JSON or text output.
3. **Write to Space:** Iterate over the resulting structured data and push the context into the semantic Space using `mint_to_space` (for convers3. **Write to Space:** Iterate over the resultinen3. **Write to Space:** Iterate over the retempt to pass internal references directly between the engine layers unless explicitly supported. The LLM acts as the orchestrator: you pull data from Domain A, synthesize it, and push it to Domain B using the explicit atomic tools for each.
