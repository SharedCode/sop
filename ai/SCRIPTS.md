# AI Assistant Scripts: Natural Language Programming

The SOP AI Assistant includes a powerful **Script System** that transforms your AI chat sessions into a "Natural Language Programming" environment. It allows you to record, script, and execute complex workflows that combine the flexibility of LLMs with the performance of compiled code.

## Concept: "Compiled Instructions"

Unlike traditional chat history, SOP Scripts are **deterministic programs**.
*   **Recording**: When you record a session, the system captures the **logic** of your interaction.
*   **Scripting**: You can manually edit or generate scripts using a standardized JSON schema, adding loops, conditions, and variables.
*   **Execution**: The SOP engine executes these steps directly using compiled Go code. This is akin to running a compiled binary or a high-performance stored procedure, not just replaying a chat log.

## The SOP Advantage: Bare Metal Performance

SOP exposes its B-Tree engine directly to the script system. This effectively creates a **machine instruction set** for end-users.

*   **Bare Metal API**: SOP's B-Tree API is compiled Go code.
*   **High-Performance Units**: Recorded scripts are sequences of JSON instructions that command this compiled code to perform iterations, expression evaluations, and data lookups.
*   **No Parsing Overhead**: Unlike SQL statements that require complex parsing and query planning at runtime, SOP scripts are pre-structured units of work.
*   **System Database**: Scripts are persisted in a dedicated **SystemDB**, a robust SOP B-Tree store. This ensures your "programs" are durable, transactional, and available across server restarts.

## Atomic Lego Blocks: The Foundation of Safe Scripting

To enable the LLM to generate scripts with "fine-grained agility" and control, we have exposed a set of **Atomic Lego Blocks**. These are highly optimized, safe, and versatile functions that the LLM can assemble to perform complex logic without writing raw code.

*   **`compare(val1, val2)`**: A universal comparator that intelligently handles strings, numbers, and dates. It returns `-1`, `0`, or `1`, making it perfect for sorting and range checks.
*   **`matchesMap(item, criteria)`**: A powerful pattern matcher inspired by MongoDB query syntax. It supports operators like:
    *   `$gt`, `$gte`, `$lt`, `$lte`: Range comparisons.
    *   `$in`: Set membership.
    *   `$eq`, `$ne`: Equality checks.
    *   `$regex`: Pattern matching.
*   **`toFloat(val)`**: A robust type converter that safely extracts numerical values from various input types (strings, ints, floats) for mathematical operations.
*   **`Scan(store, options)`**: The fundamental B-Tree traversal block. It supports forward/backward iteration, prefix matching, and range queries, serving as the engine for `SELECT * FROM ... WHERE ...` style operations.
*   **`JoinRightCursor(left_store, right_store, join_key)`**: A specialized cursor that efficiently performs a Right Outer Join between two B-Trees. It iterates through the 'Right' store and performs optimized lookups in the 'Left' store, handling missing matches gracefully.

**Why this matters:**
Instead of asking the LLM to "write a Python script to filter users," we ask it to "generate a JSON structure using `matchesMap`." This ensures:
1.  **Safety**: No arbitrary code execution.
2.  **Consistency**: The logic behaves exactly the same way every time, powered by our compiled Go implementation.
3.  **Agility**: The LLM can easily tweak the JSON parameters (e.g., changing a threshold from `> 10` to `> 20`) without rewriting logic.

## Streaming & Minimal Memory Footprint

A critical advantage of using `Scan` and `JoinRightCursor` is **Streaming**.

*   **Zero-Buffer Execution**: When performing a Join or a large Scan, the system does *not* load all results into memory. Instead, it streams items one by one through the pipeline.
*   **Huge Volume Support**: You can perform complex SQL-style Joins on B-Trees containing millions of records with **minimal memory requirements**. The memory footprint remains constant regardless of dataset size.
*   **Remote Agility**: These streams are piped directly to the HTTP response, allowing a REST client on the other side of the world (or the UI) to start receiving data immediately, byte-by-byte.

## Natural Language Programming (NLP) System

We are building a system where the **LLM acts as the compiler**.
1.  **User Intent**: You describe a complex task in natural language (e.g., "For every user in the 'users' table who hasn't logged in for 30 days, send a reminder email").
2.  **LLM Compilation**: The AI translates this intent into a structured SOP Script (JSON) containing loops (`loop`), conditions (`if`), and tool calls (`ask`, `fetch`).
3.  **SOP Execution**: The SOP engine executes this script. It fetches data from B-Trees efficiently (`fetch`), iterates in compiled code (`loop`), and only calls the LLM when semantic understanding is needed (`ask`).

This hybrid approach gives you the best of both worlds:
*   **Ease of Use**: "Programming" via chat.
*   **Performance**: Heavy lifting (loops, data access) is done by compiled Go code, not the LLM.

## Script Schema (The "Language")

The script system uses a stable JSON schema acting as a mini-SDK:

*   **`ask`**: Query the LLM for reasoning or creative generation.
*   **`set`**: Assign values to variables.
*   **`if`**: Conditional branching logic.
*   **`loop`**: Iterate over lists or data fetched from B-Trees.
*   **`fetch`**: High-performance data retrieval directly from SOP B-Trees.
*   **`say`**: Output information to the user.

## Commands

### Recording & Playback
*   `/record <name>`: Start recording a session as a new script.
*   `/stop`: Stop recording and save to SystemDB.
*   `/play <name> [param=value ...]`: Execute a saved script.
    *   **Parameters**: Pass arguments like `user_id=123`.
    *   **Templating**: Use `{{.user_id}}` in your script to inject values.

### Management
*   `/list`: List all available scripts in the SystemDB.

## Remote Execution via REST API

Because scripts are stored in the persistent SystemDB, they are instantly available as **API Endpoints**. You can trigger any complex workflow remotely:

```bash
curl -X POST http://localhost:8080/api/ai/chat \
  -d '{"message": "/play daily_report date=2025-01-01", "agent": "sql_admin"}'
```

This turns your SOP server into a programmable application server where business logic can be defined via chat and executed via REST.

## Example Workflow

1.  **Define Logic (Chat or JSON)**:
    ```json
    {
      "name": "process_users",
      "steps": [
        { "type": "fetch", "source": "btree", "resource": "users", "variable": "user_list" },
        { 
          "type": "loop", "list": "user_list", "iterator": "user", 
          "steps": [
            { "type": "ask", "prompt": "Is {{.user.name}} a VIP? (yes/no)", "output_variable": "is_vip" },
            { 
              "type": "if", "condition": "{{eq .is_vip \"yes\"}}",
              "then": [{ "type": "say", "message": "Sending gift to {{.user.name}}" }]
            }
          ]
        }
      ]
    }
    ```
2.  **Execute**:
    ```
    /play process_users
    ```
    *The system fetches 1000 users from the B-Tree (in milliseconds), loops through them in Go, and only uses the LLM to decide VIP status.*

In other words, we have turned the AI chatbot into an **IDE**, where users or data administrators can create and execute compiled program units, akin to Stored Procedures but with the reasoning power of an LLM.
