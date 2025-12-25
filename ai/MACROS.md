# AI Assistant Macros: Natural Language Programming

The SOP AI Assistant includes a powerful **Macro System** that transforms your AI chat sessions into a "Natural Language Programming" environment. It allows you to record, script, and execute complex workflows that combine the flexibility of LLMs with the performance of compiled code.

## Concept: "Compiled Instructions"

Unlike traditional chat history, SOP Macros are **deterministic programs**.
*   **Recording**: When you record a session, the system captures the **logic** of your interaction.
*   **Scripting**: You can manually edit or generate macros using a standardized JSON schema, adding loops, conditions, and variables.
*   **Execution**: The SOP engine executes these steps directly using compiled Go code. This is akin to running a compiled binary or a high-performance stored procedure, not just replaying a chat log.

## The SOP Advantage: Bare Metal Performance

SOP exposes its B-Tree engine directly to the macro system. This effectively creates a **machine instruction set** for end-users.

*   **Bare Metal API**: SOP's B-Tree API is compiled Go code.
*   **High-Performance Units**: Recorded macros are sequences of JSON instructions that command this compiled code to perform iterations, expression evaluations, and data lookups.
*   **No Parsing Overhead**: Unlike SQL statements that require complex parsing and query planning at runtime, SOP macros are pre-structured units of work.
*   **System Database**: Macros are persisted in a dedicated **SystemDB**, a robust SOP B-Tree store. This ensures your "programs" are durable, transactional, and available across server restarts.

## Natural Language Programming (NLP) System

We are building a system where the **LLM acts as the compiler**.
1.  **User Intent**: You describe a complex task in natural language (e.g., "For every user in the 'users' table who hasn't logged in for 30 days, send a reminder email").
2.  **LLM Compilation**: The AI translates this intent into a structured SOP Macro (JSON) containing loops (`loop`), conditions (`if`), and tool calls (`ask`, `fetch`).
3.  **SOP Execution**: The SOP engine executes this script. It fetches data from B-Trees efficiently (`fetch`), iterates in compiled code (`loop`), and only calls the LLM when semantic understanding is needed (`ask`).

This hybrid approach gives you the best of both worlds:
*   **Ease of Use**: "Programming" via chat.
*   **Performance**: Heavy lifting (loops, data access) is done by compiled Go code, not the LLM.

## Macro Schema (The "Language")

The macro system uses a stable JSON schema acting as a mini-SDK:

*   **`ask`**: Query the LLM for reasoning or creative generation.
*   **`set`**: Assign values to variables.
*   **`if`**: Conditional branching logic.
*   **`loop`**: Iterate over lists or data fetched from B-Trees.
*   **`fetch`**: High-performance data retrieval directly from SOP B-Trees.
*   **`say`**: Output information to the user.

## Commands

### Recording & Playback
*   `/record <name>`: Start recording a session as a new macro.
*   `/stop`: Stop recording and save to SystemDB.
*   `/play <name> [param=value ...]`: Execute a saved macro.
    *   **Parameters**: Pass arguments like `user_id=123`.
    *   **Templating**: Use `{{.user_id}}` in your macro to inject values.

### Management
*   `/list`: List all available macros in the SystemDB.

## Remote Execution via REST API

Because macros are stored in the persistent SystemDB, they are instantly available as **API Endpoints**. You can trigger any complex workflow remotely:

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
