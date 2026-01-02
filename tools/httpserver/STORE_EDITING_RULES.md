# SOP Data Manager - Store Editing Rules

This document outlines the rules for editing store metadata and structure via the SOP Data Manager UI and API.

## 1. Empty Store (Count == 0)

When a store is empty (contains no items), it is considered "malleable". You can perform a full structural rebuild, similar to creating a new store.

**Allowed Changes:**
*   **Key Type**: Can be changed (e.g., String -> Int, String -> Map).
*   **Value Type**: Can be changed.
*   **Index Specification / CEL**: Can be added, modified, or removed.
*   **Seed Data**: You can add a seed item to "lock in" the type inference.

**Restrictions:**
*   **Structural Settings**: Slot Length, Is Unique, Value In Node are **STRICTLY READ-ONLY**. They cannot be changed after creation, even if the store is empty.

## 2. Non-Empty Store (Count > 0)

When a store has data, its structure is locked to prevent data corruption or inconsistency.

**Locked Fields (Read-Only):**
*   **Key Type**: Cannot be changed.
*   **Value Type**: Cannot be changed.
*   **Structural Settings**: Slot Length, Is Unique, Value In Node, Streaming Mode.
*   **Index Specification / CEL**: Generally locked.

**Exceptions (Allowed Changes):**
*   **Fixing Missing Specs (One-Time Only)**: If a store was created (e.g., via code) *without* an Index Specification or CEL Expression, you are allowed to add them. 
    *   **WARNING**: This is a one-time operation. Once an Index or CEL is saved, it becomes **permanently locked** and cannot be edited via the UI/API, even if you made a mistake.
    *   *Recovery*: If you apply an incorrect spec, you must either delete/recreate the store or manually intervene in the file system (edit `storeinfo.txt` directly) at your own risk. **IMPORTANT: Ensure no running application (including the HTTP server) is accessing the store during this manual edit.**
*   **Description**: Always editable.
*   **Cache Configuration**: Cache Duration and TTL settings are always editable.

## 3. Admin Override

In extreme cases, an **Admin Token** (Root Password) can be used to override some locks (e.g. Key Type on empty store if UI blocks it). However, **Structural Fields (Slot Length, Is Unique, Value In Node)** remain strictly locked and cannot be changed via the API.
