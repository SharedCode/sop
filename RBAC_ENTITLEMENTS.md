# Role-Based Access Control (RBAC) & Entitlements Architecture

This document describes the data-driven RBAC and Entitlements architecture used in SOP. This system is designed to support B2B SaaS models, multi-tenancy, leased deployments, and strict resource bounding (important for costly operations like AI inference).

## Core Concepts

Instead of hardcoding role behaviors (e.g., `if user.IsProTier()`), SOP relies on a **Data-Driven RBAC Model**. Capabilities, computational quotas, and storage limits are defined in data structures ("Entitlements") which are assigned to Roles.

### 1. Functionality Categories
The system divides its functionalities into modular categories. Access and limits are mapped against these categories:
*   `system`: Core infrastructure boundaries.
*   `spaces`: Knowledgebases and vector storage.
*   `stores`: Key/Value caches and regular B-Tree data stores.
*   `scripts`: System execution and logic blocks.
*   `ai_chat`: Standard LLM inferences.
*   `ai_vision`: Image and multi-modal processing (often requires tighter quotas).
*   `ai_system_admin`: Advanced AI commands capable of mutating DBs or generating infrastructure.

### 2. Execution Limits (Time-Bound Quotas)
Used strictly for operations that cost compute or third-party API spend (e.g., AI inference). 
These are evaluated using a Token Bucket or Sliding Window logic.
*   **MaxRequests**: Total actions allowed.
*   **WindowMin**: The time frame (e.g., in minutes) before the quota resets.

### 3. Storage Limits (Physical Bounds)
Used for data persistence boundaries to prevent Out-Of-Memory (OOM) or Disk Full attacks on hosted deployments.
*   **MaxEntities**: Upper bound on creation (e.g., max 5 Spaces).
*   **MaxBytes**: Absolute payload/volume size caps.

---

## Data Models

### Role Definition
A global "Recipe" for a Role, tying together allowed actions and resource limits. Stored in the System DB (e.g., `system_roles`).

```go
type RateLimit struct {
	MaxRequests int `json:"max_requests"`
	WindowMin   int `json:"window_min"`
}

type StorageLimit struct {
	MaxEntities int   `json:"max_entities"`
	MaxBytes    int64 `json:"max_bytes"`
}

type RoleDefinition struct {
	RoleName        string                   `json:"role_name"`
	Description     string                   `json:"description"`
	// Categories mapped to a list of allowed Actions (read, write, delete, list, execute)
	Capabilities    map[string][]Action      `json:"capabilities"`
	// Rate limits for compute-heavy actions
	ExecutionLimits map[string]*RateLimit    `json:"execution_limits,omitempty"`
	// Limits on physical data footprint
	StorageLimits   map[string]*StorageLimit `json:"storage_limits,omitempty"`
}
```

### Resource Access (Local ACL)
Individual entities (e.g., a specific Space or Store) carry their own local ACLs bridging to global Roles and individual User overrides.

```go
type ResourceAccess struct {
	Visibility Visibility          `json:"visibility"` // public, private, system
	OwnerID    string              `json:"owner_id,omitempty"`
	Roles      map[string][]string `json:"roles,omitempty"`
	Users      map[string][]string `json:"users,omitempty"`
}
```

---

## Evaluation Flow & Best Practices

1. **Pre-flight Quota Checks**: Before creating a resource (like a Space), the API checks the caller's applied `StorageLimit`. It queries the DB for the user's current entity count, rejecting with `403 Forbidden` or `413 Payload Too Large` if breached.
2. **AI Quota Tracking**: The `/api/ai/*` routes trigger `AuthorizeWithQuota` middleware to read/write rolling counters tracking how many times a given user has triggered `ai_chat` or `ai_vision` in the last `WindowMin`.
3. **Role Composition**: Company Admins (Tenants) act as local Super Admins. They can provision Custom Roles mapping to the capabilities extended to their Company's master plan, distributing usage slices to their members.

## UI Integration & Entitlements Flow

To ensure high performance and prevent UI-blocking race conditions, SOP employs an **Entitlements Payload Caching** strategy on the frontend rather than making granular, asynchronous permission checks per UI component.

### The Problem with Granular API Checks
Making asynchronous HTTP calls (e.g., `fetch('/api/space/config')`) halfway through synchronous UI rendering loops (like building a data grid or rendering sidebar rows) introduces severe race conditions, slows down UI interactivity, and generates excessive network traffic.

### The Solution: Pushed Entitlements Map & $O(1)$ Contextual Evaluation
Instead of the UI asking the backend *“Can the user edit this specific item?”*, the backend "streams" or pushes the comprehensive **Entitlements** context down to the UI either during page load or alongside primary resource fetching. The pattern follows: **Asset + RBAC Map**.

Crucially, **SOP eschews $O(N)$ granular per-asset checks**. Iterating over thousands of items to build individual access maps scales poorly. Instead, SOP employs **Functional Context RBAC**. The backend evaluates the user's capabilities for the encompassing context (for example, "What permissions does this user have in this Database Space?") exactly once ($O(1)$) and returns those bounds to govern the entire dataset.

To facilitate this architecture cleanly, the backend incorporates the following mechanisms:

1. **Global RBAC Blueprint (Dynamic Organic Registry)**
   Rather than a monolithic, static configuration file, the RBAC blueprint is constructed dynamically at runtime.
   * **Decentralized Authoring**: As different modules (e.g., Spaces, Stores, AI Agents) are authored, they call a centralized "RBAC Authoring API" (typically inside their `init()` functions) to register their specific asset types and permission rules.
   * **Automated Maintenance (No Stale Data)**: This guarantees the RBAC map is always organic and perfectly synchronized with the active codebase. If a codebase feature is deleted, its RBAC registration naturally disappears.
   * **Proposed Data Structure**:
     ```go
     // AssetBlueprint defines the RBAC footprint for a specific module or asset type.
     type AssetBlueprint struct {
         AssetType   string              // e.g., "space", "store", "agent"
         Description string              // Human-readable context
         Endpoints   []string            // UI API endpoints related to this asset
         Actions     []sop.Action        // Capabilities: Read, Write, Delete, Execute
         
         // Evaluator executes the actual permission check for a specific asset instance
         Evaluator   func(ctx context.Context, assetID string, action sop.Action) bool
     }

     // Global Blueprint Registry
     var SystemRBACMap = make(map[string]AssetBlueprint)

     // RBAC Authoring API (Called by modules during initialization)
     func RegisterAssetRBAC(blueprint AssetBlueprint) {
         SystemRBACMap[blueprint.AssetType] = blueprint
     }
     ```

2. **Centralized RBAC Resolution API**
   Whenever an endpoint is hit, a generalized API handles RBAC resolution. Given the logged-in user and the endpoint called, it consults the Global RBAC Blueprint to determine permissions.
   * It takes the raw data (the Assets) and pairs it with the evaluated permissions map.
   
3. **Bundled Response Payloads**
   The API seamlessly bundles the resulting RBAC map as a secondary piece of the outgoing response. The UI consumes this data in pairs: the underlying Data payload and its governing Permissions map.
   
   *Example Response Structure:*
   ```json
   {
     "data": [
       {"id": "uuid-1", "name": "SOP/data"},
       {"id": "uuid-2", "name": "InternalProject"}
     ],
     "rbac": {
       "uuid-1": {"can_edit": false, "can_delete": false},
       "uuid-2": {"can_edit": true, "can_delete": true}
     }
   }
   ```

4. **Synchronous UI Lookup**
   The frontend caches this bundled payload in local memory (e.g., `let currentPermissionsMap = {}`). When frontend rendering methods (like `isSpaceReadOnly()`, `showDetail()`) are invoked, they execute a highly performant $O(1)$ synchronous lookup against the memory map rather than making network requests.

5. **Cache Invalidation**
   The UI memory map inherently stays fresh because it is forcefully refreshed on logical boundaries (e.g., changing databases, refreshing the grid, or a full page reload). The brief caching window heavily conserves API traffic while preventing the UI from rendering stale permission states long-term.
