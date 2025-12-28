# SOP AI Swarm: Distributed Compute Architecture

**Status:** Proposal / Vision
**Context:** "The Database is the Computer"

## 1. The Vision: Swarm Computing
We are evolving SOP from a distributed *storage* engine into a distributed *compute* engine. By leveraging the existing clustered database architecture, we can enable "Swarm Computing" where logic (Macros) and workers (Agents) are distributed across the network natively.

## 2. Core Philosophy
*   **Data Gravity**: Move the compute to the data. Agents run locally on the nodes where the data resides.
*   **Macros as Instructions**: Macros are the serialized instructions sent to the swarm.
*   **SOP as Coordination**: We do not need a separate message bus (Kafka/RabbitMQ). The database itself (via Redis "pheromones" and B-Tree storage) acts as the coordination layer.

## 3. Proposed Architecture

### 3.1. The "Job" Store
A dedicated SOP B-Tree (`sys_jobs`) acts as the task queue.
*   **Key**: `JobKey` (Struct: `JobID`, `Status`, `WorkerID`)
    *   *Optimization*: We use SOP's "Ride on Key" capability. The `Status` and `WorkerID` are stored in the Key metadata. This allows checking and claiming jobs without fetching the heavy `JobSpec` payload.
    *   *Indexing*: To support the Hydrator, we will maintain a secondary index (or use a composite key `Status|Priority|JobID`) to efficiently query for `PENDING` jobs.
*   **Value**: `JobSpec` (MacroID, Parameters, TargetFilter)

### 3.2. The "Result" Store
A dedicated SOP B-Tree (`sys_results`) collects outputs.
*   **Key**: `JobID|NodeID`
*   **Value**: `ResultData` (JSON)

### 3.3. Distribution Helpers (The "Tools")
We will implement a set of high-level helpers to abstract the complexity of distribution.

#### `Distribute(ctx, macroName, params, nodeFilter)`
*   **Purpose**: "Fire and Forget" a task to the swarm.
*   **Mechanism**:
    1.  **Store Payload**: Creates a `Job` record in `sys_jobs` (SOP B-Tree). This handles the heavy data (params, macro body) and provides durability.
    2.  **Return**: Returns `JobID` to caller immediately.
    *   *Note*: We do not push to Redis here to avoid overloading it during bulk submissions. The **Hydrator** handles the signaling.

#### `Await(ctx, jobID, timeout)`
*   **Purpose**: Collect results from the swarm.
*   **Mechanism**:
    1.  Polls `sys_results` for entries matching `JobID|*`.
    2.  Aggregates results as they arrive.
    3.  Returns when all expected nodes have reported or timeout occurs.

#### `MapReduce(ctx, inputStore, mapMacro, outputStore, reduceMacro)`
*   **Purpose**: Classic distributed processing on SOP data.
*   **Mechanism**:
    1.  **Partition**: The `inputStore` is logically partitioned and `mapMacro` jobs are distributed to the swarm.
    2.  **Map**: Workers process items and write intermediate key/values to a temporary `shuffle_store`.
    3.  **Shuffle**: **SOP's B-Tree naturally keeps the `shuffle_store` sorted by key**, grouping values for the reducer automatically.
    4.  **Reduce**: A reducer job iterates over the `shuffle_store`, aggregating values for each key and writing to `outputStore`.

### 3.4. The Hydrator (Flow Control)
To prevent overloading Redis with millions of Job IDs, we decouple "Submission" from "Notification" using a Flow Control pattern.

*   **Component**: A singleton background worker (The "Hydrator").
*   **Logic**:
    1.  **Monitor**: Checks the depth of the Redis Job Queue.
    2.  **Hydrate**: If queue length < Threshold (e.g., 1000), it queries `sys_jobs` (B-Tree) for the next batch of `PENDING` jobs.
    3.  **Enqueue**: Pushes this batch of JobIDs to Redis.
*   **Benefit**:
    *   **Backpressure**: The B-Tree absorbs the write spike (e.g., 1M jobs). Redis only sees what the workers can handle.
    *   **Reliability**: If Redis crashes, the Hydrator simply refills the queue from the B-Tree source of truth.

### 3.5. Infrastructure
*   **Separate Redis**: We support configuring a dedicated Redis cluster for Swarm Signaling, distinct from the SOP Cache/Locking Redis. This ensures compute signaling does not impact storage latency.

### 3.6. Worker Batch Consumption
To maximize throughput, Workers do not process jobs one by one.
*   **Batch Fetch**: A worker pops a batch of JobIDs (e.g., 100-500) from Redis.
*   **Transactional Claim**: It opens a **single SOP Transaction** to:
    1.  Read the batch of **JobKeys** from `sys_jobs`.
    2.  Verify `Status` is `PENDING` (Zero Payload I/O).
    3.  Update the Key metadata to `Status=RUNNING` and `WorkerID=Self`.
    4.  Commit.
*   **Efficiency**: This reduces the transaction overhead by orders of magnitude, leveraging SOP's ability to handle large batch updates efficiently.

### 3.7. The Swarm Runtime (The "Iceberg")
To the developer, it looks simple. Under the hood, it handles the complex distributed systems logic.

*   **Developer Experience**:
    ```go
    // Simple, pure logic. No transaction management code here.
    swarm.Handle("AnalyzeLogs", func(ctx context.Context, args Args) Result {
        return process(args)
    })
    ```

*   **Runtime Responsibilities (The Hidden Complexity)**:
    1.  **Transaction Management**: Automatically opens `Begin()` and `Commit()`. Handles retries on `Conflict`.
    2.  **Resource Cleanup**: Automatically calls `OnIdle()` periodically to release locks and memory, preventing "leakage" in long-running workers.
    3.  **Batch Optimization**: Can be configured to execute multiple user function calls within a single SOP transaction for throughput.
    4.  **Panic Safety**: Wraps execution in `recover()` blocks. If a job crashes, the Runtime catches it, marks the job as `FAILED`, and keeps the worker alive.

### 3.8. Deployment Model
We will provide production-ready implementations of the core components:
*   **The Governor (Publisher)**: A highly optimized, standalone service responsible for the Hydration loop.
*   **The Worker (Subscriber)**: A robust service wrapper that hosts the Swarm Runtime.

**User Responsibility**: The user's role is simply **Capacity Planning**.
*   They deploy the Governor (usually 1 is enough).
*   They deploy as many Workers as needed to achieve their desired throughput (e.g., 50 nodes for 50k jobs/sec).
*   They register their Macros with the Workers.

## 4. Implementation Roadmap
1.  **Define Job/Result Schemas**: Create the Go structs for Jobs and Results.
2.  **Implement Job Store**: Add `sys_jobs` and `sys_results` to the standard store set.
3.  **Agent "Worker Mode"**: Update `DataAdminAgent` to have a background loop that listens for Jobs.
4.  **Build the Helpers**: Implement the `Distribute` and `Await` functions in a new `ai/swarm` package.

---
* "We just have to make helpers/tools for distribution, which is easy, because we are clustered database native."*
