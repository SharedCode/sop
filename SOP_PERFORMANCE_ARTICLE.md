## ⚡ Performance

SOP is designed for high throughput and low latency. These benchmarks were conducted on a 2015 MacBook Pro (Dual-Core Intel Core i5, 8GB RAM) to demonstrate high efficiency even on modest, resource-constrained hardware.

### Optimization Guide: Tuning `SlotLength`

The `SlotLength` parameter controls the number of items stored in each B-Tree node. Tuning this value can significantly impact performance depending on your dataset size and workload.

**Configuration Used:**
- `IsValueDataInNodeSegment: true` (Values stored directly in leaf nodes)
- `CacheType: sop.InMemory` (Persisted to disk with ACID support)

#### 10,000 Items Benchmark
For smaller datasets, a `SlotLength` of **2,000** offers the best balance.

| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 107,652 | 136,754 | 40,964 |
| **2,000** | **132,901** | **142,907** | **50,093** |
| 3,000 | 135,066 | 137,035 | 49,754 |
| 4,000 | 123,190 | 122,228 | 48,094 |
| 5,000 | 132,670 | 126,432 | 47,150 |

#### 100,000 Items Benchmark
For larger datasets, increasing `SlotLength` to **4,000** yields higher throughput.

| SlotLength | Insert (ops/sec) | Read (ops/sec) | Delete (ops/sec) |
| :--- | :--- | :--- | :--- |
| 1,000 | 121,139 | 145,195 | 48,346 |
| 2,000 | 132,805 | 136,684 | 51,817 |
| 3,000 | 137,296 | 141,764 | 50,605 |
| **4,000** | **145,417** | 143,770 | **51,988** |
| 5,000 | 137,054 | 144,565 | 50,309 |

*Recommendation: Start with a `SlotLength` of 2,000 for general use, and increase to 4,000+ for write-heavy workloads with large datasets.*

> **Pro Tip for Massive Scale:**
> For datasets reaching into the **hundreds of billions or trillions** of records, you can increase `SlotLength` up to **10,000**. This maximizes node density, allowing a single B-Tree to manage petabytes of data with minimal metadata overhead. See [Scalability & Limits](SCALABILITY.md) for the math.

### Why this matters
These benchmarks are running with **Full ACID Transaction** protection. Unlike simple Key-Value stores that optimize purely for random writes (often sacrificing order or safety), SOP provides a robust foundation for complex data access:
- **Sorted Data**: Native support for `ORDER BY ASC/DESC`.
- **Fast Search**: Efficient range scans and lookups.
- **SQL-Ready**: The ordered structure supports efficient Merge Joins and complex query patterns out of the box.
- **Linear Scalability**: SOP is built on **SWARM** (SOP exclusive) transaction technology, allowing it to scale linearly and horizontally across the network as nodes and hardware are added.

### Competitive Landscape

How does SOP compare to other storage engines in the Go ecosystem?

| Database | Type | Typical Batch Write (Ops/Sec) | Key Differences |
| :--- | :--- | :--- | :--- |
| **SOP** | B-Tree | **~145,000** | **ACID, Ordered, SWARM Scalability** |
| **BadgerDB** | LSM Tree | ~150,000 - 300,000 | Faster writes (LSM), but requires compaction and lacks native B-Tree ordering features. |
| **SQLite** | B-Tree (C) | ~50,000 - 100,000 | Slower due to SQL parsing overhead. |
| **BoltDB** | B-Tree | ~10,000 - 50,000 | Slower random writes due to copy-on-write page structure. |
| **Go `map`** | Hash Map | ~10,000,000+ | In-memory only. No persistence, no ACID, no ordering. |

> **Benchmark Hardware Context:**
> The SOP benchmarks above were achieved on a **2015 MacBook Pro (Dual-Core i5)**. In contrast, published benchmarks for engines like BadgerDB or BoltDB are typically conducted on modern, high-end servers with NVMe SSDs and many cores. On comparable high-end hardware, SOP's throughput would be significantly higher.

> **Note on SOP's Unique Value Proposition:**
> While raw speed is comparable to top-tier engines, SOP distinguishes itself by combining features that usually don't exist together:
> *   **Full ACID Transactions**: Guarantees safety where others might trade it for speed.
> *   **SWARM Technology**: Unlike monolithic engines, SOP scales linearly across the network. SOP's exclusive decentralized coordination allows it to scale linearly and horizontally across a network as you add nodes.
> *   **SQL-Ready Structure**: Data is stored in a strictly ordered structure, enabling ORDER BY, range scans, and efficient Merge Joins out of the box.