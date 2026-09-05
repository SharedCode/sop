import { CopilotQA } from '../types';

export const COPILOT_KNOWLEDGE: CopilotQA[] = [
  {
    id: 'q1',
    category: 'fault_tolerance',
    question: 'What just happened when the worker crashed?',
    answer: 'Engram detected the heartbeat interruption in <15ms, rolled back any uncommitted transaction frames to protect consistency, and redistributed the queued tasks to healthy swarm workers.',
    deepDive: 'Because Engram combines compute scheduling with transactional state management, workers do not leave orphan locks in an external database. Uncommitted write sets in Engram are protected by snapshot isolation tokens. When a worker fails to commit before its lease expires, Engram simply invalidates the transaction token and reassigns the payload.',
    sopFeature: 'Swarm Compute & Heartbeat Lease Protocol'
  },
  {
    id: 'q2',
    category: 'fault_tolerance',
    question: 'Why did the storage node failure not corrupt data?',
    answer: 'Engram uses Reed-Solomon Erasure Coding. Data blocks are partitioned across shards with parity chunks, allowing instant reconstruction of missing blocks without 3x replication cost.',
    deepDive: 'Unlike traditional databases that copy the entire raw data 3 times across machines, Engram divides objects into data (K) and parity (M) fragments. Any K out of K+M shards can mathematically reconstruct the full B-Tree segment. When Node 2 went offline, Engram reconstructed queries on-the-fly using the surviving parity blocks.',
    sopFeature: 'Reed-Solomon Erasure Coding & Partition Resilience'
  },
  {
    id: 'q3',
    category: 'architecture',
    question: 'How is Engram different from Postgres + Kafka + Redis?',
    answer: 'Instead of spending 60% of your engineering effort maintaining glue code, distributed locks, retry queues, and synchronization between 3 separate systems, Engram provides one unified engine for data, compute, and transactions.',
    deepDive: 'In a traditional multi-component stack: (1) App writes to Postgres, (2) Emits event to Kafka, (3) Locks cache in Redis, (4) Workers read Kafka. If step 2 or 3 fails, the system enters split-brain or inconsistent state. In Engram: Application -> Engram. The storage, coordination, and compute queue share the same transactional boundary.',
    sopFeature: 'Unified Data + Compute Architecture'
  },
  {
    id: 'q4',
    category: 'transactions',
    question: 'Where does the ACID transaction actually happen?',
    answer: 'Transactions happen locally within the embedded Engram engine using Write-Ahead Logging (WAL) and Snapshot Isolation, achieving sub-millisecond commits.',
    deepDive: 'Engram does not require a standalone database daemon (like mysqld or postgres). The B-Tree indexing kernel runs embedded inside your application process or WebAssembly sandbox. Writes are appended to an in-memory WAL segment, validated against optimistic concurrency tokens, and committed atomically.',
    sopFeature: 'Embedded ACID B-Tree Engine'
  },
  {
    id: 'q5',
    category: 'ai_swarm',
    question: 'How does Engram accelerate AI Agent Workforces?',
    answer: 'AI agents require fast context checkpointing, vector similarity indexing, and task coordination. Engram combines high-dimensional vector search and transactional state in the same engine.',
    deepDive: 'Most AI frameworks use a vector database (Pinecone/Milvus) for embeddings plus Postgres for metadata plus Redis for agent lock states. Engram allows an agent to save its prompt context, embedding vector, and execution state in a single atomic transaction, preventing fragmented context when agents scale.',
    sopFeature: 'Integrated AI Vector & Context Persistence'
  },
  {
    id: 'q6',
    category: 'business',
    question: 'What is the ROI / business value of Engram for a CTO?',
    answer: 'Engram reduces infrastructure bills by running embedded/edge workloads, eliminates database connection bottlenecks, and cuts backend glue code by over 50%.',
    deepDive: 'By unifying data persistence with compute coordination, engineering teams spend less time building custom retry loops, distributed lock managers, and outbox pattern listeners. Deployment is as simple as importing a Go, Python, or C# library.',
    sopFeature: 'Developer Velocity & Infrastructure Consolidation'
  }
];
