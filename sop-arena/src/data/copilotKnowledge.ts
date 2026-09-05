import { CopilotQA } from '../types';

export const COPILOT_KNOWLEDGE: CopilotQA[] = [
  {
    id: 'q1',
    category: 'fault_tolerance',
    question: 'What just happened when the worker crashed?',
    answer: 'Joltrin detected the heartbeat interruption in <15ms, rolled back any uncommitted transaction frames to protect consistency, and redistributed the queued tasks to healthy swarm workers.',
    deepDive: 'Because Joltrin combines compute scheduling with transactional state management, workers do not leave orphan locks in an external database. Uncommitted write sets in Joltrin are protected by snapshot isolation tokens. When a worker fails to commit before its lease expires, Joltrin simply invalidates the transaction token and reassigns the payload.',
    sopFeature: 'Swarm Compute & Heartbeat Lease Protocol'
  },
  {
    id: 'q2',
    category: 'fault_tolerance',
    question: 'Why did the storage node failure not corrupt data?',
    answer: 'Joltrin uses Reed-Solomon Erasure Coding. Data blocks are partitioned across shards with parity chunks, allowing instant reconstruction of missing blocks without 3x replication cost.',
    deepDive: 'Unlike traditional databases that copy the entire raw data 3 times across machines, Joltrin divides objects into data (K) and parity (M) fragments. Any K out of K+M shards can mathematically reconstruct the full B-Tree segment. When Node 2 went offline, Joltrin reconstructed queries on-the-fly using the surviving parity blocks.',
    sopFeature: 'Reed-Solomon Erasure Coding & Partition Resilience'
  },
  {
    id: 'q3',
    category: 'architecture',
    question: 'How is Joltrin different from Postgres + Kafka + Redis?',
    answer: 'Instead of spending 60% of your engineering effort maintaining glue code, distributed locks, retry queues, and synchronization between 3 separate systems, Joltrin provides one unified engine for data, compute, and transactions.',
    deepDive: 'In a traditional multi-component stack: (1) App writes to Postgres, (2) Emits event to Kafka, (3) Locks cache in Redis, (4) Workers read Kafka. If step 2 or 3 fails, the system enters split-brain or inconsistent state. In Joltrin: Application -> Joltrin. The storage, coordination, and compute queue share the same transactional boundary.',
    sopFeature: 'Unified Data + Compute Architecture'
  },
  {
    id: 'q4',
    category: 'transactions',
    question: 'Where does the ACID transaction actually happen?',
    answer: 'Transactions happen locally within the embedded Joltrin engine using Write-Ahead Logging (WAL) and Snapshot Isolation, achieving sub-millisecond commits.',
    deepDive: 'Joltrin does not require a standalone database daemon (like mysqld or postgres). The B-Tree indexing kernel runs embedded inside your application process or WebAssembly sandbox. Writes are appended to an in-memory WAL segment, validated against optimistic concurrency tokens, and committed atomically.',
    sopFeature: 'Embedded ACID B-Tree Engine'
  },
  {
    id: 'q5',
    category: 'ai_swarm',
    question: 'How does Joltrin accelerate AI Agent Workforces?',
    answer: 'AI agents require fast context checkpointing, vector similarity indexing, and task coordination. Joltrin combines high-dimensional vector search and transactional state in the same engine.',
    deepDive: 'Most AI frameworks use a vector database (Pinecone/Milvus) for embeddings plus Postgres for metadata plus Redis for agent lock states. Joltrin allows an agent to save its prompt context, embedding vector, and execution state in a single atomic transaction, preventing fragmented context when agents scale.',
    sopFeature: 'Integrated AI Vector & Context Persistence'
  },
  {
    id: 'q6',
    category: 'business',
    question: 'What is the ROI / business value of Joltrin for a CTO?',
    answer: 'Joltrin reduces infrastructure bills by running embedded/edge workloads, eliminates database connection bottlenecks, and cuts backend glue code by over 50%.',
    deepDive: 'By unifying data persistence with compute coordination, engineering teams spend less time building custom retry loops, distributed lock managers, and outbox pattern listeners. Deployment is as simple as importing a Go, Python, or C# library.',
    sopFeature: 'Developer Velocity & Infrastructure Consolidation'
  }
];
