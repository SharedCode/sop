import { UseCaseCard } from '../types';

export const USE_CASES: UseCaseCard[] = [
  {
    id: 'ai-workforce',
    badge: 'Flagship Use Case',
    title: 'Real-Time AI Agent Swarms',
    tagline: 'Persistent memory + distributed task coordination without glue code.',
    iconName: 'Bot',
    problem: 'Multi-agent LLM systems currently require an orchestration framework + Redis for state + Pinecone for vectors + Postgres for logs + RabbitMQ for task queues. If an agent crashes mid-thought, context becomes fragmented.',
    sopAdvantage: 'Engram combines vector indexing, B-Tree persistent object storage, and swarm compute into a single embedded engine. Agent memory commits atomically, and worker failover redistributes reasoning tasks instantly.',
    metrics: [
      { label: 'Glue Infrastructure Eliminated', value: '4 Layers' },
      { label: 'Context Hand-off Latency', value: '< 15 ms' },
      { label: 'Memory Invariance', value: '100% ACID' }
    ],
    technicalFeatures: [
      'Transactional vector & object writes in one commit',
      'Embedded client/worker execution (no external DB hops)',
      'Sub-millisecond context checkpointing'
    ]
  },
  {
    id: 'multiplayer-games',
    badge: 'Gaming & Spatial',
    title: 'Real-Time Multiplayer & Spatial State',
    tagline: 'Synchronized inventory, match state, and spatial objects.',
    iconName: 'Gamepad2',
    problem: 'Game servers struggle to persist player state, combat transactions, and spatial data at 60Hz without introducing database bottlenecks or risking inventory duplication exploits.',
    sopAdvantage: 'Engram provides embedded ordered B-Tree storage directly in the game server process. Transactions are serialized in microseconds, and server crashes failover without state rollbacks.',
    metrics: [
      { label: 'Write Latency', value: '< 0.3 ms' },
      { label: 'State Sync', value: 'Strict Serializable' },
      { label: 'Server Memory Footprint', value: '~40 MB' }
    ],
    technicalFeatures: [
      'In-memory speed with active disk/cloud persistence',
      'Erasure-coded partition tolerance across instances',
      'C# / Go / Python native language bindings'
    ]
  },
  {
    id: 'fintech-ledger',
    badge: 'Financial Systems',
    title: 'Sub-Millisecond Financial & Escrow Ledgers',
    tagline: 'Multi-account balance invariants with zero-server overhead.',
    iconName: 'Landmark',
    problem: 'Financial transaction engines require absolute ACID guarantees. Traditional relational databases introduce 15-50ms network roundtrips and require costly high-availability failover appliances.',
    sopAdvantage: 'Engram executes transactions locally using Write-Ahead Logging (WAL) and Snapshot Isolation. Invariant checks (sum balance == zero delta) verify prior to commit with zero network overhead.',
    metrics: [
      { label: 'Transaction Latency', value: '< 120 µs' },
      { label: 'ACID Compliance', value: 'Strict Serializable' },
      { label: 'Infrastructure Cost', value: '$0 DB Host' }
    ],
    technicalFeatures: [
      'Two-phase commit coordination',
      'Optimistic Concurrency Control (OCC) conflict recovery',
      'Deterministic rollback on invariant violation'
    ]
  },
  {
    id: 'edge-iot',
    badge: 'Edge & Decentralized',
    title: 'Decentralized Edge & IoT Swarms',
    tagline: 'Autonomous data collection, local processing, and mesh sync.',
    iconName: 'Radio',
    problem: 'Edge nodes operating in factory floors or vehicles experience intermittent connectivity and cannot rely on constant central cloud database connections.',
    sopAdvantage: 'Engram compiles to standalone binaries and WebAssembly. Devices store, query, and transact locally with full ACID compliance, syncing to the swarm when connectivity resumes.',
    metrics: [
      { label: 'Offline Availability', value: '100% Local' },
      { label: 'Storage Efficiency', value: 'Reed-Solomon N+K' },
      { label: 'Binary Footprint', value: '< 8 MB' }
    ],
    technicalFeatures: [
      'WebAssembly (WASM) & ARM64 optimized',
      'Peer-to-peer partition healing',
      'Compact B-Tree node layout'
    ]
  },
  {
    id: 'saas-metadata',
    badge: 'Developer Infrastructure',
    title: 'Serverless Metadata & Multi-Tenant State',
    tagline: 'Eliminate database connection pool limits in serverless lambdas.',
    iconName: 'Server',
    problem: 'Serverless functions (AWS Lambda, Cloudflare Workers) exhaust database connection pools and pay high latency taxes opening TCP sockets to remote database servers on every invocation.',
    sopAdvantage: 'Engram embeds directly inside the worker runtime. State is persisted to object storage or local NVMe with local caching, eliminating connection pooling bottlenecks entirely.',
    metrics: [
      { label: 'Cold Start Penalty', value: '0 ms' },
      { label: 'Connection Limits', value: 'Unlimited' },
      { label: 'Read Throughput', value: '150,000+ ops/sec' }
    ],
    technicalFeatures: [
      'Zero external daemon dependencies',
      'S3 / Cassandra / Redis pluggable backend adapters',
      'Polyglot runtime support'
    ]
  }
];
