export type NodeType = 'storage' | 'worker' | 'coordinator' | 'client';

export type NodeStatus = 'healthy' | 'degraded' | 'failed' | 'recovering' | 'scaling';

export interface TopologyNode {
  id: string;
  type: NodeType;
  name: string;
  status: NodeStatus;
  load: number; // 0 to 100%
  activeTasks: number;
  capacity: number;
  x: number; // 0 to 100 (relative percent)
  y: number; // 0 to 100 (relative percent)
  memoryMb: number;
  shards?: string[];
  roleDescription?: string;
  failTime?: number;
}

export interface JobParticle {
  id: string;
  fromId: string;
  toId: string;
  progress: number; // 0.0 to 1.0
  type: 'tx_write' | 'tx_commit' | 'ai_task' | 'rebalance' | 'recovery';
  label?: string;
  speed: number;
}

export interface SystemMetrics {
  tps: number; // Transactions / tasks per second
  totalTransactions: number;
  consistencyRate: number; // 99.999% or 100.00%
  activeWorkers: number;
  totalWorkers: number;
  activeStorageNodes: number;
  totalStorageNodes: number;
  avgLatencyMs: number;
  p99LatencyMs: number;
  reliabilityScore: number; // 0 to 100
  redistributedJobs: number;
  erasureChunksRecovered: number;
  conflictsResolved: number;
  glueComponentsEliminated: number; // The "Without SOP" comparison metric
}

export type LogLevel = 'info' | 'warn' | 'error' | 'success' | 'sop';
export type LogCategory = 'TX' | 'SWARM' | 'FAILOVER' | 'RECOVERY' | 'AI' | 'SYSTEM';

export interface LogEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  category: LogCategory;
  message: string;
  detail?: string;
}

export type ViewMode = 'arena' | 'disaster' | 'investor' | 'compare';

export type ScenarioType = 'sandbox' | 'disaster_60s' | 'ai_agent_workforce' | 'game_state_sync';

export interface ScenarioStep {
  timeOffsetSec: number;
  title: string;
  description: string;
  action: (engine: any) => void;
  sopPrinciple: string;
}

export interface UseCaseCard {
  id: string;
  badge: string;
  title: string;
  tagline: string;
  iconName: string;
  problem: string;
  sopAdvantage: string;
  metrics: { label: string; value: string }[];
  technicalFeatures: string[];
}

export interface CopilotQA {
  id: string;
  question: string;
  answer: string;
  deepDive: string;
  sopFeature: string;
  category: 'architecture' | 'fault_tolerance' | 'ai_swarm' | 'transactions' | 'business';
}

export interface DataRecord {
  key: string;
  value: any;
  version: number;
  shard: string;
  lockedBy?: string;
  updatedAt: string;
}
