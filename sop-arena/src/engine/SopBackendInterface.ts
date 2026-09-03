import { TopologyNode, SystemMetrics, LogEntry, JobParticle, DataRecord } from '../types';

export interface SopBackendListener {
  onNodesUpdate: (nodes: TopologyNode[]) => void;
  onMetricsUpdate: (metrics: SystemMetrics) => void;
  onLogEntry: (log: LogEntry) => void;
  onParticlesUpdate: (particles: JobParticle[]) => void;
  onRecordsUpdate: (records: DataRecord[]) => void;
}

export interface SopBackendInterface {
  init(listener: SopBackendListener): void;
  destroy(): void;
  
  // Workload Controls
  setTargetTps(tps: number): void;
  createTransactionStorm(multiplier?: number): void;
  
  // Worker & Swarm Operations
  addWorker(): void;
  removeWorker(workerId?: string): void;
  killWorker(workerId?: string): void;
  
  // Storage & Fault Operations
  failStorageNode(nodeId?: string): void;
  recoverStorageNode(nodeId?: string): void;
  triggerSelfHealing(): void;
  
  // State Queries
  getNodes(): TopologyNode[];
  getMetrics(): SystemMetrics;
  getRecords(): DataRecord[];
  
  // Reset
  resetSystem(): void;
}
