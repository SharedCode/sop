import React from 'react';
import { SystemMetrics } from '../types';
import { 
  Activity, 
  ShieldCheck, 
  Clock, 
  Cpu, 
  Database, 
  RefreshCw,
  Zap,
  Sparkles
} from 'lucide-react';

interface MetricsGridProps {
  metrics: SystemMetrics;
}

export const MetricsGrid: React.FC<MetricsGridProps> = ({ metrics }) => {
  return (
    <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
      
      {/* 1. System Reliability Score */}
      <div className="bg-dark-900 border border-dark-800 rounded-xl p-3.5 relative overflow-hidden">
        <div className="flex items-center justify-between text-slate-400 mb-1">
          <span className="text-[11px] font-mono">Reliability</span>
          <ShieldCheck className="w-3.5 h-3.5 text-brand-400" />
        </div>
        <div className="flex items-baseline space-x-1">
          <span className={`text-2xl font-bold font-mono ${metrics.reliabilityScore >= 95 ? 'text-brand-400' : 'text-amber-400'}`}>
            {metrics.reliabilityScore.toFixed(1)}%
          </span>
        </div>
        <span className="text-[9px] font-mono text-slate-500 block mt-0.5">Composite SLA</span>
      </div>

      {/* 2. Real-time Throughput (TPS) */}
      <div className="bg-dark-900 border border-dark-800 rounded-xl p-3.5">
        <div className="flex items-center justify-between text-slate-400 mb-1">
          <span className="text-[11px] font-mono">Throughput</span>
          <Activity className="w-3.5 h-3.5 text-blue-400" />
        </div>
        <div className="flex items-baseline space-x-1">
          <span className="text-2xl font-bold font-mono text-white">
            {metrics.tps.toLocaleString()}
          </span>
          <span className="text-[10px] text-slate-400 font-mono">ops/s</span>
        </div>
        <span className="text-[9px] font-mono text-slate-500 block mt-0.5">Total: {(metrics.totalTransactions / 1000).toFixed(1)}k</span>
      </div>

      {/* 3. Consistency Invariant */}
      <div className="bg-dark-900 border border-dark-800 rounded-xl p-3.5">
        <div className="flex items-center justify-between text-slate-400 mb-1">
          <span className="text-[11px] font-mono">ACID Invariant</span>
          <Sparkles className="w-3.5 h-3.5 text-accent-cyan" />
        </div>
        <div className="flex items-baseline space-x-1">
          <span className="text-2xl font-bold font-mono text-accent-cyan">
            {metrics.consistencyRate.toFixed(2)}%
          </span>
        </div>
        <span className="text-[9px] font-mono text-slate-500 block mt-0.5">0 Dropped Writes</span>
      </div>

      {/* 4. Sub-Millisecond Latency */}
      <div className="bg-dark-900 border border-dark-800 rounded-xl p-3.5">
        <div className="flex items-center justify-between text-slate-400 mb-1">
          <span className="text-[11px] font-mono">Latency (p50)</span>
          <Clock className="w-3.5 h-3.5 text-purple-400" />
        </div>
        <div className="flex items-baseline space-x-1">
          <span className="text-2xl font-bold font-mono text-purple-300">
            {metrics.avgLatencyMs.toFixed(2)}
          </span>
          <span className="text-[10px] text-slate-400 font-mono">ms</span>
        </div>
        <span className="text-[9px] font-mono text-slate-500 block mt-0.5">p99: {metrics.p99LatencyMs.toFixed(2)}ms</span>
      </div>

      {/* 5. Swarm & Shard Capacity */}
      <div className="bg-dark-900 border border-dark-800 rounded-xl p-3.5">
        <div className="flex items-center justify-between text-slate-400 mb-1">
          <span className="text-[11px] font-mono">Swarm Nodes</span>
          <Cpu className="w-3.5 h-3.5 text-brand-400" />
        </div>
        <div className="flex items-baseline space-x-1">
          <span className="text-2xl font-bold font-mono text-white">
            {metrics.activeWorkers}/{metrics.totalWorkers}
          </span>
          <span className="text-[10px] text-slate-400 font-mono">workers</span>
        </div>
        <span className="text-[9px] font-mono text-slate-500 block mt-0.5">{metrics.activeStorageNodes}/4 Storage Shards</span>
      </div>

      {/* 6. Self-Healing & Redistribution */}
      <div className="bg-dark-900 border border-dark-800 rounded-xl p-3.5">
        <div className="flex items-center justify-between text-slate-400 mb-1">
          <span className="text-[11px] font-mono">Failover Sync</span>
          <RefreshCw className="w-3.5 h-3.5 text-accent-amber" />
        </div>
        <div className="flex items-baseline space-x-1">
          <span className="text-2xl font-bold font-mono text-accent-amber">
            {metrics.redistributedJobs}
          </span>
          <span className="text-[10px] text-slate-400 font-mono">tasks</span>
        </div>
        <span className="text-[9px] font-mono text-slate-500 block mt-0.5">Auto-redistributed</span>
      </div>

    </div>
  );
};
